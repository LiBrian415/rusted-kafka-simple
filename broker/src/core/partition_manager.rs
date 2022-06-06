use std::{
    cmp::min,
    collections::{HashMap, HashSet},
    sync::{Arc, Mutex, RwLock},
    time::{SystemTime, UNIX_EPOCH},
};

use crate::common::{
    broker::BrokerInfo,
    topic_partition::{LeaderAndIsr, TopicPartition},
};

use super::{
    config::{MAX_DELAY, MAX_LAG},
    log_manager::{Log, LogManager},
};

pub struct PartitionManager {
    partitions: RwLock<HashMap<TopicPartition, Arc<PartitionState>>>,
    log_manager: Arc<LogManager>,
}

impl PartitionManager {
    pub fn init(log_manager: Arc<LogManager>) -> PartitionManager {
        PartitionManager {
            partitions: RwLock::new(HashMap::new()),
            log_manager,
        }
    }

    pub fn get_partition_state(
        &self,
        topic_partition: &TopicPartition,
    ) -> Option<Arc<PartitionState>> {
        let r = self.partitions.read().unwrap();
        match (*r).get(topic_partition) {
            Some(partition_state) => Some(partition_state.clone()),
            None => None,
        }
    }

    pub fn get_all_leaders(&self) -> Vec<(TopicPartition, Arc<PartitionState>)> {
        let r = self.partitions.read().unwrap();
        let mut partition_states = Vec::new();
        for (topic_partition, partition_state) in &(*r) {
            if partition_state.leader.is_none() {
                partition_states.push((topic_partition.clone(), partition_state.clone()));
            }
        }
        partition_states
    }

    pub fn set_leader_partition(
        &self,
        topic_partition: TopicPartition,
        leader_and_isr: LeaderAndIsr,
    ) -> bool {
        let mut w = self.partitions.write().unwrap();
        let log = self.log_manager.get_or_create_log(&topic_partition);

        if let Some(partition_state) = (*w).get(&topic_partition) {
            // Only update if the version of the request is larger
            if !partition_state.check_and_update_version(&leader_and_isr, true) {
                return false;
            }
        }

        println!("----- LEADER -----");
        println!(
            "topic_partition: {:?}, leader_and_isr: {:?}",
            topic_partition, leader_and_isr
        );
        println!();

        // Since the followers also poll, replacing is fine.
        (*w).insert(
            topic_partition,
            Arc::new(PartitionState::init_leader(leader_and_isr, log)),
        );

        true
    }

    pub fn set_follower_partition(
        &self,
        topic_partition: TopicPartition,
        leader: BrokerInfo,
        leader_and_isr: LeaderAndIsr,
    ) -> bool {
        let mut w = self.partitions.write().unwrap();
        let log = self.log_manager.get_or_create_log(&topic_partition);

        if let Some(partition_state) = (*w).get(&topic_partition) {
            // Only update if the version of the request is larger
            if !partition_state.check_and_update_version(&leader_and_isr, false) {
                return false;
            }
        }

        println!("----- FOLLOW -----");
        println!(
            "topic_partition: {:?}, leader_and_isr: {:?}",
            topic_partition, leader_and_isr
        );
        println!();

        // We only care about the leader and versions when following, so replacing is fine
        (*w).insert(
            topic_partition,
            Arc::new(PartitionState::init_follower(leader, leader_and_isr, log)),
        );

        true
    }

    pub fn get_leader_info(&self, topic_partition: &TopicPartition) -> Option<BrokerInfo> {
        let r = self.partitions.read().unwrap();
        match (*r).get(topic_partition) {
            Some(partition_state) => partition_state.leader.clone(),
            None => None,
        }
    }

    pub fn is_leader(&self, topic_partition: &TopicPartition) -> bool {
        let r = self.partitions.read().unwrap();
        match (*r).get(topic_partition) {
            Some(partition_state) => partition_state.leader.is_none(),
            None => false,
        }
    }

    pub fn is_follower(&self, topic_partition: &TopicPartition) -> bool {
        let r = self.partitions.read().unwrap();
        match (*r).get(topic_partition) {
            Some(partition_state) => partition_state.leader.is_some(),
            None => false,
        }
    }
}

pub struct PartitionState {
    leader: Option<BrokerInfo>,
    followers_last_fetch: RwLock<HashMap<u32, u128>>,
    followers_last_offset: RwLock<HashMap<u32, u64>>,
    isr: RwLock<HashSet<u32>>,
    controller_epoch: Mutex<u128>,
    leader_epoch: Mutex<u128>,
    log: Arc<Log>,
}

impl PartitionState {
    pub fn init_leader(leader_and_isr: LeaderAndIsr, log: Arc<Log>) -> PartitionState {
        PartitionState {
            leader: None,
            followers_last_fetch: RwLock::new(HashMap::new()),
            followers_last_offset: RwLock::new(HashMap::new()),
            isr: RwLock::new(HashSet::from_iter(leader_and_isr.isr)),
            controller_epoch: Mutex::new(leader_and_isr.controller_epoch),
            leader_epoch: Mutex::new(leader_and_isr.leader_epoch),
            log,
        }
    }

    pub fn init_follower(
        leader: BrokerInfo,
        leader_and_isr: LeaderAndIsr,
        log: Arc<Log>,
    ) -> PartitionState {
        PartitionState {
            leader: Some(leader),
            followers_last_fetch: RwLock::new(HashMap::new()),
            followers_last_offset: RwLock::new(HashMap::new()),
            isr: RwLock::new(HashSet::new()),
            controller_epoch: Mutex::new(leader_and_isr.controller_epoch),
            leader_epoch: Mutex::new(leader_and_isr.leader_epoch),
            log,
        }
    }

    pub fn update_follower_offset(&self, follower_id: u32, offset: u64) {
        let mut w = self.followers_last_offset.write().unwrap();
        (*w).insert(follower_id, offset);
    }

    pub fn update_follower_fetch(&self, follower_id: u32, time: u128) {
        let mut w = self.followers_last_fetch.write().unwrap();
        (*w).insert(follower_id, time);
    }

    pub fn check_and_update_version(&self, leader_and_isr: &LeaderAndIsr, update: bool) -> bool {
        let mut gc = self.controller_epoch.lock().unwrap();
        let mut gl = self.leader_epoch.lock().unwrap();

        // Consider invalid update if the request has any small epochs or if it's exactly the same
        if leader_and_isr.controller_epoch < (*gc)
            || leader_and_isr.leader_epoch < (*gl)
            || (leader_and_isr.controller_epoch == (*gc) && leader_and_isr.leader_epoch == (*gl))
        {
            return false;
        }

        if update {
            *gc = leader_and_isr.controller_epoch;
            *gl = leader_and_isr.leader_epoch;
        }

        true
    }

    pub fn get_current_version(&self) -> (u128, u128) {
        let gc = self.controller_epoch.lock().unwrap();
        let gl = self.leader_epoch.lock().unwrap();
        (*gc, *gl)
    }

    pub fn update_isr(&self) -> bool {
        let curr_time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis();

        self._update_isr(curr_time, MAX_LAG, MAX_DELAY)
    }

    pub fn _update_isr(&self, curr_time: u128, max_lag: u128, max_delay: u128) -> bool {
        let ro = self.followers_last_offset.read().unwrap();
        let rf = self.followers_last_fetch.read().unwrap();
        let mut wi = self.isr.write().unwrap();

        let mut new_isr_set = HashSet::new();

        // followers should belong in isr if they've fetched something that is more
        // recent than the maximum lag time and if the last fetch performed was within
        // MAX_DELAY.
        // again we're just replacing the isr set instead of using deltas to make my life easier
        for (f_id, offset) in &(*ro) {
            let t = self.log.get_create_time(*offset);
            if let Some(l_f) = (*rf).get(f_id) {
                if t != u128::min_value()
                    && (t == u128::max_value() || curr_time - t < max_lag)
                    && curr_time - l_f < max_delay
                {
                    new_isr_set.insert(*f_id);
                }
            }
        }

        if *wi != new_isr_set {
            (*wi) = new_isr_set;
            true
        } else {
            false
        }
    }

    pub fn get_isr(&self) -> HashSet<u32> {
        let r = self.isr.read().unwrap();
        (*r).clone()
    }

    /// used to check if an append has been properly acknowledged and find a new high watermark
    /// checkpoint
    pub fn get_isr_ack(&self) -> u64 {
        let rf = self.followers_last_offset.read().unwrap();
        let ri = self.isr.read().unwrap();

        let mut min_offset = u64::max_value();
        for id in &(*ri) {
            if let Some(offset) = (*rf).get(id) {
                min_offset = min(min_offset, *offset);
            } else {
                min_offset = u64::min_value();
            }
        }

        min_offset
    }

    /// used to check if an append has been properly acknowledged and find a new high watermark
    /// checkpoint
    pub fn get_max_isr_ack(&self) -> u64 {
        let rf = self.followers_last_offset.read().unwrap();
        let ri = self.isr.read().unwrap();

        if (*ri).len() == 0 {
            return u64::max_value();
        }

        let mut max_offset = u64::min_value();
        for id in &(*ri) {
            if let Some(offset) = (*rf).get(id) {
                max_offset = min(max_offset, *offset);
            }
        }

        max_offset
    }

    pub fn get_log(&self) -> Arc<Log> {
        self.log.clone()
    }
}

#[cfg(test)]
mod partition_state_tests {
    use std::{collections::HashSet, sync::Arc};

    use crate::{
        common::topic_partition::LeaderAndIsr,
        core::{config::DEFAULT_SEGMENT_SIZE, log_manager::Log},
    };

    use super::PartitionState;

    fn serialize(message: &str) -> Vec<u8> {
        let mut serialized = Vec::new();
        serialized.append(&mut u32::to_be_bytes(message.len() as u32).to_vec());
        serialized.append(&mut String::into_bytes(message.to_string()));
        serialized
    }

    fn construct_message_set(messages: Vec<&str>) -> Vec<u8> {
        let mut message_set = Vec::new();
        for message in messages {
            message_set.append(&mut serialize(message));
        }
        message_set
    }

    #[test]
    fn update_isr_same() {
        let log = Log::init_test(DEFAULT_SEGMENT_SIZE);
        let isr = vec![1, 2, 3];
        let l_and_i = LeaderAndIsr::init(0, isr.clone(), 0, 0);
        let partition_state = PartitionState::init_leader(l_and_i, Arc::new(log));
        let t = 10;

        for id in &isr {
            partition_state.update_follower_offset(*id, 0);
            partition_state.update_follower_fetch(*id, t);
        }

        assert!(!partition_state._update_isr(t, u128::max_value(), u128::max_value()));
        assert_eq!(partition_state.get_isr(), HashSet::from_iter(isr));
    }

    #[test]
    fn update_isr_shrink_offset() {
        let log = Arc::new(Log::init_test(DEFAULT_SEGMENT_SIZE));
        let isr = vec![1, 2, 3];
        let l_and_i = LeaderAndIsr::init(0, isr.clone(), 0, 0);
        let partition_state = PartitionState::init_leader(l_and_i, log.clone());
        let t1 = 10;
        let t2 = 11;

        let msg_set1 = construct_message_set(vec!["msg1"]);
        let msg_set2 = construct_message_set(vec!["msg2"]);

        log._append_messages(msg_set1, t1);
        log._append_messages(msg_set2, t2);

        for id in 1..3 {
            partition_state.update_follower_offset(id, 0);
            partition_state.update_follower_fetch(id, t2);
        }
        partition_state.update_follower_offset(3, 8);
        partition_state.update_follower_fetch(3, t2);

        assert!(partition_state._update_isr(t2, 1, u128::max_value()));
        assert_eq!(partition_state.get_isr(), HashSet::from_iter(vec![3]));
    }

    #[test]
    fn update_isr_expand_offset() {
        let log = Arc::new(Log::init_test(DEFAULT_SEGMENT_SIZE));
        let isr = vec![1, 2, 3];
        let l_and_i = LeaderAndIsr::init(0, isr.clone(), 0, 0);
        let partition_state = PartitionState::init_leader(l_and_i, log.clone());
        let t1 = 10;
        let t2 = 11;
        let t3 = 12;

        let msg_set1 = construct_message_set(vec!["msg1"]);
        let msg_set2 = construct_message_set(vec!["msg2"]);

        log._append_messages(msg_set1, t1);
        log._append_messages(msg_set2, t2);

        for id in 1..3 {
            partition_state.update_follower_offset(id, 0);
            partition_state.update_follower_fetch(id, t2);
        }
        partition_state.update_follower_offset(3, 8);
        partition_state.update_follower_fetch(3, t2);

        assert!(partition_state._update_isr(t2, 1, u128::max_value()));
        assert_eq!(partition_state.get_isr(), HashSet::from_iter(vec![3]));

        for id in 1..4 {
            partition_state.update_follower_offset(id, 16);
            partition_state.update_follower_fetch(id, t3);
        }

        assert!(partition_state._update_isr(t3, 1, u128::max_value()));
        assert_eq!(partition_state.get_isr(), HashSet::from_iter(isr));
    }

    #[test]
    fn update_isr_shrink_fetch() {
        let log = Arc::new(Log::init_test(DEFAULT_SEGMENT_SIZE));
        let isr = vec![1, 2, 3];
        let l_and_i = LeaderAndIsr::init(0, isr.clone(), 0, 0);
        let partition_state = PartitionState::init_leader(l_and_i, log.clone());
        let t1 = 10;
        let t2 = 11;
        let t3 = 12;

        let msg_set1 = construct_message_set(vec!["msg1"]);
        let msg_set2 = construct_message_set(vec!["msg2"]);

        log._append_messages(msg_set1, t1);
        log._append_messages(msg_set2, t2);

        for id in 1..3 {
            partition_state.update_follower_offset(id, 0);
            partition_state.update_follower_fetch(id, t3);
        }
        partition_state.update_follower_offset(3, 8);
        partition_state.update_follower_fetch(3, t2);

        assert!(partition_state._update_isr(t3, u128::max_value(), 1));
        assert_eq!(partition_state.get_isr(), HashSet::from_iter(vec![1, 2]));
    }

    #[test]
    fn update_isr_expand_fetch() {
        let log = Arc::new(Log::init_test(DEFAULT_SEGMENT_SIZE));
        let isr = vec![1, 2, 3];
        let l_and_i = LeaderAndIsr::init(0, isr.clone(), 0, 0);
        let partition_state = PartitionState::init_leader(l_and_i, log.clone());
        let t1 = 10;
        let t2 = 11;
        let t3 = 12;

        let msg_set1 = construct_message_set(vec!["msg1"]);
        let msg_set2 = construct_message_set(vec!["msg2"]);

        log._append_messages(msg_set1, t1);
        log._append_messages(msg_set2, t2);

        for id in 1..3 {
            partition_state.update_follower_offset(id, 0);
            partition_state.update_follower_fetch(id, t3);
        }
        partition_state.update_follower_offset(3, 8);
        partition_state.update_follower_fetch(3, t2);

        assert!(partition_state._update_isr(t3, u128::max_value(), 1));
        assert_eq!(partition_state.get_isr(), HashSet::from_iter(vec![1, 2]));

        partition_state.update_follower_offset(3, 16);
        partition_state.update_follower_fetch(3, t3);

        assert!(partition_state._update_isr(t3, u128::max_value(), 1));
        assert_eq!(partition_state.get_isr(), HashSet::from_iter(vec![1, 2, 3]));
    }

    #[test]
    pub fn get_isr_ack() {
        let log = Arc::new(Log::init_test(DEFAULT_SEGMENT_SIZE));
        let isr = vec![2, 3, 4];
        let l_and_i = LeaderAndIsr::init(0, isr.clone(), 0, 0);
        let partition_state = PartitionState::init_leader(l_and_i, log.clone());

        for id in 1..=4 {
            partition_state.update_follower_offset(id, 8 * id as u64);
        }

        assert_eq!(partition_state.get_isr_ack(), 16);
    }

    #[test]
    pub fn get_isr_ack_empty() {
        let log = Arc::new(Log::init_test(DEFAULT_SEGMENT_SIZE));
        let isr = vec![];
        let l_and_i = LeaderAndIsr::init(0, isr.clone(), 0, 0);
        let partition_state = PartitionState::init_leader(l_and_i, log.clone());

        assert_eq!(partition_state.get_isr_ack(), u64::max_value());
    }

    #[test]
    pub fn check_or_update_version() {
        let log = Arc::new(Log::init_test(DEFAULT_SEGMENT_SIZE));
        let isr = vec![];
        let old = LeaderAndIsr::init(0, isr.clone(), 0, 0);
        let partition_state = PartitionState::init_leader(old, log.clone());

        let new = LeaderAndIsr::init(0, isr.clone(), 1, 0);
        assert!(partition_state.check_and_update_version(&new, false));

        let new = LeaderAndIsr::init(0, isr.clone(), 0, 1);
        assert!(partition_state.check_and_update_version(&new, false));

        let new = LeaderAndIsr::init(0, isr.clone(), 1, 1);
        assert!(partition_state.check_and_update_version(&new, false));
    }

    #[test]
    pub fn check_or_update_version_old() {
        let log = Arc::new(Log::init_test(DEFAULT_SEGMENT_SIZE));
        let isr = vec![];
        let old = LeaderAndIsr::init(0, isr.clone(), 1, 1);
        let partition_state = PartitionState::init_leader(old, log.clone());

        let new = LeaderAndIsr::init(0, isr.clone(), 1, 0);
        assert!(!partition_state.check_and_update_version(&new, false));

        let new = LeaderAndIsr::init(0, isr.clone(), 0, 1);
        assert!(!partition_state.check_and_update_version(&new, false));
    }
}
