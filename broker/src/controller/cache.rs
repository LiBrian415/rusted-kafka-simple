use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, Mutex},
};

use rand::{thread_rng, Rng};

use crate::{
    common::{
        broker::BrokerInfo,
        topic_partition::{LeaderAndIsr, ReplicaAssignment, TopicPartition},
    },
    core::replica_manager::ReplicaManager,
    zk::zk_client::KafkaZkClient,
};

use super::{
    err::{ControllerError, ControllerResult},
    requests_manager::RequestsManager,
};

pub struct Cache {
    controller_epoch: Mutex<Option<u128>>,
    brokers: Mutex<HashSet<u32>>,
    zk_client: Arc<KafkaZkClient>,
    requests_manager: RequestsManager,
}

impl Cache {
    pub fn init(
        broker_info: BrokerInfo,
        zk_client: Arc<KafkaZkClient>,
        replica_manager: Arc<ReplicaManager>,
    ) -> Arc<Cache> {
        Arc::new(Cache {
            controller_epoch: Mutex::new(None),
            brokers: Mutex::new(HashSet::new()),
            zk_client,
            requests_manager: RequestsManager::init(broker_info.clone(), replica_manager.clone()),
        })
    }

    pub fn start(&self, controller_epoch: u128) {
        {
            let mut g = self.controller_epoch.lock().unwrap();
            (*g) = Some(controller_epoch);
        }
    }

    pub fn load(&self) -> ControllerResult<()> {
        let mut bg = self.brokers.lock().unwrap();

        // Load brokers
        let brokers = self.zk_client.get_all_brokers()?;
        for b in brokers {
            if let Some(info) = b {
                (*bg).insert(info.id);
                self.requests_manager.add_client(info.clone());
            }
        }

        println!("----- LOAD -----");
        println!("{:?}", *bg);
        println!();

        // TODO: consistency check

        Ok(())
    }

    pub fn handle_brokers_change(&self) -> ControllerResult<()> {
        let mut bg = self.brokers.lock().unwrap();

        // Load brokers
        let brokers = self.zk_client.get_all_brokers()?;
        let mut brokers_set = HashSet::new();
        for b in brokers {
            if let Some(info) = b {
                brokers_set.insert(info.id);
                if !(*bg).contains(&info.id) {
                    (*bg).insert(info.id);
                    self.requests_manager.add_client(info.clone());
                }
            }
        }

        // Remove brokers
        for b in (*bg).clone() {
            if !brokers_set.contains(&b) {
                (*bg).remove(&b);
            }
        }

        println!("----- BROKER CHANGE -----");
        println!("{:?}", *bg);
        println!();

        // TODO: promote leader if necessary

        Ok(())
    }

    /// To keep things simple, we'll use random numbers
    pub fn create_topic(
        &self,
        topic: String,
        partitions: u32,
        replicas: u32,
    ) -> ControllerResult<()> {
        let bg = self.brokers.lock().unwrap();

        if ((*bg).len() as u32) < replicas {
            return Err(Box::new(ControllerError::NotEnoughBrokers()));
        }

        // Create topic_partitions assignment
        let mut topic_partitions = HashMap::new();
        let brokers: Vec<u32> = (*bg).clone().into_iter().collect();
        let st = thread_rng().gen_range(0..brokers.len());
        for i in 0..partitions as usize {
            let mut replica_set = Vec::new();
            let idx = (st + i) % brokers.len();
            for j in 0..replicas as usize {
                replica_set.push(brokers[(idx + j) % brokers.len()])
            }
            let topic_partition = TopicPartition::init(&topic, i as u32);
            topic_partitions.insert(topic_partition, replica_set);
        }

        // Set topic_partition info
        // Create topic
        let replica_assignment = ReplicaAssignment::init(
            replicas,
            topic_partitions.clone(),
            HashMap::new(),
            HashMap::new(),
        );
        self.zk_client
            .create_new_topic(topic.clone(), replica_assignment)?;

        // Init leader_and_isr and offset
        let controller_epoch = {
            let g = self.controller_epoch.lock().unwrap();
            (*g).unwrap()
        };
        let mut leader_and_isrs = HashMap::new();
        for (topic_partition, replica_set) in &topic_partitions {
            let topic_partition = topic_partition.clone();
            let leader_and_isr = LeaderAndIsr::init(replica_set[0], vec![], controller_epoch, 0);
            leader_and_isrs.insert(topic_partition, leader_and_isr);
        }
        let results = self
            .zk_client
            .create_topic_partition_state(leader_and_isrs.clone());
        for result in results {
            if let Err(e) = result {
                return Err(Box::new(e));
            }
        }
        let results = self
            .zk_client
            .create_topic_partition_offset(leader_and_isrs.keys().cloned().collect());
        for result in results {
            if let Err(e) = result {
                return Err(Box::new(e));
            }
        }

        // Notify brokers
        let mut requests = HashMap::new();
        for (topic_partition, replica_set) in &topic_partitions {
            let leader_and_isr = leader_and_isrs.get(topic_partition).unwrap().clone();

            let request = requests
                .entry(replica_set[0])
                .or_insert((HashMap::new(), HashMap::new()));
            request
                .0
                .insert(topic_partition.clone(), leader_and_isr.clone());

            for i in 1..replica_set.len() {
                let request = requests
                    .entry(replica_set[i])
                    .or_insert((HashMap::new(), HashMap::new()));
                request
                    .1
                    .insert(topic_partition.clone(), leader_and_isr.clone());
            }
        }

        self.requests_manager.queue_leader_and_follow(requests);

        Ok(())
    }
}
