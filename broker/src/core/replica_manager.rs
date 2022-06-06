use std::{
    collections::HashMap,
    sync::{Arc, RwLock},
    time::{SystemTime, UNIX_EPOCH},
};

use tokio::task::JoinHandle;

use crate::{
    common::{
        broker::BrokerInfo,
        topic_partition::{LeaderAndIsr, TopicPartition},
    },
    zk::zk_client::KafkaZkClient,
};

use super::{
    ack_manager::AckManager,
    background::{isr_update::isr_update_task, watermark_checkpoint::watermark_cp_task},
    err::{ReplicaError, ReplicaResult},
    fetcher_manager::FetcherManager,
    log_manager::LogManager,
    partition_manager::PartitionManager,
};

/// The ReplicaManager is in-charge of managing the replica
/// information of partitions that it's in-charge of. Namely,
/// it should maintain a local view of the isr of the partitions
/// that it's the leader of as well as the offset of the partitions
/// that it's the follower of.
///
/// To track the isr of the followers of its partitions, it maintains
/// a offset tracker of followers for each partition. If they fall
/// within or outside a range, they'll be considered in/out of the isr
/// set.
pub struct ReplicaManager {
    broker_id: u32,
    controller_epoch: Arc<RwLock<Option<u128>>>,
    log_manager: Arc<LogManager>,
    zk_client: Arc<KafkaZkClient>,
    partition_manager: Arc<PartitionManager>,
    ack_manager: Arc<AckManager>,
    fetcher_manager: Arc<FetcherManager>,

    _isr_update_task: JoinHandle<()>,
    _watermark_cp_task: JoinHandle<()>,
}

impl ReplicaManager {
    pub fn init(
        broker_id: u32,
        controller_epoch: Option<u128>,
        log_manager: Arc<LogManager>,
        zk_client: Arc<KafkaZkClient>,
    ) -> Arc<ReplicaManager> {
        let partition_manager = Arc::new(PartitionManager::init(log_manager.clone()));
        let ack_manager = Arc::new(AckManager::init(log_manager.clone()));
        let controller_epoch = Arc::new(RwLock::new(controller_epoch));
        let fetcher_manager = Arc::new(FetcherManager::init(zk_client.clone()));

        let replica_manager = Arc::new(ReplicaManager {
            broker_id,
            controller_epoch: controller_epoch.clone(),
            log_manager: log_manager.clone(),
            zk_client: zk_client.clone(),
            partition_manager: partition_manager.clone(),
            ack_manager: ack_manager.clone(),
            fetcher_manager: fetcher_manager.clone(),

            _isr_update_task: isr_update_task(
                broker_id,
                controller_epoch.clone(),
                partition_manager.clone(),
                ack_manager.clone(),
                zk_client.clone(),
            ),
            _watermark_cp_task: watermark_cp_task(partition_manager.clone()),
        });

        // finish init of fetcher_manager
        fetcher_manager.set_replica_manager(replica_manager.clone());

        replica_manager
    }

    /// Used to handle replica metadata changes and assignment
    /// from the controller.
    pub fn update_leader_or_follower(
        &self,
        leader_set: HashMap<TopicPartition, LeaderAndIsr>,
        follower_set: HashMap<TopicPartition, LeaderAndIsr>,
    ) -> ReplicaResult<()> {
        for (topic_partition, leader_and_isr) in leader_set {
            // Wrong broker
            if leader_and_isr.leader != self.broker_id {
                return Err(Box::new(ReplicaError::Unknown(
                    "Invalid request leader_and_isr request".to_string(),
                )));
            }

            self.make_leader(topic_partition, leader_and_isr)?;
        }

        for (topic_partition, leader_and_isr) in follower_set {
            self.make_follower(topic_partition, leader_and_isr)?;
        }

        Ok(())
    }

    /// To make the broker a leader, it'll add/update the PartitionState
    /// to reflect this.
    ///
    /// We should also try to delete any fetcher threads that previously exists if
    /// the broker was a follower of this topic_partition for a previous view.
    pub fn make_leader(
        &self,
        topic_partition: TopicPartition,
        leader_and_isr: LeaderAndIsr,
    ) -> ReplicaResult<()> {
        if self
            .partition_manager
            .set_leader_partition(topic_partition.clone(), leader_and_isr)
        {
            self.fetcher_manager.delete_fetcher_thread(&topic_partition);
            Ok(())
        } else {
            Err(Box::new(ReplicaError::StaleEpoch(
                topic_partition.topic.clone(),
                topic_partition.partition,
            )))
        }
    }

    /// To make the broker a follower, it'll perform the following
    /// steps:
    /// 1) change the partition_state to follow (Note: make sure that the new leader is alive)
    /// 2) stop any ongoing fetcher threads for the partition
    /// 3) truncate the log to the last high watermark
    /// 4) add a watcher + start a fetch event for the new leader
    pub fn make_follower(
        &self,
        topic_partition: TopicPartition,
        leader_and_isr: LeaderAndIsr,
    ) -> ReplicaResult<()> {
        // 1)
        // Make sure that the leader actually exists
        let leader_info = self.zk_client.get_broker(leader_and_isr.leader)?;
        if leader_info.is_none() {
            return Err(Box::new(ReplicaError::InvalidLeader(leader_and_isr.leader)));
        }
        let leader_info = leader_info.unwrap();

        // change the partition_state to follow
        if !self.partition_manager.set_follower_partition(
            topic_partition.clone(),
            leader_info,
            leader_and_isr,
        ) {
            return Err(Box::new(ReplicaError::StaleEpoch(
                topic_partition.topic.clone(),
                topic_partition.partition,
            )));
        }

        // 2)
        self.fetcher_manager.delete_fetcher_thread(&topic_partition);

        // 3)
        self.log_manager
            .get_or_create_log(&topic_partition)
            .truncate();

        // 4)
        self.fetcher_manager.create_fetcher_thread(&topic_partition);

        Ok(())
    }

    pub fn update_controller_epoch(&self, controller_epoch: u128) {
        let mut w = self.controller_epoch.write().unwrap();
        if let Some(epoch) = *w {
            if epoch < controller_epoch {
                *w = Some(controller_epoch);
            }
        } else {
            *w = Some(controller_epoch);
        }
    }

    /// Fetches the desired messages from the TopicPartitions. Alongside
    /// the messages, it also returns the current high_watermark.
    ///
    /// lib - If GC is added, then for the same reasons as log_manager,
    /// this should also return the first offset of the retrieved messages.
    pub async fn fetch_messages(
        &self,
        replica_id: Option<u32>,
        fetch_max_bytes: u64,
        topic_partition: TopicPartition,
        start_offset: u64,
    ) -> ReplicaResult<(Vec<u8>, u64)> {
        if self.partition_manager.is_leader(&topic_partition) {
            let result;

            if let Some(log) = self.log_manager.get_log(&topic_partition) {
                let messages =
                    log.fetch_messages(start_offset, fetch_max_bytes, replica_id.is_none());
                let watermark = log.get_high_watermark();
                result = (messages, watermark);
            } else {
                return Err(Box::new(ReplicaError::MissingLog(
                    topic_partition.topic.clone(),
                    topic_partition.partition,
                )));
            }

            let curr_time = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis();
            if let Some(replica_id) = replica_id {
                println!("-----REPLICA FETCH-----");
                println!("topic_partition: {:?}", topic_partition);
                println!("replica_id: {}", replica_id);
                println!("offset: {}", start_offset);
                println!();

                if let Some(partition_state) =
                    self.partition_manager.get_partition_state(&topic_partition)
                {
                    partition_state.update_follower_fetch(replica_id, curr_time);
                    partition_state.update_follower_offset(replica_id, start_offset);

                    let min_isr_ack = partition_state.get_isr_ack();
                    let max_isr_ack = partition_state.get_max_isr_ack();

                    if let Some(ack_handler) = self.ack_manager.get_handler(&topic_partition) {
                        ack_handler.notify(min_isr_ack, max_isr_ack)
                    }
                }
            }

            Ok(result)
        } else {
            Err(Box::new(ReplicaError::BrokerIsNotLeader(
                topic_partition.topic.clone(),
                topic_partition.partition,
            )))
        }
    }

    /// Only allow appends to 1 partition, this simplifies the logic as we'll only need to
    /// wait for 1 partition to be properly ack'd
    pub async fn append_messages(
        &self,
        required_acks: i8,
        topic_partition: TopicPartition,
        messages: Vec<u8>,
    ) -> ReplicaResult<()> {
        if ReplicaManager::is_valid_ack(required_acks) {
            // Only append_messages if we're actually the leader
            if self.partition_manager.is_leader(&topic_partition) {
                if required_acks != 0 {
                    // Depending on required_acks decide how long we'll have to wait
                    // See observer pattern: https://github.com/lpxxn/rust-design-pattern/blob/master/behavioral/observer.rs
                    // See notify: https://docs.rs/tokio/latest/tokio/sync/struct.Notify.html

                    let ack_handler = self.ack_manager.get_or_create_handler(&topic_partition)?;
                    let notify = ack_handler.append_messages(messages, required_acks);

                    // Send notification
                    let _ = self.zk_client.set_topic_partition_offset(
                        &topic_partition.topic,
                        topic_partition.partition,
                        None,
                    )?;

                    // Check if we can wake immediately
                    let partition_state = self
                        .partition_manager
                        .get_partition_state(&topic_partition)
                        .unwrap();
                    let min_isr_ack = partition_state.get_isr_ack();
                    let max_isr_ack = partition_state.get_max_isr_ack();
                    ack_handler.notify(min_isr_ack, max_isr_ack);

                    notify.notified().await;
                } else {
                    let _ = self.append_local(&topic_partition, messages)?;

                    // Send notification
                    let _ = self.zk_client.set_topic_partition_offset(
                        &topic_partition.topic,
                        topic_partition.partition,
                        None,
                    )?;
                }

                Ok(())
            } else {
                Err(Box::new(ReplicaError::BrokerIsNotLeader(
                    topic_partition.topic.clone(),
                    topic_partition.partition,
                )))
            }
        } else {
            // Return some error
            Err(Box::new(ReplicaError::InvalidRequiredAck(required_acks)))
        }
    }

    /// Check if all nodes in the isr ack'd the
    /// replica.
    fn is_valid_ack(required_acks: i8) -> bool {
        required_acks == -1 || required_acks == 0 || required_acks == 1
    }

    /// Used by the ReplicaFetcher to get the leader of
    /// a partition
    pub fn get_leader_info(&self, topic_partition: &TopicPartition) -> ReplicaResult<BrokerInfo> {
        if let Some(leader_info) = self.partition_manager.get_leader_info(topic_partition) {
            Ok(leader_info)
        } else {
            Err(Box::new(ReplicaError::MissingPartition(
                topic_partition.topic.clone(),
                topic_partition.partition,
            )))
        }
    }

    /// Used by the ReplicaFetcher to get the log_end of
    /// a partition
    pub fn get_log_end(&self, topic_partition: &TopicPartition) -> ReplicaResult<u64> {
        let log = self.log_manager.get_log(topic_partition);
        if let Some(log) = log {
            Ok(log.get_log_end())
        } else {
            Err(Box::new(ReplicaError::MissingLog(
                topic_partition.topic.clone(),
                topic_partition.partition,
            )))
        }
    }

    /// Used by the ReplicaFetcher to update the watermark w/ the leader
    pub fn checkpoint_high_watermark(
        &self,
        topic_partition: &TopicPartition,
        watermark: u64,
    ) -> ReplicaResult<()> {
        let log = self.log_manager.get_log(topic_partition);
        if let Some(log) = log {
            Ok(log.checkpoint_high_watermark(watermark))
        } else {
            Err(Box::new(ReplicaError::MissingLog(
                topic_partition.topic.clone(),
                topic_partition.partition,
            )))
        }
    }

    pub fn get_broker_id(&self) -> u32 {
        self.broker_id
    }

    // Append the messages to the local log for the topic partition
    pub fn append_local(
        &self,
        topic_partition: &TopicPartition,
        messages: Vec<u8>,
    ) -> ReplicaResult<u64> {
        let log = self.log_manager.get_log(topic_partition);
        if let Some(log) = log {
            Ok(log.append_messages(messages))
        } else {
            Err(Box::new(ReplicaError::MissingLog(
                topic_partition.topic.clone(),
                topic_partition.partition,
            )))
        }
    }
}
