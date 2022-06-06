use std::sync::{Arc, Mutex};

use crate::{
    common::{broker::BrokerInfo, topic_partition::TopicPartition},
    core::replica_manager::ReplicaManager,
    zk::zk_client::KafkaZkClient,
};

use super::{
    cache::Cache,
    err::{ControllerError, ControllerResult},
    watcher::{broker_watcher::BrokerWatcher, controller_watcher::ControllerWatcher},
};

pub struct Controller {
    broker_info: BrokerInfo,
    replica_manager: Arc<ReplicaManager>,
    zk_client: Arc<KafkaZkClient>,
    is_controller: Mutex<bool>,
    cache: Arc<Cache>,
}

impl Controller {
    pub fn init(
        broker_info: BrokerInfo,
        replica_manager: Arc<ReplicaManager>,
        zk_client: Arc<KafkaZkClient>,
    ) -> Arc<Controller> {
        let controller = Arc::new(Controller {
            broker_info: broker_info.clone(),
            replica_manager: replica_manager.clone(),
            zk_client: zk_client.clone(),
            is_controller: Mutex::new(false),
            cache: Cache::init(
                broker_info.clone(),
                zk_client.clone(),
                replica_manager.clone(),
            ),
        });

        zk_client
            .register_znode_change_handler(Arc::new(ControllerWatcher::init(controller.clone())));

        controller
    }

    pub fn start(&self) -> ControllerResult<()> {
        // 0) Make sure persistent registries are setup
        self.zk_client.create_top_level_paths();

        // 1) Register broker
        self.zk_client.register_broker(self.broker_info.clone())?;

        // 2) Get or register controller
        self.check_or_set_controller()?;

        Ok(())
    }

    pub fn check_or_set_controller(&self) -> ControllerResult<()> {
        // Try to race to become the next controller
        // 1) Set controller if necessary
        let _ = self
            .zk_client
            .register_controller_and_increment_controller_epoch(self.broker_info.id);

        let curr_epoch = self.zk_client.get_controller_epoch()?.unwrap().0;

        if let Some(controller_id) = self.zk_client.get_controller_id()? {
            // 2) Update local controller_epoch
            self.replica_manager.update_controller_epoch(curr_epoch);

            // If the broker is the controller, then we'll need some additional steps
            if self.broker_info.id == controller_id {
                // 0) flag self as controller
                {
                    let mut g = self.is_controller.lock().unwrap();
                    (*g) = true;
                }

                // 1) start controller-related tasks
                self.cache.start(curr_epoch);

                // 2) Setup brokers watcher
                self.zk_client
                    .register_znode_child_change_handler(Arc::new(BrokerWatcher::init(
                        self.cache.clone(),
                    )));

                // 3) Load the cache
                self.cache.load()?;
            }

            Ok(())
        } else {
            Err(Box::new(ControllerError::MissingController()))
        }
    }

    pub fn create_topic(
        &self,
        topic: String,
        partitions: u32,
        replicas: u32,
    ) -> ControllerResult<()> {
        // Make sure broker is the controller
        {
            let g = self.is_controller.lock().unwrap();
            if !(*g) {
                return Err(Box::new(ControllerError::NotTheController(
                    self.broker_info.id,
                )));
            }
        }

        // Create topic_replica
        self.cache.create_topic(topic, partitions, replicas)
    }

    pub fn get_leader_info(
        &self,
        topic_partition: &TopicPartition,
    ) -> ControllerResult<BrokerInfo> {
        let leader_and_isr = self.zk_client.get_leader_and_isr(topic_partition)?;
        let broker_info = self.zk_client.get_broker(leader_and_isr.leader)?;
        match broker_info {
            Some(info) => Ok(info),
            None => Err(Box::new(ControllerError::MissingBroker(
                leader_and_isr.leader,
            ))),
        }
    }

    pub fn get_controller_info(&self) -> ControllerResult<BrokerInfo> {
        if let Some(controller_id) = self.zk_client.get_controller_id()? {
            let broker_info = self.zk_client.get_broker(controller_id)?;
            match broker_info {
                Some(info) => Ok(info),
                None => Err(Box::new(ControllerError::MissingBroker(controller_id))),
            }
        } else {
            Err(Box::new(ControllerError::MissingController()))
        }
    }

    pub fn get_partitions_for_topic(&self, topic: &str) -> ControllerResult<Vec<u32>> {
        match self.zk_client.get_partitions_for_topic(topic) {
            Ok(partitions) => Ok(partitions),
            Err(_) => Err(Box::new(ControllerError::MissingTopic(topic.to_string()))),
        }
    }
}
