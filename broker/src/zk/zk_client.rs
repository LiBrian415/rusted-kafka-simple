use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, RwLock},
    time::Duration,
};
use zookeeper::{Acl, CreateMode, Stat, ZkError, ZkResult, ZooKeeper, ZooKeeperExt};

use super::zk_data::{PersistentZkPaths, TopicPartitionZNode};
use super::{
    zk_data::{
        BrokerIdZNode, BrokerIdsZNode, ControllerEpochZNode, ControllerZNode,
        TopicPartitionOffsetZNode, TopicPartitionStateZNode, TopicPartitionsZNode, TopicZNode,
        TopicsZNode,
    },
    zk_helper::get_parent,
    zk_watcher::{KafkaZkHandlers, KafkaZkWatcher, ZkChangeHandler, ZkChildChangeHandler},
};
use crate::common::{
    broker::BrokerInfo,
    topic_partition::{LeaderAndIsr, ReplicaAssignment, TopicPartition},
};

pub struct KafkaZkClient {
    pub client: ZooKeeper,
    pub handlers: KafkaZkHandlers,
}

const INITIAL_CONTROLLER_EPOCH: u128 = 0;
const INITIAL_CONTROLLER_EPOCH_ZK_VERSION: i32 = 0;
const GET_REQUEST: u32 = 0;
const GET_CHILDREN_REQUEST: u32 = 1;
const EXIST_REQUEST: u32 = 2;

impl KafkaZkClient {
    pub fn init(conn_str: &str, sess_timeout: Duration) -> ZkResult<KafkaZkClient> {
        let handlers = KafkaZkHandlers {
            change_handlers: Arc::new(RwLock::new(HashMap::new())),
            child_change_handlers: Arc::new(RwLock::new(HashMap::new())),
        };

        match ZooKeeper::connect(
            conn_str,
            sess_timeout,
            KafkaZkWatcher::init(handlers.clone()),
        ) {
            Ok(client) => Ok(KafkaZkClient {
                client: client,
                handlers: handlers.clone(),
            }),
            Err(e) => Err(e),
        }
    }

    /// Ensures that the follow paths exist:
    ///  - BrokersIdZNode
    ///  - TopicsZNode
    pub fn create_top_level_paths(&self) {
        let persistent_path = PersistentZkPaths::init();
        let _: Vec<ZkResult<()>> = persistent_path
            .paths
            .iter()
            .map(|path| self.check_persistent_path(path))
            .collect();
    }

    pub fn cleanup(&self) {
        let persistent_path = PersistentZkPaths::init();
        let _: Vec<ZkResult<()>> = persistent_path
            .paths
            .iter()
            .map(|path| self.client.delete_recursive(path))
            .collect();
        let _ = self
            .client
            .delete(ControllerEpochZNode::path().as_str(), None);
    }

    fn check_persistent_path(&self, path: &str) -> ZkResult<()> {
        self.create_recursive(path, Vec::new())
    }

    fn create_recursive(&self, path: &str, data: Vec<u8>) -> ZkResult<()> {
        let res = self.client.create(
            path,
            data.clone(),
            Acl::open_unsafe().clone(),
            zookeeper::CreateMode::Persistent,
        );

        match res {
            Ok(_) => Ok(()),
            Err(e1) => match e1 {
                ZkError::NoNode => match self.recursive_create(get_parent(path).as_str()) {
                    Ok(_) => match self.client.create(
                        path,
                        data,
                        Acl::open_unsafe().clone(),
                        zookeeper::CreateMode::Persistent,
                    ) {
                        Ok(_) => Ok(()),
                        Err(e) => Err(e),
                    },
                    Err(e2) => match e2 {
                        ZkError::NodeExists => Ok(()),
                        _ => Err(e2),
                    },
                },
                ZkError::NodeExists => Ok(()),
                _ => Err(e1),
            },
        }
    }

    fn recursive_create(&self, path: &str) -> ZkResult<()> {
        if path == "" {
            return Ok(());
        }

        match self.client.create(
            path,
            Vec::new(),
            Acl::open_unsafe().clone(),
            zookeeper::CreateMode::Persistent,
        ) {
            Ok(_) => Ok(()),
            Err(e1) => match e1 {
                ZkError::NoNode => match self.recursive_create(get_parent(path).as_str()) {
                    Ok(_) => match self.client.create(
                        path,
                        Vec::new(),
                        Acl::open_unsafe().clone(),
                        zookeeper::CreateMode::Persistent,
                    ) {
                        Ok(_) => Ok(()),
                        Err(e) => Err(e),
                    },
                    Err(e2) => match e2 {
                        ZkError::NodeExists => Ok(()),
                        _ => Err(e2),
                    },
                },
                _ => Err(e1),
            },
        }
    }

    // Controller
    // TODO: double check
    pub fn register_controller_and_increment_controller_epoch(
        &self,
        controller_id: u32,
    ) -> ZkResult<(u128, i32)> {
        // read /controller_epoch to get the current controller epoch and zkVersion
        // create /controller_epoch if not exists
        let result = match self.get_controller_epoch() {
            Ok(resp) => resp,
            Err(e) => return Err(e),
        };

        let (curr_epoch, curr_epoch_zk_version) = match result {
            Some(info) => (info.0, info.1.version),
            None => {
                println!("/controller_epoch not exists, create one");
                match self.create_controller_epoch_znode() {
                    Ok(r) => r,
                    Err(e) => return Err(e),
                }
            }
        };

        // try create /controller and update /controller_epoch atomatically
        let new_controller_epoch = curr_epoch + 1;
        let expected_controller_epoch_zk_version = curr_epoch_zk_version;
        let mut check = false;

        match self.client.create(
            ControllerZNode::path().as_str(),
            ControllerZNode::encode(controller_id),
            Acl::open_unsafe().clone(),
            CreateMode::Ephemeral,
        ) {
            Ok(_) => {
                println!("broker {} wins the election", controller_id);
                match self.client.set_data(
                    ControllerEpochZNode::path().as_str(),
                    ControllerEpochZNode::encode(new_controller_epoch),
                    Some(expected_controller_epoch_zk_version),
                ) {
                    Ok(resp) => return Ok((new_controller_epoch, resp.version)),
                    Err(e) => match e {
                        ZkError::BadVersion => match self
                            .check_controller_and_epoch(controller_id, new_controller_epoch)
                        {
                            Ok(result) => {
                                println!(
                                    "broker {} wins the election but fail to set epoch",
                                    controller_id
                                );
                                check = result;
                            }
                            Err(e) => return Err(e),
                        },
                        _ => return Err(e),
                    },
                }
            }
            Err(e) => match e {
                ZkError::NodeExists => {
                    match self.check_controller_and_epoch(controller_id, new_controller_epoch) {
                        Ok(result) => {
                            println!("other brokers win the election");
                            check = result;
                        }
                        Err(e) => return Err(e),
                    }
                }
                _ => {
                    println!(
                        "broker {} fails the election because {:?}",
                        controller_id, e
                    );
                    return Err(e);
                }
            },
        };

        if check {
            match self.get_controller_epoch() {
                Ok(resp) => {
                    let result = resp.unwrap();
                    Ok((result.0, result.1.version))
                }
                Err(e) => Err(e),
            }
        } else {
            return Err(ZkError::SystemError);
        }
    }

    fn create_controller_epoch_znode(&self) -> ZkResult<(u128, i32)> {
        match self.client.create(
            ControllerEpochZNode::path().as_str(),
            ControllerEpochZNode::encode(INITIAL_CONTROLLER_EPOCH),
            Acl::open_unsafe().clone(),
            CreateMode::Persistent,
        ) {
            Ok(_) => Ok((
                INITIAL_CONTROLLER_EPOCH,
                INITIAL_CONTROLLER_EPOCH_ZK_VERSION,
            )),
            Err(e) => match e {
                ZkError::NodeExists => match self.get_controller_epoch() {
                    Ok(resp) => match resp {
                        Some(info) => Ok((info.0, info.1.version)),
                        None => Err(e),
                    },
                    Err(e) => Err(e),
                },
                _ => Err(e),
            },
        }
    }

    pub fn get_controller_epoch(&self) -> ZkResult<Option<(u128, Stat)>> {
        let path = ControllerEpochZNode::path();
        let watch = match self.should_watch(path.clone(), GET_REQUEST, true) {
            Ok(res) => res,
            Err(_) => false,
        };

        let result = self.client.get_data(path.as_str(), watch);

        match result {
            Ok(resp) => Ok(Some((ControllerEpochZNode::decode(&resp.0), resp.1))),
            Err(e) => match e {
                ZkError::NoNode => Ok(None),
                _ => return Err(e),
            },
        }
    }

    pub fn get_controller_id(&self) -> ZkResult<Option<u32>> {
        let path = ControllerZNode::path();
        let watch = match self.should_watch(path.clone(), GET_REQUEST, true) {
            Ok(res) => res,
            Err(_) => false,
        };

        let result = self.client.get_data(path.as_str(), watch);

        match result {
            Ok(resp) => Ok(Some(ControllerZNode::decode(&resp.0))),
            Err(e) => match e {
                ZkError::NoNode => Ok(None),
                _ => Err(e),
            },
        }
    }

    fn check_controller(&self, controller_id: u32) -> ZkResult<bool> {
        let curr_controller_id = match self.get_controller_id() {
            Ok(resp) => match resp {
                Some(id) => id,
                None => return Err(ZkError::NoNode),
            },
            Err(e) => return Err(e),
        };

        if controller_id == curr_controller_id {
            return Ok(true);
        } else {
            return Ok(false);
        }
    }

    fn check_epoch(&self, new_controller_epoch: u128) -> ZkResult<bool> {
        let controller_epoch = match self.get_controller_epoch() {
            Ok(resp) => match resp {
                Some(info) => info.0,
                None => return Err(ZkError::NoNode),
            },
            Err(e) => return Err(e),
        };

        if new_controller_epoch == controller_epoch {
            return Ok(true);
        } else {
            return Ok(false);
        }
    }

    fn check_controller_and_epoch(&self, id: u32, epoch: u128) -> ZkResult<bool> {
        let correct_controller = match self.check_controller(id) {
            Ok(resp) => resp,
            Err(e) => return Err(e),
        };

        if correct_controller {
            match self.check_epoch(epoch) {
                Ok(resp) => return Ok(resp),
                Err(e) => return Err(e),
            }
        }
        Ok(false)
    }

    // Broker

    pub fn register_broker(&self, broker: BrokerInfo) -> ZkResult<i64> {
        match self.client.create(
            BrokerIdZNode::path(broker.id).as_str(),
            BrokerIdZNode::encode(&broker),
            Acl::open_unsafe().clone(),
            CreateMode::Ephemeral,
        ) {
            Ok(_) => match self.get_stat_after_node_exists(BrokerIdZNode::path(broker.id).as_str())
            {
                Ok(stat) => Ok(stat.czxid),
                Err(e) => Err(e),
            },
            Err(e) => match e {
                ZkError::NodeExists => {
                    match self.get_stat_after_node_exists(BrokerIdZNode::path(broker.id).as_str()) {
                        Ok(stat) => Ok(stat.czxid),
                        Err(e) => Err(e),
                    }
                }
                _ => return Err(e),
            },
        }
    }

    fn get_stat_after_node_exists(&self, path: &str) -> ZkResult<Stat> {
        let watch = match self.should_watch(path.to_string(), GET_REQUEST, true) {
            Ok(watch) => watch,
            Err(_) => false,
        };

        let result = self.client.get_data(path, watch);

        match result {
            Ok(resp) => Ok(resp.1),
            Err(e) => Err(e),
        }
    }

    pub fn get_broker(&self, broker_id: u32) -> ZkResult<Option<BrokerInfo>> {
        let path = BrokerIdZNode::path(broker_id);
        let watch = match self.should_watch(path.clone(), GET_REQUEST, true) {
            Ok(watch) => watch,
            Err(_) => false,
        };

        let result = self.client.get_data(path.as_str(), watch);

        match result {
            Ok(data) => Ok(Some(BrokerIdZNode::decode(&data.0))),
            Err(e) => match e {
                ZkError::NoNode => Ok(None),
                _ => Err(e),
            },
        }
    }

    // TODO: add watcher setup
    pub fn get_all_brokers(&self) -> ZkResult<Vec<Option<BrokerInfo>>> {
        let mut broker_ids: Vec<u32> = match self.get_children(BrokerIdsZNode::path().as_str()) {
            Ok(list) => list.iter().map(|id| id.parse::<u32>().unwrap()).collect(),
            Err(e) => return Err(e),
        };
        broker_ids.sort();

        let mut brokers: Vec<Option<BrokerInfo>> = Vec::new();

        for id in broker_ids {
            match self.get_broker(id) {
                Ok(info) => brokers.push(info),
                Err(e) => return Err(e),
            }
        }

        Ok(brokers)
    }

    // Topic + Partition

    pub fn create_new_topic(
        &self,
        topic: String,
        replica_assignment: ReplicaAssignment,
    ) -> ZkResult<()> {
        let path = TopicZNode::path(topic.as_str());

        match self.client.create(
            path.as_str(),
            TopicZNode::encode(replica_assignment.clone()),
            Acl::open_unsafe().clone(),
            CreateMode::Persistent,
        ) {
            Ok(_) => {}
            Err(e) => return Err(e),
        }

        let _ = match self.create_topic_partitions(topic.clone()) {
            Ok(_) => {}
            Err(e) => return Err(e),
        };

        let resp =
            self.create_topic_partition(replica_assignment.partitions.keys().cloned().collect());
        for r in resp {
            match r {
                Ok(_) => {}
                Err(e) => {
                    return Err(e);
                }
            }
        }

        Ok(())
    }

    pub fn get_all_topics(&self, register_watch: bool) -> ZkResult<HashSet<String>> {
        let path = TopicsZNode::path();
        let watch = match self.should_watch(path.to_string(), GET_CHILDREN_REQUEST, register_watch)
        {
            Ok(watch) => watch,
            Err(_) => false,
        };

        let result = self.client.get_children(path.as_str(), watch);

        match result {
            Ok(resp) => Ok(HashSet::from_iter(resp.iter().cloned())),
            Err(e) => match e {
                ZkError::NoNode => Ok(HashSet::new()),
                _ => Err(e),
            },
        }
    }

    fn create_topic_partitions(&self, topic: String) -> ZkResult<String> {
        self.client.create(
            TopicPartitionsZNode::path(&topic).as_str(),
            Vec::new(),
            Acl::open_unsafe().clone(),
            CreateMode::Persistent,
        )
    }

    fn create_topic_partition(&self, partitions: Vec<TopicPartition>) -> Vec<ZkResult<String>> {
        let resps: Vec<ZkResult<String>> = partitions
            .iter()
            .map(|partition| {
                self.client.create(
                    TopicPartitionZNode::path(partition.topic.as_str(), partition.partition)
                        .as_str(),
                    Vec::new(),
                    Acl::open_unsafe().clone(),
                    CreateMode::Persistent,
                )
            })
            .collect();

        resps
    }

    pub fn create_topic_partition_state(
        &self,
        leader_isr_and_epoch: HashMap<TopicPartition, LeaderAndIsr>,
    ) -> Vec<ZkResult<String>> {
        leader_isr_and_epoch
            .iter()
            .map(|(partition, leader_and_isr)| {
                self.client.create(
                    TopicPartitionStateZNode::path(partition.topic.as_str(), partition.partition)
                        .as_str(),
                    TopicPartitionStateZNode::encode(leader_and_isr.clone()),
                    Acl::open_unsafe().clone(),
                    CreateMode::Persistent,
                )
            })
            .collect()
    }

    pub fn create_topic_partition_offset(
        &self,
        topic_partitions: Vec<TopicPartition>,
    ) -> Vec<ZkResult<String>> {
        topic_partitions
            .iter()
            .map(|partition| {
                self.client.create(
                    TopicPartitionOffsetZNode::path(partition.topic.as_str(), partition.partition)
                        .as_str(),
                    TopicPartitionOffsetZNode::encode(),
                    Acl::open_unsafe().clone(),
                    CreateMode::Persistent,
                )
            })
            .collect()
    }

    /// gets the partition numbers for a given topic
    pub fn get_partitions_for_topic(&self, topic: &str) -> ZkResult<Vec<u32>> {
        let partition_paths = self.get_children(&TopicPartitionsZNode::path(topic))?;
        Ok(partition_paths
            .into_iter()
            .map(|path| path.parse::<u32>().unwrap())
            .collect())
    }

    fn get_partition_assignment_for_topics(
        &self,
        topics: Vec<String>,
    ) -> ZkResult<HashMap<String, Option<ReplicaAssignment>>> {
        let mut partition_assignments = HashMap::new();
        let resps: Vec<ZkResult<(Vec<u8>, Stat)>> = topics
            .iter()
            .map(|topic| {
                let path = TopicZNode::path(topic);
                let watch = match self.should_watch(path.clone(), GET_REQUEST, true) {
                    Ok(resp) => resp,
                    Err(_) => false,
                };
                self.client.get_data(path.as_str(), watch)
            })
            .collect();
        let mut i = 0;

        for resp in resps {
            match resp {
                Ok(data) => {
                    partition_assignments
                        .insert(topics[i].clone(), Some(TopicZNode::decode(&data.0)));
                }
                Err(e) => match e {
                    ZkError::NoNode => {
                        partition_assignments.insert(topics[i].clone(), None);
                    }
                    _ => return Err(e),
                },
            }
            i = i + 1;
        }

        Ok(partition_assignments)
    }

    pub fn get_leader_and_isr(&self, topic_partition: &TopicPartition) -> ZkResult<LeaderAndIsr> {
        let path = TopicPartitionStateZNode::path(
            topic_partition.topic.as_str(),
            topic_partition.partition,
        );
        let watch = match self.should_watch(path.clone(), GET_REQUEST, true) {
            Ok(watch) => watch,
            Err(_) => false,
        };

        let result = self.client.get_data(path.as_str(), watch);

        match result {
            Ok(resp) => Ok(TopicPartitionStateZNode::decode(&resp.0)),
            Err(e) => return Err(e),
        }
    }

    pub fn set_leader_and_isr(
        &self,
        leader_and_isrs: HashMap<TopicPartition, LeaderAndIsr>,
        expected_controller_epoch_zk_version: u128,
    ) -> ZkResult<bool> {
        if !self.check_epoch(expected_controller_epoch_zk_version)? {
            return Ok(false);
        }

        for (partition, leader_and_isr) in leader_and_isrs.iter() {
            match self.client.set_data(
                TopicPartitionStateZNode::path(partition.topic.as_str(), partition.partition)
                    .as_str(),
                TopicPartitionStateZNode::encode(leader_and_isr.clone()),
                None,
            ) {
                Ok(_) => {}
                Err(e) => return Err(e),
            }
        }

        Ok(true)
    }

    pub fn get_topic_partition_offset(
        &self,
        topic: &str,
        partition: u32,
    ) -> ZkResult<Option<Vec<u8>>> {
        let path = TopicPartitionOffsetZNode::path(topic, partition);
        let watch = match self.should_watch(path.clone(), GET_REQUEST, true) {
            Ok(resp) => resp,
            Err(_) => false,
        };

        let result = self.client.get_data(path.as_str(), watch);

        match result {
            Ok(resp) => Ok(Some(resp.0)),
            Err(e) => match e {
                ZkError::NoNode => Ok(None),
                _ => Err(e),
            },
        }
    }

    pub fn set_topic_partition_offset(
        &self,
        topic: &str,
        partition: u32,
        version: Option<i32>,
    ) -> ZkResult<bool> {
        match self.client.set_data(
            TopicPartitionOffsetZNode::path(topic, partition).as_str(),
            TopicPartitionOffsetZNode::encode(),
            version,
        ) {
            Ok(_) => Ok(true),
            Err(e) => match e {
                ZkError::BadVersion | ZkError::NoNode => Ok(false),
                _ => Err(e),
            },
        }
    }

    pub fn get_topic_partition_states(
        &self,
        partitions: Vec<TopicPartition>,
    ) -> ZkResult<HashMap<TopicPartition, LeaderAndIsr>> {
        let mut partition_states: HashMap<TopicPartition, LeaderAndIsr> = HashMap::new();

        for partition in partitions {
            let path =
                TopicPartitionStateZNode::path(partition.topic.as_str(), partition.partition);
            let watch = match self.should_watch(path.clone(), GET_REQUEST, true) {
                Ok(watch) => watch,
                Err(_) => false,
            };
            let result = self.client.get_data(path.as_str(), watch);

            match result {
                Ok(resp) => {
                    partition_states
                        .insert(partition.clone(), TopicPartitionStateZNode::decode(&resp.0));
                }
                Err(e) => match e {
                    ZkError::NoNode => {}
                    _ => return Err(e),
                },
            };
        }

        Ok(partition_states)
    }

    // ZooKeeper

    pub fn get_children(&self, path: &str) -> ZkResult<Vec<String>> {
        let watch = match self.should_watch(path.to_string(), GET_CHILDREN_REQUEST, true) {
            Ok(watch) => watch,
            Err(_) => false,
        };

        let result = self.client.get_children(path, watch);

        match result {
            Ok(resp) => Ok(resp),
            Err(e) => match e {
                ZkError::NoNode => Ok(Vec::new()),
                _ => Err(e),
            },
        }
    }

    pub fn register_znode_change_handler_and_check_existence(
        &self,
        handler: Arc<dyn ZkChangeHandler>,
    ) -> ZkResult<bool> {
        self.register_znode_change_handler(handler.clone());
        match self.client.exists_w(
            handler.path().as_str(),
            KafkaZkWatcher::init(self.handlers.clone()),
        ) {
            Ok(_) => Ok(true),
            Err(e) => match e {
                ZkError::NoNode => Ok(false),
                _ => Err(e),
            },
        }
    }

    pub fn register_znode_change_handler(&self, handler: Arc<dyn ZkChangeHandler>) {
        self.handlers.register_znode_change_handler(handler);
    }

    pub fn unregister_znode_change_handler(&self, path: &str) {
        self.handlers.unregister_znode_change_handler(path);
    }

    pub fn register_znode_child_change_handler(&self, handler: Arc<dyn ZkChildChangeHandler>) {
        self.handlers.register_znode_child_change_handler(handler);
    }

    pub fn unregister_znode_child_change_handler(&self, path: &str) {
        self.handlers.unregister_znode_child_change_handler(path);
    }

    fn should_watch(&self, path: String, req_type: u32, register_watch: bool) -> ZkResult<bool> {
        match req_type {
            GET_CHILDREN_REQUEST => {
                let handlers = self.handlers.child_change_handlers.read().unwrap();
                match handlers.get(&path) {
                    Some(_) => Ok(true && register_watch),
                    None => Ok(false),
                }
            }
            GET_REQUEST | EXIST_REQUEST => {
                let handlers = self.handlers.change_handlers.read().unwrap();
                match handlers.get(&path) {
                    Some(_) => Ok(true),
                    None => Ok(false),
                }
            }
            _ => Err(ZkError::NoAuth),
        }
    }
}
