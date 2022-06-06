use std::{
    collections::{HashMap, VecDeque},
    sync::{Arc, Mutex},
};

use tokio::{sync::Notify, task::JoinHandle};

use crate::{
    common::{
        broker::BrokerInfo,
        topic_partition::{LeaderAndIsr, TopicPartition},
    },
    core::replica_manager::ReplicaManager,
    kafka_client::KafkaClient,
    new_kafka_client,
};

pub struct RequestsManager {
    broker_info: BrokerInfo,
    replica_manager: Arc<ReplicaManager>,
    notify: Arc<Notify>,
    leader_and_follow: Arc<
        Mutex<
            VecDeque<(
                u32,
                HashMap<TopicPartition, LeaderAndIsr>,
                HashMap<TopicPartition, LeaderAndIsr>,
            )>,
        >,
    >,
    kafka_clients: Arc<Mutex<HashMap<u32, Arc<KafkaClient>>>>,
    task: JoinHandle<()>,
}

impl RequestsManager {
    pub fn init(info: BrokerInfo, replica_manager: Arc<ReplicaManager>) -> RequestsManager {
        let notify = Arc::new(Notify::new());
        let leader_and_follow = Arc::new(Mutex::new(VecDeque::new()));
        let kafka_clients = Arc::new(Mutex::new(HashMap::new()));

        RequestsManager {
            broker_info: info.clone(),
            replica_manager: replica_manager.clone(),
            notify: notify.clone(),
            leader_and_follow: leader_and_follow.clone(),
            kafka_clients: kafka_clients.clone(),
            task: RequestsManager::send_task(
                info.clone(),
                notify.clone(),
                replica_manager.clone(),
                leader_and_follow.clone(),
                kafka_clients.clone(),
            ),
        }
    }

    pub fn send_task(
        broker_info: BrokerInfo,
        notify: Arc<Notify>,
        replica_manager: Arc<ReplicaManager>,
        leader_and_follow: Arc<
            Mutex<
                VecDeque<(
                    u32,
                    HashMap<TopicPartition, LeaderAndIsr>,
                    HashMap<TopicPartition, LeaderAndIsr>,
                )>,
            >,
        >,
        kafka_clients: Arc<Mutex<HashMap<u32, Arc<KafkaClient>>>>,
    ) -> JoinHandle<()> {
        tokio::spawn(async move {
            loop {
                notify.notified().await;

                let requests;
                {
                    let mut g = leader_and_follow.lock().unwrap();
                    requests = (*g).clone();
                    (*g) = VecDeque::new();
                }

                for (id, leader, follow) in requests {
                    if id == broker_info.id {
                        let _ = replica_manager.update_leader_or_follower(leader, follow);
                    } else {
                        let client;
                        {
                            let g = kafka_clients.lock().unwrap();
                            client = (*g).get(&id).unwrap().clone();
                        }

                        let _ = client.set_leader_or_follow(leader, follow).await;
                    }
                }
            }
        })
    }

    pub fn add_client(&self, info: BrokerInfo) {
        let mut g = self.kafka_clients.lock().unwrap();
        (*g).insert(info.id, Arc::new(new_kafka_client(info.addr())));
    }

    pub fn queue_leader_and_follow(
        &self,
        requests: HashMap<
            u32,
            (
                HashMap<TopicPartition, LeaderAndIsr>,
                HashMap<TopicPartition, LeaderAndIsr>,
            ),
        >,
    ) {
        let mut g = self.leader_and_follow.lock().unwrap();
        for (id, request) in requests {
            (*g).push_back((id, request.0, request.1));
        }
        self.notify.notify_one();
    }
}
