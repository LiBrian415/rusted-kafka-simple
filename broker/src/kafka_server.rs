use std::{collections::HashMap, net::ToSocketAddrs, sync::Arc, time::Duration};

use async_trait::async_trait;
use tokio::time::sleep;
use tonic::{transport::Server, Response};

use crate::{
    broker::{
        broker_server::{Broker, BrokerServer},
        BrokerInfo as RPCBrokerInfo, ConsumerInput, ConsumerOutput, LeaderOrFollow, Partitions,
        ProducerInput, Topic, TopicInput, TopicPartition as RPCTopicPartition, Void,
    },
    common::{
        broker::BrokerInfo,
        topic_partition::{LeaderAndIsr, TopicPartition},
    },
    controller::controller::Controller,
    core::{err::ReplicaResult, log_manager::LogManager, replica_manager::ReplicaManager},
    zk::zk_client::KafkaZkClient,
};

pub struct ServerConfig {
    id: u32,
    hostname: String,
    port: String,
    zk_addr: String,
}

impl ServerConfig {
    pub fn init(id: u32, hostname: String, port: String, zk_addr: String) -> ServerConfig {
        ServerConfig {
            id,
            hostname,
            port,
            zk_addr,
        }
    }
}

pub struct KafkaServer {
    replica_manager: Arc<ReplicaManager>,
    controller: Arc<Controller>,
}

impl KafkaServer {
    pub fn init(replica_manager: Arc<ReplicaManager>, controller: Arc<Controller>) -> KafkaServer {
        KafkaServer {
            replica_manager,
            controller,
        }
    }

    pub async fn startup(config: ServerConfig) -> ReplicaResult<()> {
        // init
        let broker_info = BrokerInfo::init(&config.hostname, &config.port, config.id);

        let log_manager = Arc::new(LogManager::init());
        let zk_client = Arc::new(KafkaZkClient::init(
            &config.zk_addr,
            Duration::from_secs(15),
        )?);
        let replica_manager =
            ReplicaManager::init(config.id, None, log_manager.clone(), zk_client.clone());
        let controller = Controller::init(
            broker_info.clone(),
            replica_manager.clone(),
            zk_client.clone(),
        );
        let server = KafkaServer::init(replica_manager.clone(), controller.clone());

        // Setup rpc server
        let mut addrs = format!("{}:{}", config.hostname, config.port).to_socket_addrs()?;
        let addr = addrs.next().unwrap();
        let server = tokio::spawn(async move {
            let svc = BrokerServer::new(server);
            let _ = Server::builder().add_service(svc).serve(addr).await;
        });

        // Just wait 10s to make sure the rpc server is running
        sleep(Duration::from_secs(10)).await;

        controller.start()?;

        let _ = server.await;

        Ok(())
    }
}

#[async_trait]
impl Broker for KafkaServer {
    async fn get_controller(
        &self,
        _request: tonic::Request<Void>,
    ) -> Result<tonic::Response<RPCBrokerInfo>, tonic::Status> {
        match self.controller.get_controller_info() {
            Ok(controller_info) => {
                let rpc_info = RPCBrokerInfo {
                    hostname: controller_info.hostname,
                    port: controller_info.port,
                    replica_id: controller_info.id,
                };
                Ok(Response::new(rpc_info))
            }
            Err(e) => Err(tonic::Status::unknown(e.to_string())),
        }
    }

    async fn get_leader(
        &self,
        request: tonic::Request<RPCTopicPartition>,
    ) -> Result<tonic::Response<RPCBrokerInfo>, tonic::Status> {
        let input = request.into_inner();

        let topic_partition = TopicPartition::init(&input.topic, input.partition);
        match self.controller.get_leader_info(&topic_partition) {
            Ok(leader_info) => {
                let rpc_info = RPCBrokerInfo {
                    hostname: leader_info.hostname,
                    port: leader_info.port,
                    replica_id: leader_info.id,
                };
                Ok(Response::new(rpc_info))
            }
            Err(e) => Err(tonic::Status::unknown(e.to_string())),
        }
    }

    async fn create_topic(
        &self,
        request: tonic::Request<TopicInput>,
    ) -> Result<tonic::Response<Void>, tonic::Status> {
        let input = request.into_inner();

        match self
            .controller
            .create_topic(input.topic, input.partitions, input.replicas)
        {
            Ok(()) => Ok(Response::new(Void {})),
            Err(e) => Err(tonic::Status::unknown(e.to_string())),
        }
    }

    async fn produce(
        &self,
        request: tonic::Request<ProducerInput>,
    ) -> Result<tonic::Response<Void>, tonic::Status> {
        let input = request.into_inner();

        let topic_partition = TopicPartition::init(&input.topic, input.partition);

        match self
            .replica_manager
            .append_messages(
                input.required_acks as i8,
                topic_partition,
                input.message_set,
            )
            .await
        {
            Ok(()) => Ok(Response::new(Void {})),
            Err(e) => Err(tonic::Status::unknown(e.to_string())),
        }
    }

    async fn consume(
        &self,
        request: tonic::Request<ConsumerInput>,
    ) -> Result<tonic::Response<ConsumerOutput>, tonic::Status> {
        let input = request.into_inner();

        let replica_id = if input.replica_id < 0 {
            None
        } else {
            Some(input.replica_id as u32)
        };
        let topic_partition = TopicPartition::init(&input.topic, input.partition);

        match self
            .replica_manager
            .fetch_messages(replica_id, input.max_bytes, topic_partition, input.offset)
            .await
        {
            Ok((msg, wtr)) => Ok(Response::new(ConsumerOutput {
                messages: msg,
                watermark: wtr,
            })),
            Err(e) => Err(tonic::Status::unknown(e.to_string())),
        }
    }

    async fn set_leader_or_follow(
        &self,
        request: tonic::Request<LeaderOrFollow>,
    ) -> Result<tonic::Response<Void>, tonic::Status> {
        let input = request.into_inner();

        let mut leader = HashMap::new();
        let mut follow = HashMap::new();
        for (ser_topic, ser_lai) in input.leader {
            let topic_partition: TopicPartition = serde_json::from_str(&ser_topic).unwrap();
            let leader_and_isr: LeaderAndIsr = serde_json::from_str(&ser_lai).unwrap();
            leader.insert(topic_partition, leader_and_isr);
        }
        for (ser_topic, ser_lai) in input.follow {
            let topic_partition: TopicPartition = serde_json::from_str(&ser_topic).unwrap();
            let leader_and_isr: LeaderAndIsr = serde_json::from_str(&ser_lai).unwrap();
            follow.insert(topic_partition, leader_and_isr);
        }

        match self
            .replica_manager
            .update_leader_or_follower(leader, follow)
        {
            Ok(()) => Ok(Response::new(Void {})),
            Err(e) => Err(tonic::Status::unknown(e.to_string())),
        }
    }

    async fn get_partitions(
        &self,
        request: tonic::Request<Topic>,
    ) -> Result<tonic::Response<Partitions>, tonic::Status> {
        let input = request.into_inner();

        match self.controller.get_partitions_for_topic(&input.topic) {
            Ok(partitions) => {
                let partitions = Partitions {
                    partitions: partitions.clone(),
                };
                Ok(Response::new(partitions))
            }
            Err(e) => Err(tonic::Status::unknown(e.to_string())),
        }
    }
}
