use std::{collections::HashMap, sync::Arc};

use tokio::sync::Mutex;
use tonic::transport::Channel;

use crate::{
    broker::{
        broker_client::BrokerClient, BrokerInfo as RPCBrokerInfo, ConsumerInput, LeaderOrFollow,
        ProducerInput, Topic, TopicInput, TopicPartition as RPCTopicPartition, Void,
    },
    common::{
        broker::BrokerInfo,
        topic_partition::{LeaderAndIsr, TopicPartition},
    },
    core::err::ReplicaResult,
};

pub struct KafkaClient {
    pub addr: String,
    pub client: Arc<Mutex<Option<BrokerClient<Channel>>>>,
}

impl KafkaClient {
    pub fn init(addr: String) -> KafkaClient {
        KafkaClient {
            addr,
            client: Arc::new(Mutex::new(None)),
        }
    }

    async fn connect(&self) -> ReplicaResult<BrokerClient<Channel>> {
        let mut client = self.client.lock().await;
        if let None = *client {
            *client = Some(BrokerClient::connect(self.addr.clone()).await?);
        }
        match &*client {
            Some(inner) => Ok(inner.clone()),
            None => panic!("Safe - Line 18-20 ensures that this must have a value"),
        }
    }

    pub async fn get_controller(&self) -> ReplicaResult<BrokerInfo> {
        let mut client = self.connect().await?;

        let input = Void {};

        let output: RPCBrokerInfo = client
            .get_controller(tonic::Request::new(input))
            .await?
            .into_inner();

        Ok(BrokerInfo::init(
            &output.hostname,
            &output.port,
            output.replica_id,
        ))
    }

    pub async fn get_leader(&self, topic_partition: &TopicPartition) -> ReplicaResult<BrokerInfo> {
        let mut client = self.connect().await?;

        let input: RPCTopicPartition = RPCTopicPartition {
            topic: topic_partition.topic.clone(),
            partition: topic_partition.partition,
        };

        let output: RPCBrokerInfo = client
            .get_leader(tonic::Request::new(input))
            .await?
            .into_inner();

        Ok(BrokerInfo::init(
            &output.hostname,
            &output.port,
            output.replica_id,
        ))
    }

    pub async fn create_topic(
        &self,
        topic: String,
        partitions: u32,
        replicas: u32,
    ) -> ReplicaResult<()> {
        let mut client = self.connect().await?;

        let input = TopicInput {
            topic,
            partitions,
            replicas,
        };

        client.create_topic(tonic::Request::new(input)).await?;

        Ok(())
    }

    pub async fn produce(
        &self,
        topic_partition: TopicPartition,
        msg_set: Vec<u8>,
        required_acks: i8,
    ) -> ReplicaResult<()> {
        let mut client = self.connect().await?;

        let input = ProducerInput {
            topic: topic_partition.topic.clone(),
            partition: topic_partition.partition,
            message_set: msg_set,
            required_acks: required_acks as i32,
        };

        client.produce(tonic::Request::new(input)).await?;

        Ok(())
    }

    pub async fn consume(
        &self,
        replica_id: Option<u32>,
        topic_partition: TopicPartition,
        start_offset: u64,
        max_fetch: u64,
    ) -> ReplicaResult<(Vec<u8>, u64)> {
        let mut client = self.connect().await?;
        let replica_id = if let Some(id) = replica_id {
            id as i32
        } else {
            -1
        };

        let input = ConsumerInput {
            replica_id: replica_id,
            topic: topic_partition.topic.clone(),
            partition: topic_partition.partition,
            offset: start_offset,
            max_bytes: max_fetch,
        };

        let output = client
            .consume(tonic::Request::new(input))
            .await?
            .into_inner();

        Ok((output.messages, output.watermark))
    }

    pub async fn set_leader_or_follow(
        &self,
        leader: HashMap<TopicPartition, LeaderAndIsr>,
        follower: HashMap<TopicPartition, LeaderAndIsr>,
    ) -> ReplicaResult<()> {
        let mut client = self.connect().await?;

        let mut ser_leader = HashMap::new();
        let mut ser_follow = HashMap::new();
        for (k, v) in &leader {
            ser_leader.insert(serde_json::to_string(k)?, serde_json::to_string(v)?);
        }
        for (k, v) in &follower {
            ser_follow.insert(serde_json::to_string(k)?, serde_json::to_string(v)?);
        }
        let input = LeaderOrFollow {
            leader: ser_leader,
            follow: ser_follow,
        };

        client
            .set_leader_or_follow(tonic::Request::new(input))
            .await?;

        Ok(())
    }

    pub async fn get_partitions(&self, topic: &str) -> ReplicaResult<Vec<u32>> {
        let mut client = self.connect().await?;

        let input = Topic {
            topic: topic.to_string(),
        };

        let output = client.get_partitions(tonic::Request::new(input)).await?;

        Ok(output.into_inner().partitions)
    }
}
