use crate::{
    core::err::ReplicaResult,
    kafka_client::KafkaClient,
    kafka_server::{KafkaServer, ServerConfig},
};

pub async fn serve_broker(config: ServerConfig) -> ReplicaResult<()> {
    KafkaServer::startup(config).await
}

pub fn new_kafka_client(addr: String) -> KafkaClient {
    KafkaClient::init(addr)
}
