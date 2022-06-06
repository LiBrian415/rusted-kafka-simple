use std::env;
use broker::new_kafka_client;


/// Arguments:
///  - topic - Name of the topic
///  - partitions - Number of desired partitions
///  - replicas - Number of desired replicas
///  - broker_addr - Addr of any broker
#[tokio::main]
async fn main() {
    let args: Vec<String> = env::args().collect();
    let topic = args[1].clone();
    let partitions = args[2].parse::<u32>().unwrap();
    let replicas = args[3].parse::<u32>().unwrap();
    let broker_addr = args[4].clone();

    let client = new_kafka_client(format!("http://{}", broker_addr));

    // Find controller node
    let controller_info = client.get_controller().await.unwrap();
    println!("Controller: {}, {}:{}", controller_info.id, controller_info.hostname, controller_info.port);

    let controller_client = new_kafka_client(controller_info.addr());
    controller_client.create_topic(topic, partitions, replicas).await.unwrap();
}