use std::{env, sync::{Mutex, Arc}, collections::HashMap};
use broker::{new_kafka_client, common::{topic_partition::TopicPartition, broker::BrokerInfo}, kafka_client::KafkaClient};
use rand::{distributions::Alphanumeric, Rng};

/// Arguments:
///  - topic - Name of the topic
///  - required_acks - -1 (all), 0, or 1
///  - num_msgs
///  - batch_size
///  - broker_addr - Addr of any broker
#[tokio::main]
async fn main() {
    let args: Vec<String> = env::args().collect();
    let topic = args[1].clone();
    let required_acks = args[2].parse::<i8>().unwrap();
    let num_msgs = args[3].parse::<u32>().unwrap();
    let batch_size = args[4].parse::<u32>().unwrap();
    let broker_addr = args[5].clone();
    let client_pool = ClientPool::init();

    let client = new_kafka_client(format!("http://{}", broker_addr));

    let mut batches = Vec::new();
    // Construct messages
    for _ in 0..(num_msgs / batch_size) {
        let mut msgs = Vec::new();
        for _ in 0..batch_size {
            let msg = generate_message(100); //100 byte messages
            msgs.push(msg);
        }
        batches.push(construct_message_set(msgs));
    }
    if num_msgs % batch_size != 0 {
        let mut msgs = Vec::new();
        for _ in 0..(num_msgs % batch_size) {
            let msg = generate_message(100); //100 byte messages
            msgs.push(msg);
        }
        batches.push(construct_message_set(msgs));
    }

    // Get partitions
    let partitions = client.get_partitions(&topic).await.unwrap();
    println!("partitions for topic {}: {:?}", topic, partitions);

    // Round-robin produce
    for i in 0..batches.len() {
        println!("{}", i);
        let p = partitions[i % partitions.len()];

        // Get leader of partition
        let topic_partition = TopicPartition::init(&topic, p);
        let leader_info = client.get_leader(&topic_partition).await.unwrap();
        println!("Leader: {:?}, {}:{}", topic_partition, leader_info.hostname, leader_info.port);

        let client = client_pool.get_or_create_client(leader_info);
        client.produce(topic_partition.clone(), batches[i].clone(), required_acks).await.unwrap();
    }
}

struct ClientPool {
    clients: Mutex<HashMap<u32, Arc<KafkaClient>>>
}

impl ClientPool {
    pub fn init() -> ClientPool {
        ClientPool { clients: Mutex::new(HashMap::new()) }
    }

    pub fn get_or_create_client(&self, info: BrokerInfo) -> Arc<KafkaClient> {
        let mut g = self.clients.lock().unwrap();
        if !(*g).contains_key(&info.id) {
            (*g).insert(info.id, Arc::new(new_kafka_client(info.addr())));
        } 
        (*g).get(&info.id).unwrap().clone()
    }
}

fn generate_message(bytes: usize) -> String {
    rand::thread_rng()
        .sample_iter(&Alphanumeric)
        .take(bytes)
        .map(char::from)
        .collect()
}

fn serialize(message: &str) -> Vec<u8> {
    let mut serialized = Vec::new();
    serialized.append(&mut u32::to_be_bytes(message.len() as u32).to_vec());
    serialized.append(&mut String::into_bytes(message.to_string()));
    serialized
}

fn construct_message_set(messages: Vec<String>) -> Vec<u8> {
    let mut message_set = Vec::new();
    for message in messages {
        message_set.append(&mut serialize(&message));
    }
    message_set
}