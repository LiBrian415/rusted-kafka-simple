use std::env;
use broker::{serve_broker, ServerConfig};


/// Arguments:
///  - id - id of the broker
///  - hostname
///  - port
///  - zk_addr - addr of zookeeper node
#[tokio::main]
async fn main() {
    let args: Vec<String> = env::args().collect();
    let config = ServerConfig::init(
        args[1].parse::<u32>().unwrap(),
        args[2].clone(),
        args[3].clone(),
        args[4].clone(),
    );
    let _ = serve_broker(config).await;
}