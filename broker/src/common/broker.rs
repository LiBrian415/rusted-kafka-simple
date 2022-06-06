use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Clone, Eq, PartialEq, Hash)]
pub struct BrokerInfo {
    pub hostname: String,
    pub port: String,
    pub id: u32,
}

impl BrokerInfo {
    pub fn init(hostname: &str, port: &str, id: u32) -> BrokerInfo {
        BrokerInfo {
            hostname: hostname.to_string(),
            port: port.to_string(),
            id: id,
        }
    }

    pub fn addr(&self) -> String {
        format!("http://{}:{}", self.hostname, self.port)
    }
}
