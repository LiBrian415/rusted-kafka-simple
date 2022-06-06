use std::{error::Error, fmt::Display};

#[derive(Debug, Clone)]
pub enum ReplicaError {
    // used when partition accessed on the Broker despite not existing
    MissingPartition(String, u32),
    // used when log doesn't exist
    MissingLog(String, u32),
    // used when the passed in ack type isn't -1, 0, or 1
    InvalidRequiredAck(i8),
    // used when follower tries to update partition
    BrokerIsNotLeader(String, u32),
    // used when leader tries to perform follower specific task
    BrokerIsNotFollower(String, u32),
    // used when the epoch of a leader_and_isr request is stale
    StaleEpoch(String, u32),
    // used when the leader doesn't exist
    InvalidLeader(u32),
    // misc
    Unknown(String),
}

impl Display for ReplicaError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let x = match self {
            ReplicaError::MissingPartition(topic, partition) => {
                format!("partition for \"{}.{}\" does not exist", topic, partition)
            }
            ReplicaError::MissingLog(topic, partition) => {
                format!("log for \"{}.{}\" does not exist", topic, partition)
            }
            ReplicaError::InvalidRequiredAck(required_acks) => {
                format!("\"{}\" required acks is invalid", required_acks)
            }
            ReplicaError::BrokerIsNotLeader(topic, partition) => {
                format!("broker is follower for \"{}.{}\"", topic, partition)
            }
            ReplicaError::BrokerIsNotFollower(topic, partition) => {
                format!("broker is leader for \"{}.{}\" ", topic, partition)
            }
            ReplicaError::StaleEpoch(topic, partition) => {
                format!(
                    "stale leader_and_isr_request found for \"{}.{}\" ",
                    topic, partition
                )
            }
            ReplicaError::InvalidLeader(leader_id) => {
                format!("leader \"{}\" doesn't exist", leader_id)
            }
            ReplicaError::Unknown(x) => format!("unknown error: {}", x),
        };
        write!(f, "{}", x)
    }
}

impl Error for ReplicaError {}

pub type ReplicaResult<T> = Result<T, Box<(dyn Error + Send + Sync)>>;
