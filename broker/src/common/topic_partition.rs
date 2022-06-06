use std::collections::HashMap;

use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Eq, PartialEq, Hash, Clone, Debug)]
pub struct TopicPartition {
    pub topic: String,
    pub partition: u32,
}

impl TopicPartition {
    pub fn init(topic: &str, partition: u32) -> TopicPartition {
        TopicPartition {
            topic: topic.to_string(),
            partition,
        }
    }
}

#[derive(Serialize, Deserialize, Clone)]
pub struct ReplicaAssignment {
    pub replicas: u32,
    pub partitions: HashMap<TopicPartition, Vec<u32>>,
    pub adding_replicas: HashMap<TopicPartition, Vec<u32>>,
    pub removing_replicas: HashMap<TopicPartition, Vec<u32>>,
}

impl ReplicaAssignment {
    pub fn init(
        replicas: u32,
        partitions: HashMap<TopicPartition, Vec<u32>>,
        adding_replicas: HashMap<TopicPartition, Vec<u32>>,
        removing_replicas: HashMap<TopicPartition, Vec<u32>>,
    ) -> ReplicaAssignment {
        ReplicaAssignment {
            replicas,
            partitions,
            adding_replicas,
            removing_replicas,
        }
    }

    pub fn serializable(&self) -> ReplicaAssignmentString {
        let mut ser_partitions = HashMap::new();
        let mut ser_adding = HashMap::new();
        let mut ser_removing = HashMap::new();

        for (topic_partition, replica_set) in &self.partitions {
            ser_partitions.insert(
                serde_json::to_string(topic_partition).unwrap(),
                replica_set.clone(),
            );
        }
        for (topic_partition, replica_set) in &self.adding_replicas {
            ser_adding.insert(
                serde_json::to_string(topic_partition).unwrap(),
                replica_set.clone(),
            );
        }
        for (topic_partition, replica_set) in &self.removing_replicas {
            ser_removing.insert(
                serde_json::to_string(topic_partition).unwrap(),
                replica_set.clone(),
            );
        }

        ReplicaAssignmentString {
            replicas: self.replicas,
            partitions: ser_partitions,
            adding_replicas: ser_adding,
            removing_replicas: ser_removing,
        }
    }

    pub fn from_serializable(replica_assignment_s: ReplicaAssignmentString) -> ReplicaAssignment {
        let mut partitions = HashMap::new();
        let mut adding = HashMap::new();
        let mut removing = HashMap::new();

        for (topic_partition, replica_set) in replica_assignment_s.partitions {
            partitions.insert(
                serde_json::from_str(&topic_partition).unwrap(),
                replica_set.clone(),
            );
        }
        for (topic_partition, replica_set) in replica_assignment_s.adding_replicas {
            adding.insert(
                serde_json::from_str(&topic_partition).unwrap(),
                replica_set.clone(),
            );
        }
        for (topic_partition, replica_set) in replica_assignment_s.removing_replicas {
            removing.insert(
                serde_json::from_str(&topic_partition).unwrap(),
                replica_set.clone(),
            );
        }

        ReplicaAssignment {
            replicas: replica_assignment_s.replicas,
            partitions,
            adding_replicas: adding,
            removing_replicas: removing,
        }
    }

    pub fn is_being_reassigned(&self) -> bool {
        !self.adding_replicas.is_empty() || !self.removing_replicas.is_empty()
    }
}

#[derive(Serialize, Deserialize)]
pub struct ReplicaAssignmentString {
    pub replicas: u32,
    pub partitions: HashMap<String, Vec<u32>>,
    pub adding_replicas: HashMap<String, Vec<u32>>,
    pub removing_replicas: HashMap<String, Vec<u32>>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct LeaderAndIsr {
    pub leader: u32,
    pub isr: Vec<u32>,
    pub controller_epoch: u128,
    pub leader_epoch: u128,
}

impl LeaderAndIsr {
    pub fn init(
        leader: u32,
        isr: Vec<u32>,
        controller_epoch: u128,
        leader_epoch: u128,
    ) -> LeaderAndIsr {
        LeaderAndIsr {
            leader,
            isr,
            controller_epoch,
            leader_epoch,
        }
    }
}
