use std::{
    collections::HashMap,
    sync::{Arc, RwLock},
};

use tokio::{task::JoinHandle, time};

use crate::{
    common::topic_partition::LeaderAndIsr,
    core::{ack_manager::AckManager, config::ISR_INTERVAL, partition_manager::PartitionManager},
    zk::zk_client::KafkaZkClient,
};

pub fn isr_update_task(
    broker_id: u32,
    controller_epoch: Arc<RwLock<Option<u128>>>,
    partition_manager: Arc<PartitionManager>,
    ack_manager: Arc<AckManager>,
    zk_client: Arc<KafkaZkClient>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut interval = time::interval(time::Duration::from_millis(ISR_INTERVAL));
        let mut updates = HashMap::new();

        loop {
            interval.tick().await;

            let partition_states = partition_manager.get_all_leaders();
            for (topic_partition, partition_state) in partition_states {
                if partition_state.update_isr() {
                    // update zk partition_state
                    let updated_isr = partition_state.get_isr().into_iter().collect();
                    let (controller_epoch, leader_epoch) = partition_state.get_current_version();
                    updates.insert(
                        topic_partition.clone(),
                        LeaderAndIsr::init(broker_id, updated_isr, controller_epoch, leader_epoch),
                    );

                    // isr changed so isr acks might've also changed
                    let min_isr_ack = partition_state.get_isr_ack();
                    let max_isr_ack = partition_state.get_max_isr_ack();
                    if let Some(ack_handler) = ack_manager.get_handler(&topic_partition) {
                        ack_handler.notify(min_isr_ack, max_isr_ack)
                    }
                }
            }

            // update zk partition states if necessary
            if updates.len() > 0 {
                println!("----- ISR UPDATE -----");
                println!("{:?}", updates);
                println!();

                let expected_controlled_epoch = {
                    let r = controller_epoch.read().unwrap();
                    *r
                };
                if let Some(epoch) = expected_controlled_epoch {
                    if let Ok(true) = zk_client.set_leader_and_isr(updates.clone(), epoch) {
                        // If successfully updates zk, then we can clear updates
                        updates = HashMap::new();
                    }
                }
            }
        }
    })
}
