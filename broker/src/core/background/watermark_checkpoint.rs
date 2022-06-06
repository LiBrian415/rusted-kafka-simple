use std::sync::Arc;

use tokio::{task::JoinHandle, time};

use crate::core::{config::WATERMARK_INTERVAL, partition_manager::PartitionManager};

pub fn watermark_cp_task(partition_manager: Arc<PartitionManager>) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut interval = time::interval(time::Duration::from_millis(WATERMARK_INTERVAL));

        loop {
            interval.tick().await;

            let partition_states = partition_manager.get_all_leaders();
            for (_topic_partition, partition_state) in partition_states {
                let min_isr_ack = partition_state.get_isr_ack();
                let log = partition_state.get_log();
                let checkpoint = if min_isr_ack == u64::max_value() {
                    log.get_log_end()
                } else {
                    min_isr_ack
                };
                log.checkpoint_high_watermark(checkpoint);
            }
        }
    })
}
