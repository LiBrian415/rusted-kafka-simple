use std::sync::Arc;

use crate::{
    controller::cache::Cache,
    zk::{zk_data::BrokerIdsZNode, zk_watcher::ZkChildChangeHandler},
};

pub struct BrokerWatcher {
    cache: Arc<Cache>,
}

impl BrokerWatcher {
    pub fn init(cache: Arc<Cache>) -> BrokerWatcher {
        BrokerWatcher { cache }
    }
}

impl ZkChildChangeHandler for BrokerWatcher {
    fn path(&self) -> String {
        BrokerIdsZNode::path()
    }

    fn handle_child_change(&self) {
        self.cache.handle_brokers_change();
    }
}
