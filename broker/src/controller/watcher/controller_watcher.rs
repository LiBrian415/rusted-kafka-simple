use std::sync::Arc;

use crate::{
    controller::controller::Controller,
    zk::{zk_data::ControllerZNode, zk_watcher::ZkChangeHandler},
};

pub struct ControllerWatcher {
    controller: Arc<Controller>,
}

impl ControllerWatcher {
    pub fn init(controller: Arc<Controller>) -> ControllerWatcher {
        ControllerWatcher { controller }
    }
}

impl ZkChangeHandler for ControllerWatcher {
    fn path(&self) -> String {
        ControllerZNode::path()
    }

    fn handle_create(&self) {}

    fn handle_delete(&self) {
        self.controller.check_or_set_controller().unwrap();
    }

    fn handle_data_change(&self) {}
}
