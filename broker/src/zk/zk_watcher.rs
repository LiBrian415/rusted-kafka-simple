use std::{
    collections::HashMap,
    sync::{Arc, RwLock},
};

use zookeeper::{WatchedEventType, Watcher};

use super::zk_data::ControllerZNode;

pub struct KafkaZkWatcher {
    pub handlers: KafkaZkHandlers,
}

/// Note: Adding a handler only registers it with the Handler object. It doesn't
/// create a Watcher. Instead, a watcher will be created for an operation if a
/// handler exists for the path.
///
/// I.e. It's better to think of handlers and watchers as notifications/interrupts
/// instead of callbacks. Namely, they should trigger a state change which will
/// causes a cache invalidation and force the Broker to retry at some later time.
#[derive(Clone)]
pub struct KafkaZkHandlers {
    pub change_handlers: Arc<RwLock<HashMap<String, Arc<dyn ZkChangeHandler>>>>,
    pub child_change_handlers: Arc<RwLock<HashMap<String, Arc<dyn ZkChildChangeHandler>>>>,
}

pub trait ZkChangeHandler: Send + Sync {
    fn path(&self) -> String;
    fn handle_create(&self);
    fn handle_delete(&self);
    fn handle_data_change(&self);
}

pub trait ZkChildChangeHandler: Send + Sync {
    fn path(&self) -> String;
    fn handle_child_change(&self);
}

impl KafkaZkWatcher {
    pub fn init(handlers: KafkaZkHandlers) -> KafkaZkWatcher {
        KafkaZkWatcher { handlers }
    }
}

impl Watcher for KafkaZkWatcher {
    // TODO: Should also handle KeeperState Changes
    fn handle(&self, event: zookeeper::WatchedEvent) {
        if let Some(path) = event.path {
            match event.event_type {
                WatchedEventType::NodeCreated => {
                    if path != ControllerZNode::path() {
                        let g = self.handlers.change_handlers.read().unwrap();
                        if let Some(handler) = (*g).get(&path) {
                            handler.handle_create();
                        }
                    }
                }
                WatchedEventType::NodeDeleted => {
                    let g = self.handlers.change_handlers.read().unwrap();
                    if let Some(handler) = (*g).get(&path) {
                        handler.handle_delete();
                    }
                }
                WatchedEventType::NodeDataChanged => {
                    let g = self.handlers.change_handlers.read().unwrap();
                    if let Some(handler) = (*g).get(&path) {
                        handler.handle_data_change();
                    }
                }
                WatchedEventType::NodeChildrenChanged => {
                    let g = self.handlers.child_change_handlers.read().unwrap();
                    if let Some(handler) = (*g).get(&path) {
                        handler.handle_child_change();
                    }
                }
                _ => {}
            }
        }
    }
}

impl KafkaZkHandlers {
    pub fn init() -> KafkaZkHandlers {
        KafkaZkHandlers {
            change_handlers: Arc::new(RwLock::new(HashMap::new())),
            child_change_handlers: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub fn register_znode_change_handler(&self, handler: Arc<dyn ZkChangeHandler>) {
        let mut g = self.change_handlers.write().unwrap();
        (*g).insert(handler.path(), handler);
    }

    pub fn unregister_znode_change_handler(&self, path: &str) {
        let mut g = self.change_handlers.write().unwrap();
        (*g).remove(path);
    }

    pub fn contains_znode_change_handler(&self, path: &str) -> bool {
        let g = self.change_handlers.read().unwrap();
        (*g).contains_key(path)
    }

    pub fn register_znode_child_change_handler(&self, handler: Arc<dyn ZkChildChangeHandler>) {
        let mut g = self.child_change_handlers.write().unwrap();
        (*g).insert(handler.path(), handler);
    }

    pub fn unregister_znode_child_change_handler(&self, path: &str) {
        let mut g = self.child_change_handlers.write().unwrap();
        (*g).remove(path);
    }

    pub fn contains_znode_child_change_handler(&self, path: &str) -> bool {
        let g = self.child_change_handlers.read().unwrap();
        (*g).contains_key(path)
    }
}

#[cfg(test)]
mod handler_tests {
    use std::sync::{Arc, Mutex};

    use zookeeper::{KeeperState, WatchedEvent, WatchedEventType, Watcher};

    use super::{KafkaZkHandlers, KafkaZkWatcher, ZkChangeHandler, ZkChildChangeHandler};

    const TEST_PATH: &str = "tmp";

    struct TestStruct {
        data: Arc<Mutex<i32>>,
    }

    impl TestStruct {
        fn init() -> TestStruct {
            TestStruct {
                data: Arc::new(Mutex::new(0)),
            }
        }
    }

    struct TestStructChangeHandler {
        data: Arc<Mutex<i32>>,
    }

    impl TestStructChangeHandler {
        pub fn init(ts: &TestStruct) -> TestStructChangeHandler {
            TestStructChangeHandler {
                data: ts.data.clone(),
            }
        }
    }

    impl ZkChangeHandler for TestStructChangeHandler {
        fn path(&self) -> String {
            TEST_PATH.to_string()
        }

        fn handle_create(&self) {
            let mut g = self.data.lock().unwrap();
            (*g) = 1;
        }

        fn handle_delete(&self) {
            let mut g = self.data.lock().unwrap();
            (*g) = 2;
        }

        fn handle_data_change(&self) {
            let mut g = self.data.lock().unwrap();
            (*g) = 3;
        }
    }

    struct TestStructChildChangeHandler {
        data: Arc<Mutex<i32>>,
    }

    impl TestStructChildChangeHandler {
        pub fn init(ts: &TestStruct) -> TestStructChildChangeHandler {
            TestStructChildChangeHandler {
                data: ts.data.clone(),
            }
        }
    }

    impl ZkChildChangeHandler for TestStructChildChangeHandler {
        fn path(&self) -> String {
            TEST_PATH.to_string()
        }

        fn handle_child_change(&self) {
            let mut g = self.data.lock().unwrap();
            (*g) = 4;
        }
    }

    fn setup() -> (KafkaZkWatcher, KafkaZkHandlers) {
        let handler = KafkaZkHandlers::init();
        let watcher = KafkaZkWatcher::init(handler.clone());
        (watcher, handler)
    }

    fn mock_event(
        event_type: WatchedEventType,
        keeper_state: KeeperState,
        path: Option<String>,
    ) -> WatchedEvent {
        WatchedEvent {
            event_type,
            keeper_state,
            path,
        }
    }

    #[test]
    fn node_create() {
        let (watcher, handler) = setup();
        let ts = TestStruct::init();
        let ts_handler = TestStructChangeHandler::init(&ts);
        handler.register_znode_change_handler(Arc::new(ts_handler));

        let event = mock_event(
            WatchedEventType::NodeCreated,
            KeeperState::SyncConnected,
            Some(TEST_PATH.to_string()),
        );
        watcher.handle(event);

        let g = ts.data.lock().unwrap();
        assert_eq!(*g, 1);
    }

    #[test]
    fn node_delete() {
        let (watcher, handler) = setup();
        let ts = TestStruct::init();
        let ts_handler = TestStructChangeHandler::init(&ts);
        handler.register_znode_change_handler(Arc::new(ts_handler));

        let event = mock_event(
            WatchedEventType::NodeDeleted,
            KeeperState::SyncConnected,
            Some(TEST_PATH.to_string()),
        );
        watcher.handle(event);

        let g = ts.data.lock().unwrap();
        assert_eq!(*g, 2);
    }

    #[test]
    fn node_data_change() {
        let (watcher, handler) = setup();
        let ts = TestStruct::init();
        let ts_handler = TestStructChangeHandler::init(&ts);
        handler.register_znode_change_handler(Arc::new(ts_handler));

        let event = mock_event(
            WatchedEventType::NodeDataChanged,
            KeeperState::SyncConnected,
            Some(TEST_PATH.to_string()),
        );
        watcher.handle(event);

        let g = ts.data.lock().unwrap();
        assert_eq!(*g, 3);
    }

    #[test]
    fn node_child_change() {
        let (watcher, handler) = setup();
        let ts = TestStruct::init();
        let ts_handler = TestStructChildChangeHandler::init(&ts);
        handler.register_znode_child_change_handler(Arc::new(ts_handler));

        let event = mock_event(
            WatchedEventType::NodeChildrenChanged,
            KeeperState::SyncConnected,
            Some(TEST_PATH.to_string()),
        );
        watcher.handle(event);

        let g = ts.data.lock().unwrap();
        assert_eq!(*g, 4);
    }

    #[test]
    fn no_handlers() {
        let (watcher, _handler) = setup();
        let ts = TestStruct::init();

        let event = mock_event(
            WatchedEventType::NodeCreated,
            KeeperState::SyncConnected,
            Some(TEST_PATH.to_string()),
        );
        watcher.handle(event);

        let g = ts.data.lock().unwrap();
        assert_eq!(*g, 0);
    }

    #[test]
    fn contains_change() {
        let (_watcher, handler) = setup();
        let ts = TestStruct::init();

        assert!(!handler.contains_znode_change_handler(TEST_PATH));

        let ts_handler = TestStructChangeHandler::init(&ts);
        handler.register_znode_change_handler(Arc::new(ts_handler));

        assert!(handler.contains_znode_change_handler(TEST_PATH));
    }

    #[test]
    fn unregister_change() {
        let (watcher, handler) = setup();
        let ts = TestStruct::init();

        let ts_handler = TestStructChangeHandler::init(&ts);
        handler.register_znode_change_handler(Arc::new(ts_handler));
        handler.unregister_znode_change_handler(TEST_PATH);

        let event = mock_event(
            WatchedEventType::NodeCreated,
            KeeperState::SyncConnected,
            Some(TEST_PATH.to_string()),
        );
        watcher.handle(event);

        let g = ts.data.lock().unwrap();
        assert_eq!(*g, 0);
    }

    #[test]
    fn contains_child_change() {
        let (_watcher, handler) = setup();
        let ts = TestStruct::init();

        assert!(!handler.contains_znode_child_change_handler(TEST_PATH));

        let ts_handler = TestStructChildChangeHandler::init(&ts);
        handler.register_znode_child_change_handler(Arc::new(ts_handler));

        assert!(handler.contains_znode_child_change_handler(TEST_PATH));
    }

    #[test]
    fn unregister_child_change() {
        let (watcher, handler) = setup();
        let ts = TestStruct::init();

        let ts_handler = TestStructChildChangeHandler::init(&ts);
        handler.register_znode_child_change_handler(Arc::new(ts_handler));
        handler.unregister_znode_child_change_handler(TEST_PATH);

        let event = mock_event(
            WatchedEventType::NodeCreated,
            KeeperState::SyncConnected,
            Some(TEST_PATH.to_string()),
        );
        watcher.handle(event);

        let g = ts.data.lock().unwrap();
        assert_eq!(*g, 0);
    }
}
