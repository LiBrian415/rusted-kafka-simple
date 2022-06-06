use std::{
    collections::{HashMap, VecDeque},
    sync::{Arc, Mutex, RwLock},
};

use tokio::sync::Notify;

use crate::common::topic_partition::TopicPartition;

use super::{
    err::{ReplicaError, ReplicaResult},
    log_manager::{Log, LogManager},
};

pub struct AckManager {
    log_manager: Arc<LogManager>,
    handlers: RwLock<HashMap<TopicPartition, Arc<AckHandler>>>,
}

impl AckManager {
    pub fn init(log_manager: Arc<LogManager>) -> AckManager {
        AckManager {
            handlers: RwLock::new(HashMap::new()),
            log_manager,
        }
    }

    pub fn get_or_create_handler(
        &self,
        topic_partition: &TopicPartition,
    ) -> ReplicaResult<Arc<AckHandler>> {
        let exists;
        {
            let r = self.handlers.read().unwrap();
            exists = (*r).contains_key(&topic_partition);
        }

        if !exists {
            let mut w = self.handlers.write().unwrap();
            if let Some(log) = self.log_manager.get_log(topic_partition) {
                if !(*w).contains_key(&topic_partition) {
                    (*w).insert(
                        topic_partition.clone(),
                        Arc::new(AckHandler::init(log.clone())),
                    );
                }
            } else {
                return Err(Box::new(ReplicaError::MissingLog(
                    topic_partition.topic.clone(),
                    topic_partition.partition,
                )));
            }
        }

        let r = self.handlers.read().unwrap();
        Ok((*r).get(&topic_partition).unwrap().clone())
    }

    pub fn get_handler(&self, topic_partition: &TopicPartition) -> Option<Arc<AckHandler>> {
        let r = self.handlers.read().unwrap();
        match (*r).get(topic_partition) {
            Some(ack_handler) => Some(ack_handler.clone()),
            None => None,
        }
    }
}

pub struct AckHandler {
    log: Arc<Log>,
    all: Mutex<VecDeque<(Arc<Notify>, u64)>>,
    one: Mutex<VecDeque<(Arc<Notify>, u64)>>,
}

impl AckHandler {
    pub fn init(log: Arc<Log>) -> AckHandler {
        AckHandler {
            log,
            all: Mutex::new(VecDeque::new()),
            one: Mutex::new(VecDeque::new()),
        }
    }

    /// Required_acks shouldn't be 0
    pub fn append_messages(&self, messages: Vec<u8>, required_acks: i8) -> Arc<Notify> {
        let notify = Arc::new(Notify::new());
        {
            if required_acks == 1 {
                let mut g = self.one.lock().unwrap();
                let end_offset = self.log.append_messages(messages);

                (*g).push_back((notify.clone(), end_offset));
            } else {
                // ack_type == -1
                let mut g = self.all.lock().unwrap();
                let end_offset = self.log.append_messages(messages);

                (*g).push_back((notify.clone(), end_offset));
            }
        }
        notify
    }

    /// Should be called whenever a new replica fetches more entries or whenever the
    /// isr changes as those are the only two events that can change the values passed in
    pub fn notify(&self, min_isr_ack: u64, max_isr_ack: u64) {
        // Notify single
        {
            let mut g = self.one.lock().unwrap();
            while !(*g).is_empty() {
                if let Some((notify, offset)) = (*g).front() {
                    if *offset <= max_isr_ack {
                        notify.notify_one();
                        (*g).pop_front();
                    } else {
                        break;
                    }
                }
            }
        }
        // Notify all
        {
            let mut g = self.all.lock().unwrap();
            while !(*g).is_empty() {
                if let Some((notify, offset)) = (*g).front() {
                    if *offset <= min_isr_ack {
                        notify.notify_one();
                        (*g).pop_front();
                    } else {
                        break;
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod ack_handler {
    use std::sync::Arc;

    use crate::core::{config::DEFAULT_SEGMENT_SIZE, log_manager::Log};

    use super::AckHandler;

    fn serialize(message: &str) -> Vec<u8> {
        let mut serialized = Vec::new();
        serialized.append(&mut u32::to_be_bytes(message.len() as u32).to_vec());
        serialized.append(&mut String::into_bytes(message.to_string()));
        serialized
    }

    fn construct_message_set(messages: Vec<&str>) -> Vec<u8> {
        let mut message_set = Vec::new();
        for message in messages {
            message_set.append(&mut serialize(message));
        }
        message_set
    }

    #[test]
    fn one_single() {
        let log = Arc::new(Log::init_test(DEFAULT_SEGMENT_SIZE));
        let handler = AckHandler::init(log);

        let msg_set = construct_message_set(vec!["msg1"]);
        let _notify = handler.append_messages(msg_set, 1);

        {
            let r = handler.one.lock().unwrap();
            assert_eq!(r.len(), 1);
        }

        handler.notify(0, 0);
        {
            let r = handler.one.lock().unwrap();
            assert_eq!(r.len(), 1);
        }

        handler.notify(0, 8);
        {
            let r = handler.one.lock().unwrap();
            assert_eq!(r.len(), 0);
        }
    }

    #[test]
    fn one_multiple() {
        let log = Arc::new(Log::init_test(DEFAULT_SEGMENT_SIZE));
        let handler = AckHandler::init(log);

        let msg_set = construct_message_set(vec!["msg1"]);
        let _notify = handler.append_messages(msg_set, 1);
        let msg_set = construct_message_set(vec!["msg2"]);
        let _notify = handler.append_messages(msg_set, 1);

        {
            let r = handler.one.lock().unwrap();
            assert_eq!(r.len(), 2);
        }

        handler.notify(0, 16);
        {
            let r = handler.one.lock().unwrap();
            assert_eq!(r.len(), 0);
        }
    }

    #[test]
    fn all_single() {
        let log = Arc::new(Log::init_test(DEFAULT_SEGMENT_SIZE));
        let handler = AckHandler::init(log);

        let msg_set = construct_message_set(vec!["msg1"]);
        let _notify = handler.append_messages(msg_set, -1);

        {
            let r = handler.all.lock().unwrap();
            assert_eq!(r.len(), 1);
        }

        handler.notify(0, 0);
        {
            let r = handler.all.lock().unwrap();
            assert_eq!(r.len(), 1);
        }

        handler.notify(0, 16);
        {
            let r = handler.all.lock().unwrap();
            assert_eq!(r.len(), 1);
        }

        handler.notify(16, 16);
        {
            let r = handler.all.lock().unwrap();
            assert_eq!(r.len(), 0);
        }
    }

    #[test]
    fn all_multiple() {
        let log = Arc::new(Log::init_test(DEFAULT_SEGMENT_SIZE));
        let handler = AckHandler::init(log);

        let msg_set = construct_message_set(vec!["msg1"]);
        let _notify = handler.append_messages(msg_set, -1);
        let msg_set = construct_message_set(vec!["msg2"]);
        let _notify = handler.append_messages(msg_set, -1);

        {
            let r = handler.all.lock().unwrap();
            assert_eq!(r.len(), 2);
        }

        handler.notify(16, 16);
        {
            let r = handler.all.lock().unwrap();
            assert_eq!(r.len(), 0);
        }
    }
}
