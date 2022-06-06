use std::{
    collections::{HashMap, VecDeque},
    sync::{Arc, RwLock},
    time::{SystemTime, UNIX_EPOCH},
};

use crate::common::topic_partition::TopicPartition;

use super::config::DEFAULT_SEGMENT_SIZE;

/// Simple Log which can be visualized as a linkedlist of
/// segments, each containing 1 or more messages. The
/// high_watermark can be seen as a checkpoint. Also, note that
/// the log manager isn't responsible for updating the high_watermark,
/// just maintaining it.
///
/// lib - If we include GC of logs, then fetch messages should also
/// return the first offset. This is necessary for backfill events of
/// newly added replicas.
pub struct Log {
    segments: RwLock<VecDeque<LogSegment>>,
    high_watermark: RwLock<u64>,
    segment_size: usize,
}

impl Log {
    pub fn init() -> Log {
        Log {
            segments: RwLock::new(VecDeque::new()),
            high_watermark: RwLock::new(0),
            segment_size: DEFAULT_SEGMENT_SIZE,
        }
    }

    pub fn init_test(segment_size: usize) -> Log {
        Log {
            segments: RwLock::new(VecDeque::new()),
            high_watermark: RwLock::new(0),
            segment_size,
        }
    }

    /// Fetch all messages >= start.
    /// Also, if the request was issued by a client, then also restrict the
    /// messages to the set of committed messages
    pub fn fetch_messages(
        &self,
        start: u64,
        fetch_max_bytes: u64,
        issued_by_client: bool,
    ) -> Vec<u8> {
        // Check if an upper bound is needed
        let upper_bound;
        if issued_by_client {
            let r = self.high_watermark.read().unwrap();
            upper_bound = *r;
        } else {
            upper_bound = u64::max_value();
        }

        let mut i = 0;

        let r = self.segments.read().unwrap();

        // Find the starting segment
        while (i + 1) < (*r).len() && (*r)[i + 1].get_byte_offset() <= start {
            i += 1;
        }

        // Fetch the actual messages
        let mut messages = Vec::new();
        let mut remaining_space = fetch_max_bytes;

        while i < (*r).len() {
            let (mut msgs, reached_bound) =
                (*r)[i].fetch_messages(start, remaining_space, upper_bound);

            messages.append(&mut msgs);

            remaining_space = fetch_max_bytes - messages.len() as u64;
            i += 1;

            if reached_bound {
                break;
            }
        }

        messages
    }

    /// Appends the sequence of messages into the log and returns the
    /// log end offset
    pub fn append_messages(&self, messages: Vec<u8>) -> u64 {
        let curr_time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis();

        self._append_messages(messages, curr_time)
    }

    pub fn _append_messages(&self, mut messages: Vec<u8>, curr_time: u128) -> u64 {
        let mut w = self.segments.write().unwrap();
        let mut prev_end: u64 = 0;

        while messages.len() > 0 {
            if let Some(segment) = (*w).back() {
                messages = segment.append_messages(messages, curr_time);
                prev_end = segment.byte_offset + segment.get_len() as u64;
            }

            if messages.len() > 0 {
                (*w).push_back(LogSegment::init(prev_end, self.segment_size));
            }
        }

        prev_end
    }

    pub fn checkpoint_high_watermark(&self, high_watermark: u64) {
        let watermark = u64::min(high_watermark, self.get_log_end());

        let mut w = self.high_watermark.write().unwrap();
        *w = watermark;
    }

    pub fn get_high_watermark(&self) -> u64 {
        let r = self.high_watermark.read().unwrap();
        *r
    }

    pub fn get_log_end(&self) -> u64 {
        let r = self.segments.read().unwrap();
        if let Some(segment) = (*r).back() {
            segment.get_byte_offset() + segment.get_len() as u64
        } else {
            0
        }
    }

    /// Reset the log end offset to the last high watermark
    pub fn truncate(&self) {
        let mut i = 0;

        // Stop any further changes to the segment and watermark
        let mut ws = self.segments.write().unwrap();
        let rw = self.high_watermark.read().unwrap();

        // Find the starting segment
        while (i + 1) < (*ws).len() && (*ws)[i + 1].get_byte_offset() <= *rw {
            i += 1;
        }

        if i < (*ws).len() {
            (*ws)[i].truncate(*rw);
            while (*ws).len() > i + 1 {
                (*ws).pop_back();
            }
        }
    }

    pub fn is_empty(&self) -> bool {
        let r = self.segments.read().unwrap();
        (*r).len() == 0
    }

    pub fn get_create_time(&self, message_offset: u64) -> u128 {
        let r = self.segments.read().unwrap();

        if (*r).len() == 0 {
            return u128::max_value();
        }

        let mut i = 0;
        // Find the starting segment
        while (i + 1) < (*r).len() && (*r)[i + 1].get_byte_offset() <= message_offset {
            i += 1;
        }

        (*r)[i].get_create_time(message_offset)
    }
}

pub struct LogSegment {
    data: RwLock<Vec<u8>>,
    byte_offset: u64,
    segment_size: usize,
    create_time: RwLock<HashMap<u64, u128>>,
}

impl LogSegment {
    pub fn init(byte_offset: u64, segment_size: usize) -> LogSegment {
        LogSegment {
            data: RwLock::new(Vec::new()),
            byte_offset,
            segment_size,
            create_time: RwLock::new(HashMap::new()),
        }
    }

    pub fn get_byte_offset(&self) -> u64 {
        self.byte_offset
    }

    pub fn get_len(&self) -> usize {
        let r = self.data.read().unwrap();
        (*r).len()
    }

    pub fn fetch_messages(
        &self,
        start: u64,
        fetch_max_bytes: u64,
        upper_limit: u64,
    ) -> (Vec<u8>, bool) {
        let offset = self.byte_offset;
        let mut i: usize = 0;
        let r = self.data.read().unwrap();

        // Find first message
        while i < (*r).len() && (offset + i as u64) < start {
            let len = u32::from_be_bytes((*r)[i..(i + 4)].try_into().unwrap());
            i += 4 + len as usize;
        }

        let mut messages = Vec::new();
        let mut reached_bound = false;

        // Add messages that are less than the upper_limit
        // Also, ensure that we don't fetch more than we can
        while i < (*r).len() {
            if (offset + i as u64) >= upper_limit {
                reached_bound = true;
                break;
            }
            let len = u32::from_be_bytes((*r)[i..(i + 4)].try_into().unwrap());
            if (4 + len as usize + messages.len()) as u64 > fetch_max_bytes {
                reached_bound = true;
                break;
            }
            messages.extend_from_slice(&(*r)[i..(i + 4 + len as usize)]);
            i += 4 + len as usize;
        }

        (messages, reached_bound)
    }

    /// Try to append as many messages as possible into the segment w/o
    /// overflowing.
    ///
    /// Returns the remaining messages that weren't able to fit.
    pub fn append_messages(&self, messages: Vec<u8>, time: u128) -> Vec<u8> {
        let mut i: usize = 0;
        let mut wd = self.data.write().unwrap();
        let mut wt = self.create_time.write().unwrap();

        // Append messages into the log segment, stopping when the segment can't
        // hold the next message
        while i < messages.len() {
            let len = u32::from_be_bytes(messages[i..(i + 4)].try_into().unwrap());
            if (4 + len as usize + (*wd).len()) > self.segment_size {
                break;
            }
            (*wt).insert(self.byte_offset + (*wd).len() as u64, time);
            (*wd).extend_from_slice(&messages[i..(i + 4 + len as usize)]);
            i += 4 + len as usize;
        }

        // Return remaining messages if necessary
        if i < messages.len() {
            messages[i..].to_vec()
        } else {
            Vec::new()
        }
    }

    pub fn truncate(&self, offset: u64) {
        let mut w = self.data.write().unwrap();

        let base_offset = self.byte_offset;
        let max_offset = base_offset + (*w).len() as u64;

        if offset < max_offset {
            (*w).truncate((offset - base_offset).try_into().unwrap());
        }
    }

    /// -inf: record doesn't exist
    /// inf: record matches largest in segment
    /// otherwise: found match
    pub fn get_create_time(&self, message_offset: u64) -> u128 {
        let rd = self.data.read().unwrap();
        let rt = self.create_time.read().unwrap();
        if let Some(t) = (*rt).get(&message_offset) {
            *t
        } else {
            if message_offset == self.byte_offset + (*rd).len() as u64 {
                u128::max_value()
            } else {
                u128::min_value()
            }
        }
    }
}

/// LogManager of in-memory, segmented logs.
pub struct LogManager {
    data: RwLock<HashMap<TopicPartition, Arc<Log>>>,
}

impl LogManager {
    pub fn init() -> LogManager {
        LogManager {
            data: RwLock::new(HashMap::new()),
        }
    }

    pub fn has_log(&self, topic_partition: &TopicPartition) -> bool {
        let r = self.data.read().unwrap();
        (*r).contains_key(topic_partition)
    }

    pub fn get_log(&self, topic_partition: &TopicPartition) -> Option<Arc<Log>> {
        let r = self.data.read().unwrap();
        match (*r).get(topic_partition) {
            Some(log) => Some(log.clone()),
            None => None,
        }
    }

    pub fn get_or_create_log(&self, topic_partition: &TopicPartition) -> Arc<Log> {
        let mut w = self.data.write().unwrap();

        if let Some(log) = (*w).get(topic_partition) {
            log.clone()
        } else {
            let log = Arc::new(Log::init());
            (*w).insert(topic_partition.clone(), log.clone());
            log
        }
    }
}

#[cfg(test)]
mod log_segment_tests {
    use super::{LogSegment, DEFAULT_SEGMENT_SIZE};

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

    fn deserialize(messages: Vec<u8>) -> Vec<String> {
        let mut deserialized = Vec::new();
        let mut i = 0;

        while i < messages.len() {
            let len = u32::from_be_bytes(messages[i..(i + 4)].try_into().unwrap());
            let str =
                String::from_utf8(messages[(i + 4)..(i + 4 + len as usize)].to_vec()).unwrap();
            deserialized.push(str);
            i += 4 + len as usize;
        }

        deserialized
    }

    fn assert_valid_append(
        remaining: &Vec<u8>,
        segment: &LogSegment,
        expected_remaining_len: usize,
        expected_data_len: usize,
    ) {
        assert_eq!(remaining.len(), expected_remaining_len);

        let r = segment.data.read().unwrap();
        assert_eq!((*r).len(), expected_data_len);
    }

    #[test]
    fn append() {
        let segment = LogSegment::init(0, DEFAULT_SEGMENT_SIZE);
        let message_set = construct_message_set(vec!["msg1", "msg2"]);
        let remaining = segment.append_messages(message_set, 0);
        assert_valid_append(&remaining, &segment, 0, 16);

        let message_set_1 = construct_message_set(vec!["msg3", "msg4"]);
        let remaining = segment.append_messages(message_set_1, 0);
        assert_valid_append(&remaining, &segment, 0, 32);
    }

    #[test]
    fn append_full() {
        let segment = LogSegment::init(0, 0);
        let message_set = construct_message_set(vec!["msg1", "msg2"]);
        let remaining = segment.append_messages(message_set, 0);
        assert_valid_append(&remaining, &segment, 16, 0);
    }

    #[test]
    fn append_partial() {
        let segment = LogSegment::init(0, 10);
        let message_set = construct_message_set(vec!["msg1", "msg2"]);
        let remaining = segment.append_messages(message_set, 0);
        assert_valid_append(&remaining, &segment, 8, 8);
    }

    #[test]
    fn fetch() {
        let segment = LogSegment::init(0, DEFAULT_SEGMENT_SIZE);
        let message_set = construct_message_set(vec!["msg1", "msg2"]);
        let _ = segment.append_messages(message_set, 0);

        let (bytes, reached_max) = segment.fetch_messages(0, u64::max_value(), u64::max_value());

        let expected: Vec<String> = ["msg1", "msg2"].iter().map(|&s| s.into()).collect();
        assert!(!reached_max);
        assert_eq!(deserialize(bytes), expected);

        // Make sure that fetching multiple times has no side-effects
        let (bytes, reached_max) = segment.fetch_messages(0, u64::max_value(), u64::max_value());

        let expected: Vec<String> = ["msg1", "msg2"].iter().map(|&s| s.into()).collect();
        assert!(!reached_max);
        assert_eq!(deserialize(bytes), expected);
    }

    #[test]
    fn fetch_start() {
        let segment = LogSegment::init(0, DEFAULT_SEGMENT_SIZE);
        let message_set = construct_message_set(vec!["msg1", "msg2"]);
        let _ = segment.append_messages(message_set, 0);

        let (bytes, reached_max) = segment.fetch_messages(8, u64::max_value(), u64::max_value());

        let expected: Vec<String> = ["msg2"].iter().map(|&s| s.into()).collect();
        assert!(!reached_max);
        assert_eq!(deserialize(bytes), expected);
    }

    #[test]
    fn fetch_max() {
        let segment = LogSegment::init(0, DEFAULT_SEGMENT_SIZE);
        let message_set = construct_message_set(vec!["msg1", "msg2"]);
        let _ = segment.append_messages(message_set, 0);

        let (bytes, reached_max) = segment.fetch_messages(0, 8, u64::max_value());
        let expected: Vec<String> = ["msg1"].iter().map(|&s| s.into()).collect();
        assert!(reached_max);
        assert_eq!(deserialize(bytes), expected);
    }

    #[test]
    fn fetch_upper() {
        let segment = LogSegment::init(0, DEFAULT_SEGMENT_SIZE);
        let message_set = construct_message_set(vec!["msg1", "msg2"]);
        let _ = segment.append_messages(message_set, 0);

        let (bytes, reached_max) = segment.fetch_messages(0, u64::max_value(), 8);

        let expected: Vec<String> = ["msg1"].iter().map(|&s| s.into()).collect();
        assert!(reached_max);
        assert_eq!(deserialize(bytes), expected);
    }

    #[test]
    fn truncate() {
        let segment = LogSegment::init(0, DEFAULT_SEGMENT_SIZE);
        let message_set = construct_message_set(vec!["msg1", "msg2"]);
        let _ = segment.append_messages(message_set, 0);

        {
            let r = segment.data.read().unwrap();
            assert_eq!(r.len(), 16);
        }

        segment.truncate(8);
        {
            let r = segment.data.read().unwrap();
            assert_eq!(r.len(), 8);
        }
        let (bytes, reached_max) = segment.fetch_messages(0, u64::max_value(), u64::max_value());
        let expected: Vec<String> = ["msg1"].iter().map(|&s| s.into()).collect();
        assert!(!reached_max);
        assert_eq!(deserialize(bytes), expected);
    }

    #[test]
    fn create_time() {
        let segment = LogSegment::init(0, DEFAULT_SEGMENT_SIZE);
        let message_set = construct_message_set(vec!["msg1", "msg2"]);
        let _ = segment.append_messages(message_set, 10);
    }

    #[test]
    fn create_time_end() {
        let segment = LogSegment::init(0, DEFAULT_SEGMENT_SIZE);
        let message_set = construct_message_set(vec!["msg1", "msg2"]);
        let _ = segment.append_messages(message_set, 10);

        assert_eq!(segment.get_create_time(0), 10);
        assert_eq!(segment.get_create_time(8), 10);

        // Doesn't exist
        assert_eq!(segment.get_create_time(1), u128::min_value());

        // Doesn't exist but last
        assert_eq!(segment.get_create_time(16), u128::max_value());
    }
}

#[cfg(test)]
mod log_tests {
    use super::Log;

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

    fn deserialize(messages: Vec<u8>) -> Vec<String> {
        let mut deserialized = Vec::new();
        let mut i = 0;

        while i < messages.len() {
            let len = u32::from_be_bytes(messages[i..(i + 4)].try_into().unwrap());
            let str =
                String::from_utf8(messages[(i + 4)..(i + 4 + len as usize)].to_vec()).unwrap();
            deserialized.push(str);
            i += 4 + len as usize;
        }

        deserialized
    }

    fn assert_valid_append(log: &Log, expected_segments: usize, expected_dist: Vec<Vec<String>>) {
        let r = log.segments.read().unwrap();
        assert_eq!((*r).len(), expected_segments);
        for i in 0..expected_segments {
            let rs = (*r)[i].data.read().unwrap();
            let messages = deserialize((*rs).clone());
            assert_eq!(messages, expected_dist[i]);
        }
    }

    #[test]
    fn append_single() {
        let log = Log::init();
        let message_set = construct_message_set(vec!["msg1", "msg2"]);
        log.append_messages(message_set);

        let expected: Vec<Vec<String>> = vec![["msg1", "msg2"].iter().map(|&s| s.into()).collect()];
        assert_valid_append(&log, 1, expected);
    }

    #[test]
    fn append_multiple() {
        let log = Log::init_test(8);
        let message_set = construct_message_set(vec!["msg1", "msg2"]);
        log.append_messages(message_set);

        let expected: Vec<Vec<String>> = vec![
            ["msg1"].iter().map(|&s| s.into()).collect(),
            ["msg2"].iter().map(|&s| s.into()).collect(),
        ];
        assert_valid_append(&log, 2, expected);
    }

    #[test]
    fn fetch_single() {
        let log = Log::init();
        let message_set = construct_message_set(vec!["msg1", "msg2", "msg3", "msg4"]);
        log.append_messages(message_set);

        let bytes = log.fetch_messages(0, u64::max_value(), false);

        let expected: Vec<String> = ["msg1", "msg2", "msg3", "msg4"]
            .iter()
            .map(|&s| s.into())
            .collect();
        assert_eq!(deserialize(bytes), expected);
    }

    #[test]
    fn fetch_multiple() {
        let log = Log::init_test(8);
        let message_set = construct_message_set(vec!["msg1", "msg2", "msg3", "msg4"]);
        log.append_messages(message_set);

        let bytes = log.fetch_messages(0, u64::max_value(), false);
        let expected: Vec<String> = ["msg1", "msg2", "msg3", "msg4"]
            .iter()
            .map(|&s| s.into())
            .collect();
        assert_eq!(deserialize(bytes), expected);
    }

    #[test]
    fn fetch_start() {
        let log = Log::init_test(8);
        let message_set = construct_message_set(vec!["msg1", "msg2", "msg3", "msg4"]);
        log.append_messages(message_set);

        let bytes = log.fetch_messages(16, u64::max_value(), false);

        let expected: Vec<String> = ["msg3", "msg4"].iter().map(|&s| s.into()).collect();
        assert_eq!(deserialize(bytes), expected);
    }

    #[test]
    fn fetch_max() {
        let log = Log::init_test(8);
        let message_set = construct_message_set(vec!["msg1", "msg2", "msg3", "msg4"]);
        log.append_messages(message_set);

        let bytes = log.fetch_messages(16, 8, false);

        let expected: Vec<String> = ["msg3"].iter().map(|&s| s.into()).collect();
        assert_eq!(deserialize(bytes), expected);
    }

    #[test]
    fn fetch_client_watermark() {
        let log = Log::init_test(8);
        let message_set = construct_message_set(vec!["msg1", "msg2", "msg3", "msg4"]);
        log.append_messages(message_set);

        let bytes = log.fetch_messages(0, u64::max_value(), true);

        let expected: Vec<String> = Vec::new();
        assert_eq!(deserialize(bytes), expected);

        log.checkpoint_high_watermark(24);
        let bytes = log.fetch_messages(0, u64::max_value(), true);

        let expected: Vec<String> = ["msg1", "msg2", "msg3"].iter().map(|&s| s.into()).collect();
        assert_eq!(deserialize(bytes), expected);
    }

    #[test]
    fn truncate() {
        let log = Log::init_test(16);
        let message_set = construct_message_set(vec![
            "msg1", "msg2", "msg3", "msg4", "msg5", "msg6", "msg7", "msg8",
        ]);
        log.append_messages(message_set);

        log.checkpoint_high_watermark(24);
        log.truncate();
        let bytes = log.fetch_messages(0, u64::max_value(), false);

        let expected: Vec<String> = ["msg1", "msg2", "msg3"].iter().map(|&s| s.into()).collect();
        assert_eq!(deserialize(bytes), expected);
    }

    #[test]
    fn truncate_empty() {
        let log = Log::init_test(16);
        log.truncate();

        let bytes = log.fetch_messages(0, u64::max_value(), false);
        assert_eq!(bytes.len(), 0);
    }

    #[test]
    fn get_create_time() {
        let log = Log::init_test(16);
        let msg_set1 = construct_message_set(vec!["msg1"]);
        let msg_set2 = construct_message_set(vec!["msg2", "msg3"]);
        let msg_set3 = construct_message_set(vec!["msg4"]);

        log._append_messages(msg_set1, 0);
        log._append_messages(msg_set2, 1);
        log._append_messages(msg_set3, 2);

        assert_eq!(log.get_create_time(0), 0);
        assert_eq!(log.get_create_time(8), 1);
        assert_eq!(log.get_create_time(16), 1);
        assert_eq!(log.get_create_time(24), 2);
        assert_eq!(log.get_create_time(32), u128::max_value());
    }

    #[test]
    fn get_create_time_empty() {
        let log = Log::init_test(16);

        assert_eq!(log.get_create_time(0), u128::max_value());
    }
}
