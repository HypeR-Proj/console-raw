// SPDX-FileCopyrightText: 2026 roolrz
// SPDX-License-Identifier: Apache-2.0

//! Per-source text log rendering. This is separate from binary raw streams.
use std::collections::VecDeque;

pub struct LogBuffer {
    bytes: VecDeque<u8>,
    limit: usize,
    previous_cr: bool,
    dropped_bytes: u64,
}
impl LogBuffer {
    pub fn new(limit: usize) -> Self {
        Self {
            bytes: VecDeque::new(),
            limit,
            previous_cr: false,
            dropped_bytes: 0,
        }
    }

    /// Accepts a complete log record only when its rendered bytes fit. Preserve
    /// CR/LF pairs split across records; dropped records do not change rendering
    /// state because those bytes never reached the display stream.
    pub fn append(&mut self, record: &[u8]) {
        let mut previous_cr = self.previous_cr;
        let mut required = record.len();
        for &byte in record {
            if byte == b'\n' && !previous_cr {
                required = required.saturating_add(1);
            }
            previous_cr = byte == b'\r';
        }
        if required > self.limit - self.bytes.len() {
            self.dropped_bytes = self.dropped_bytes.saturating_add(record.len() as u64);
            return;
        }
        for &byte in record {
            if byte == b'\n' && !self.previous_cr {
                self.bytes.push_back(b'\r');
            }
            self.bytes.push_back(byte);
            self.previous_cr = byte == b'\r';
        }
    }
    pub fn front(&self) -> &[u8] {
        self.bytes.as_slices().0
    }
    pub fn is_empty(&self) -> bool {
        self.bytes.is_empty()
    }
    pub fn consume(&mut self, count: usize) {
        self.bytes.drain(..count.min(self.bytes.len()));
    }
    pub fn dropped_bytes(&self) -> u64 {
        self.dropped_bytes
    }
}

#[cfg(test)]
mod tests {
    use super::LogBuffer;

    fn take(buffer: &mut LogBuffer) -> Vec<u8> {
        let mut result = Vec::new();
        while !buffer.is_empty() {
            result.extend_from_slice(buffer.front());
            buffer.consume(buffer.front().len());
        }
        result
    }
    #[test]
    fn newlines_preserve_crlf_across_records_and_partial_drains() {
        let mut buffer = LogBuffer::new(64);
        buffer.append(b"one\ntwo\r");
        let mut result = take(&mut buffer);
        buffer.append(b"\nthree\n");
        result.extend(take(&mut buffer));
        assert_eq!(result, b"one\r\ntwo\r\nthree\r\n");
    }
    #[test]
    fn quota_counts_rendered_bytes_and_drop_keeps_display_state() {
        let mut buffer = LogBuffer::new(3);
        buffer.append(b"a\r");
        buffer.append(b"xx"); // No room: preserve the accepted trailing CR.
        buffer.append(b"\n");
        assert_eq!(buffer.dropped_bytes(), 2);
        assert_eq!(take(&mut buffer), b"a\r\n");
        buffer.append(b"\n\n"); // Four output bytes exceed the three-byte limit.
        assert!(buffer.is_empty());
        assert_eq!(buffer.dropped_bytes(), 4);
    }
}
