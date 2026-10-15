// SPDX-FileCopyrightText: 2026 roolrz
// SPDX-License-Identifier: Apache-2.0

//! Independent bounded service logs. Saturation drops records rather than
//! making critical services depend on an interactive terminal or UART progress.

use super::Channel;
use hyper_console_raw::log_buffer::LogBuffer;
use hyper_os::{Error, Status};

pub struct Log {
    pub channel: Channel,
    // Registered by the supervisor, never inferred from a log payload. Retained
    // with the per-source loss counter for future diagnostic queries.
    _source_name: String,
    pub dead: bool,
    pub peer_closed: bool,
    pending: LogBuffer,
}
impl Log {
    pub fn new(source_name: String, channel: Channel) -> Self {
        Self {
            channel,
            _source_name: source_name,
            pending: LogBuffer::new(super::LOG_MESSAGE_LIMIT),
            dead: false,
            peer_closed: false,
        }
    }
    pub fn has_output(&self) -> bool {
        !self.pending.is_empty()
    }

    pub fn progress(&mut self, console: &hyper_os::console::Console<'_>, scratch: &mut [u8]) {
        // Read independent of UART progress. Each source has its own quota.
        if !self.peer_closed {
            match self.channel.as_byte_channel().try_receive(scratch) {
                Ok(count) => self.pending.append(&scratch[..count]),
                Err(Error::Status(Status::WOULD_BLOCK)) => {}
                Err(Error::Status(Status::PEER_CLOSED)) => self.peer_closed = true,
                Err(_) => self.dead = true,
            }
        }
        if !self.pending.is_empty() {
            match console.try_write(self.pending.front()) {
                Ok(count) => self.pending.consume(count),
                Err(Error::Status(Status::WOULD_BLOCK)) => {}
                Err(_) => self.dead = true,
            }
        }
        if self.peer_closed && self.pending.is_empty() {
            self.dead = true;
        }
    }
}
