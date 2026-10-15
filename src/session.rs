// SPDX-FileCopyrightText: 2026 roolrz
// SPDX-License-Identifier: Apache-2.0

//! One private request stream. Input commits only when its reply is enqueued.
use super::Channel;
use hyper_os::stream::{self, Header};
use hyper_os::{Error, Status};
use std::collections::VecDeque;

pub struct Session {
    pub channel: Channel,
    pub(super) access: u64,
    pub read: Option<Header>,
    pub read_blocked: bool,
    last_request: u64,
    pub reply: Option<Vec<u8>>,
    pub output: Vec<u8>,
    offset: usize,
    write_id: u64,
    pub dead: bool,
}
impl Session {
    pub fn new(channel: Channel, access: u64) -> Self {
        Self {
            channel,
            access,
            read: None,
            read_blocked: false,
            last_request: 0,
            reply: Some(Header::response(stream::INFO, 0, 0, 0, 0).encode().to_vec()),
            output: Vec::new(),
            offset: 0,
            write_id: 0,
            dead: false,
        }
    }
    fn respond(&mut self, request: Header, flags: u16, value: u64, status: Status) {
        self.reply = Some(
            Header::response(request.operation, request.id, flags, value, status.as_raw())
                .encode()
                .to_vec(),
        );
    }
    pub fn receive(&mut self, console: &hyper_os::console::Console<'_>, input: &mut VecDeque<u8>) {
        if self.reply.is_some() || !self.output.is_empty() {
            return;
        }
        let mut frame = [0; stream::MAX_FRAME];
        let count = match self.channel.as_byte_channel().try_receive(&mut frame) {
            Ok(count) => count,
            Err(Error::Status(Status::WOULD_BLOCK)) => return,
            Err(_) => {
                self.dead = true;
                return;
            }
        };
        let request = match Header::decode(&frame[..count]) {
            Ok(h) => h,
            Err(_) => {
                self.dead = true;
                return;
            }
        };
        if request.flags != 0 || request.operation & stream::RESPONSE != 0 {
            self.dead = true;
            return;
        }
        if request.operation == stream::CANCEL {
            if request.id == 0 || request.value != request.id || request.extra != 0 {
                self.dead = true;
                return;
            }
        } else {
            if request.id == 0 || request.id <= self.last_request {
                self.dead = true;
                return;
            }
            self.last_request = request.id;
        }
        match request.operation {
            stream::READ
                if self.access == stream::ACCESS_READ
                    && self.read.is_none()
                    && request.value != 0
                    && request.value <= stream::MAX_DATA as u64
                    && count == stream::HEADER_BYTES =>
            {
                self.read = Some(request)
            }
            stream::CANCEL if count == stream::HEADER_BYTES => {
                if self
                    .read
                    .is_some_and(|read| read.id == request.id && request.value == request.id)
                    && let Some(read) = self.read.take()
                {
                    self.read_blocked = false;
                    self.respond(read, stream::CANCELLED, 0, Status::OK);
                }
            }
            stream::WRITE
                if self.access == stream::ACCESS_WRITE
                    && request.value != 0
                    && request.value <= stream::MAX_DATA as u64
                    && request.extra == 0
                    && count == stream::HEADER_BYTES + request.value as usize =>
            {
                self.output
                    .extend_from_slice(&frame[stream::HEADER_BYTES..count]);
                self.offset = 0;
                self.write_id = request.id;
            }
            stream::FENCE
                if self.access == stream::ACCESS_WRITE
                    && request.value == 0
                    && request.extra == 0
                    && count == stream::HEADER_BYTES =>
            {
                self.respond(request, 0, 0, Status::OK)
            }
            stream::INFO
                if request.value == 0 && request.extra == 0 && count == stream::HEADER_BYTES =>
            {
                self.respond(request, 0, 0, Status::OK)
            }
            stream::DISCARD_INPUT
                if self.access == stream::ACCESS_READ
                    && self.read.is_none()
                    && request.value == 0
                    && request.extra == 0
                    && count == stream::HEADER_BYTES =>
            {
                match console.discard_input() {
                    Ok(count) => {
                        let count = count + input.len();
                        input.clear();
                        self.respond(request, 0, count as u64, Status::OK);
                    }
                    Err(Error::Status(status)) => self.respond(request, 0, 0, status),
                    Err(_) => self.dead = true,
                }
            }
            _ => self.respond(request, 0, 0, Status::INVALID_ARGUMENT),
        }
    }
    pub fn progress(
        &mut self,
        console: &hyper_os::console::Console<'_>,
        input: &mut VecDeque<u8>,
        now: u64,
    ) {
        if let Some(reply) = &self.reply {
            match self.channel.as_byte_channel().try_send(reply) {
                Ok(()) => self.reply = None,
                Err(Error::Status(Status::WOULD_BLOCK)) => return,
                Err(_) => {
                    self.dead = true;
                    return;
                }
            }
        }
        if let Some(read) = self.read {
            let length = (read.value as usize).min(input.len());
            if length != 0 {
                let mut frame =
                    Header::response(stream::READ, read.id, stream::DATA, length as u64, 0)
                        .encode()
                        .to_vec();
                frame.extend(input.iter().take(length));
                match self.channel.as_byte_channel().try_send(&frame) {
                    Ok(()) => {
                        input.drain(..length);
                        self.read = None;
                        self.read_blocked = false;
                    }
                    Err(Error::Status(Status::WOULD_BLOCK)) => self.read_blocked = true,
                    Err(_) => self.dead = true,
                }
            } else if read.extra != hyper_os::DEADLINE_INFINITE && read.extra <= now {
                self.read = None;
                self.read_blocked = false;
                self.respond(read, stream::TIMED_OUT, 0, Status::OK);
            }
        }
        if !self.output.is_empty() {
            match console.try_write(&self.output[self.offset..]) {
                Ok(count) => {
                    self.offset += count;
                    if self.offset == self.output.len() {
                        let length = self.output.len();
                        self.output.clear();
                        self.offset = 0;
                        self.reply = Some(
                            Header::response(stream::WRITE, self.write_id, 0, length as u64, 0)
                                .encode()
                                .to_vec(),
                        );
                    }
                }
                Err(Error::Status(Status::WOULD_BLOCK)) => {}
                Err(_) => self.dead = true,
            }
        }
    }
}
