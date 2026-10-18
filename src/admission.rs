// SPDX-FileCopyrightText: 2026 roolrz
// SPDX-License-Identifier: Apache-2.0

//! Blocking capability rendezvous is confined to admission workers. The bounded
//! mailbox wakes the byte reactor; no worker owns physical input or output.

use core::mem::MaybeUninit;
use hyper_os::capability_channel::{CapabilityChannel, CapabilityReceiveSlot};
use hyper_os::handle::{ByteChannelObject, CapabilityChannelObject, OwnedHandle};
use hyper_os::stream;
use hyper_os::{Error, Result, Status};
use hyper_service::console as contract;
use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicU64, Ordering},
    mpsc::{Receiver, SyncSender, sync_channel},
};
use std::time::Duration;

type Channel = OwnedHandle<ByteChannelObject>;
pub enum Event {
    LogsStopped,
    Control {
        operation: u32,
        epoch: u64,
        reply: Channel,
        input: Option<OwnedHandle<CapabilityChannelObject>>,
        output: Option<OwnedHandle<CapabilityChannelObject>>,
    },
    Log {
        name: String,
        channel: Channel,
    },
    Stream {
        epoch: u64,
        access: u64,
        accepted: stream::Accepted,
    },
}
#[derive(Clone)]
pub struct Sender {
    queue: SyncSender<Event>,
    wake: Arc<Channel>,
}
pub fn mailbox() -> Result<(Sender, Receiver<Event>, Channel)> {
    let (tx, rx) = sync_channel(32);
    let (wake, reader) = hyper_os::channel::create_pair()?;
    Ok((
        Sender {
            queue: tx,
            wake: Arc::new(wake),
        },
        rx,
        reader,
    ))
}
impl Sender {
    fn send(&self, event: Event) -> bool {
        if self.queue.send(event).is_err() {
            return false;
        }
        // One unread wake is enough. A full wake channel already wakes the reactor.
        let _ = self.wake.as_byte_channel().try_send(&[1]);
        true
    }
}
fn spawn(
    name: &str,
    worker: impl FnOnce() + Send + 'static,
) -> Result<std::thread::JoinHandle<()>> {
    std::thread::Builder::new()
        .name(name.into())
        .spawn(worker)
        .map_err(|_| Error::Status(Status::NO_MEMORY))
}
pub fn control(listener: CapabilityChannel, sender: Sender) -> Result<()> {
    spawn("console-control", move || {
        loop {
            let mut bytes = [MaybeUninit::uninit(); contract::CONTROL_BYTES];
            let mut slots = [
                CapabilityReceiveSlot::new::<ByteChannelObject>(contract::REPLY_RIGHTS),
                CapabilityReceiveSlot::new::<CapabilityChannelObject>(contract::SERVER_RIGHTS),
                CapabilityReceiveSlot::new::<CapabilityChannelObject>(contract::SERVER_RIGHTS),
            ];
            let message =
                match listener.receive(hyper_os::DEADLINE_INFINITE, &mut bytes, &mut slots) {
                    Ok(message) => message,
                    Err(_) => return,
                };
            if message.bytes().len() != contract::CONTROL_BYTES
                || message.bytes()[..4] != contract::CONTROL_MAGIC
            {
                continue;
            }
            let b = message.bytes();
            let operation = u32::from_le_bytes([b[4], b[5], b[6], b[7]]);
            let mut epoch = [0; 8];
            epoch.copy_from_slice(&b[8..16]);
            let count = message.capability_count();
            let Ok(Some(reply)) = slots[0].take() else {
                continue;
            };
            let valid = (operation == contract::ATTACH && count == 3)
                || (operation == contract::RESET && count == 1);
            if !valid {
                let _ = reply
                    .as_byte_channel()
                    .try_send(&Status::INVALID_ARGUMENT.as_raw().to_le_bytes());
                continue;
            }
            let input = slots[1].take().ok().flatten();
            let output = slots[2].take().ok().flatten();
            if !sender.send(Event::Control {
                operation,
                epoch: u64::from_le_bytes(epoch),
                reply,
                input,
                output,
            }) {
                return;
            }
        }
    })
    .map(drop)
}
pub fn logs(
    listener: CapabilityChannel,
    sender: Sender,
    stop: Option<Arc<AtomicBool>>,
) -> Result<()> {
    spawn("console-log-admit", move || {
        loop {
            if stop
                .as_ref()
                .is_some_and(|stop| stop.load(Ordering::Acquire))
            {
                break;
            }
            let deadline = if stop.is_some() {
                let Ok(deadline) = hyper_os::time::deadline_after(Duration::from_millis(100))
                else {
                    break;
                };
                deadline.as_raw()
            } else {
                hyper_os::DEADLINE_INFINITE
            };
            let mut bytes = [MaybeUninit::uninit(); 4 + contract::MAX_LOG_NAME];
            let mut slots = [CapabilityReceiveSlot::new::<ByteChannelObject>(
                contract::SERVER_RIGHTS,
            )];
            let message = match listener.receive(deadline, &mut bytes, &mut slots) {
                Ok(message) => message,
                Err(Error::Status(Status::TIMED_OUT)) => continue,
                Err(_) => break,
            };
            if message.bytes().len() <= 4
                || message.bytes()[..4] != contract::LOG_MAGIC
                || message.capability_count() != 1
            {
                continue;
            }
            let Ok(name) = core::str::from_utf8(&message.bytes()[4..]) else {
                continue;
            };
            if name.is_empty() || name.as_bytes().contains(&0) {
                continue;
            }
            let name = name.to_owned();
            if let Ok(Some(channel)) = slots[0].take()
                && !sender.send(Event::Log { name, channel })
            {
                return;
            }
        }
        // Ordered behind the final accepted Log event in the same mailbox.
        // Drain acknowledgment cannot overtake an admitted but unprocessed log.
        let _ = sender.send(Event::LogsStopped);
    })
    .map(drop)
}
pub fn streams(
    listener: OwnedHandle<CapabilityChannelObject>,
    sender: Sender,
    generation: Arc<AtomicU64>,
    epoch: u64,
    access: u64,
) -> Result<std::thread::JoinHandle<()>> {
    let name = if access == stream::ACCESS_READ {
        "console-input-admit"
    } else {
        "console-output-admit"
    };
    spawn(name, move || {
        let listener = CapabilityChannel::from_handle(listener);
        while generation.load(Ordering::Acquire) == epoch {
            let deadline = match hyper_os::time::deadline_after(Duration::from_millis(100)) {
                Ok(d) => d.as_raw(),
                Err(_) => return,
            };
            match stream::accept_until(&listener, deadline) {
                Ok(accepted) => {
                    if !sender.send(Event::Stream {
                        epoch,
                        access,
                        accepted,
                    }) {
                        return;
                    }
                }
                // Rejected rendezvous completes the receiver too. A malformed
                // client must not close the shared connector for later clients.
                Err(Error::Status(
                    Status::TIMED_OUT
                    | Status::INVALID_ARGUMENT
                    | Status::BAD_HANDLE
                    | Status::ACCESS_DENIED,
                ))
                | Err(Error::InvalidResponse)
                | Err(Error::MessageTooLarge { .. })
                | Err(Error::UnexpectedObjectKind { .. }) => {}
                Err(_) => return,
            }
        }
    })
}
