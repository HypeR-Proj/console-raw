// SPDX-FileCopyrightText: 2026 roolrz
// SPDX-License-Identifier: Apache-2.0

//! Bounded byte-only physical console transport. Terminal policy lives in the
//! formal terminal service. Independent log readers remain mirrored at all times.

mod admission;
mod log;
mod session;
use hyper_os::capability_channel::CapabilityChannel;
use hyper_os::handle::{ByteChannelObject, ConsoleObject, OwnedHandle};
use hyper_os::stream;
use hyper_os::wait::{ObjectSignals, WaitItem, wait_many};
use hyper_os::{Error, Result, Status};
use hyper_service::console as contract;
use log::Log;
use session::Session;
use std::collections::VecDeque;
use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicU64, Ordering},
};

const INPUT_LIMIT: usize = 4096;
const SESSION_LIMIT: usize = 16;
const LOG_LIMIT: usize = 32;
const _: () = assert!(3 + SESSION_LIMIT + LOG_LIMIT <= hyper_os::wait::MAX_ITEMS);
const LOG_MESSAGE_LIMIT: usize = 65_536;
type Channel = OwnedHandle<ByteChannelObject>;
fn run() -> Result<()> {
    let mut startup = hyper_rt::process::startup()?;
    let ready = startup.take_optional(
        hyper_os::startup::StartupPurpose::<ByteChannelObject>::new(0x8008_0020),
    )?;
    if ready.is_some() {
        // Prepare before spawning workers: their new stack mappings must not
        // race the fixed CURRENT residency transaction used for adoption.
        hyper_os::memory::lock_current_memory(startup.borrow(hyper_os::startup::ROOT_VMAR)?)?;
    }
    let console_owner = startup.take(contract::SYSTEM_CONSOLE)?;
    let console = console_owner.as_console();
    let control = CapabilityChannel::from_handle(startup.take(contract::CONTROL)?);
    let log_listener = CapabilityChannel::from_handle(startup.take(contract::LOG_CONNECTOR)?);
    let (sender, events, wake) = admission::mailbox()?;
    admission::control(control, sender.clone())?;
    let drain = startup.take_optional(
        hyper_os::startup::StartupPurpose::<ByteChannelObject>::new(0x8008_0014),
    )?;
    let stop_logs = drain.as_ref().map(|_| Arc::new(AtomicBool::new(false)));
    admission::logs(log_listener, sender.clone(), stop_logs.clone())?;
    let mut draining = false;
    let mut logs_stopped = false;
    let generation = Arc::new(AtomicU64::new(0));
    let mut stream_workers: Vec<std::thread::JoinHandle<()>> = Vec::with_capacity(4);
    let mut control_epoch = 0;
    let mut logs_first = false;
    let mut sessions = Vec::new();
    let mut logs = Vec::new();
    let mut input = VecDeque::with_capacity(INPUT_LIMIT);
    let mut scratch = vec![0; LOG_MESSAGE_LIMIT];
    if let Some(ready) = ready {
        ready.as_byte_channel().send(b"HYPER-SERVICE-ADOPTED/1")?;
    }
    loop {
        stream_workers.retain(|worker| !worker.is_finished());
        let mut wake_bytes = [0; 64];
        for _ in 0..32 {
            if wake.as_byte_channel().try_receive(&mut wake_bytes).is_err() {
                break;
            }
        }
        for _ in 0..32 {
            let Ok(event) = events.try_recv() else { break };
            match event {
                admission::Event::LogsStopped => logs_stopped = true,
                admission::Event::Log { name, channel } if logs.len() < LOG_LIMIT => {
                    logs.push(Log::new(name, channel))
                }
                admission::Event::Stream {
                    epoch,
                    access,
                    accepted,
                } if !draining
                    && epoch == generation.load(Ordering::Acquire)
                    && sessions.len() < SESSION_LIMIT
                    && access == accepted.access =>
                {
                    sessions.push(Session::new(accepted.channel, access))
                }
                admission::Event::Control {
                    operation,
                    epoch,
                    reply,
                    input: listener_in,
                    output: listener_out,
                } => {
                    let current = generation.load(Ordering::Acquire);
                    let result = if draining {
                        Err(Error::Status(Status::BUSY))
                    } else if epoch == 0 || epoch < control_epoch || current == u64::MAX {
                        Err(Error::Status(Status::INVALID_ARGUMENT))
                    } else if operation == contract::ATTACH && stream_workers.len() > 2 {
                        Err(Error::Status(Status::BUSY))
                    } else {
                        control_epoch = epoch;
                        let epoch = current + 1;
                        generation.store(epoch, Ordering::Release);
                        sessions.clear();
                        input.clear();
                        console.discard_input().and_then(|_| {
                            if operation == contract::ATTACH {
                                stream_workers.push(admission::streams(
                                    listener_in.ok_or(Error::MissingHandle)?,
                                    sender.clone(),
                                    generation.clone(),
                                    epoch,
                                    stream::ACCESS_READ,
                                )?);
                                stream_workers.push(admission::streams(
                                    listener_out.ok_or(Error::MissingHandle)?,
                                    sender.clone(),
                                    generation.clone(),
                                    epoch,
                                    stream::ACCESS_WRITE,
                                )?);
                            }
                            Ok(())
                        })
                    };
                    let status = match result {
                        Ok(()) => Status::OK,
                        Err(Error::Status(status)) => status,
                        Err(_) => Status::INVALID_ARGUMENT,
                    };
                    let _ = reply
                        .as_byte_channel()
                        .try_send(&status.as_raw().to_le_bytes());
                }
                _ => {}
            }
        }
        if let Some(drain) = &drain {
            let mut command = [0; 32];
            match drain.as_byte_channel().try_receive(&mut command) {
                Ok(count)
                    if &command[..count] == b"HYPER-CONSOLE-DRAIN/1" && sessions.is_empty() =>
                {
                    draining = true;
                    if let Some(stop) = &stop_logs {
                        stop.store(true, Ordering::Release);
                    }
                }
                Ok(_) => {
                    drain.as_byte_channel().send(b"HYPER-CONSOLE-BUSY/1")?;
                }
                Err(Error::Status(Status::WOULD_BLOCK | Status::PEER_CLOSED)) => {}
                Err(error) => return Err(error),
            }
        }
        // An unattached raw service is a log mirror only. During adoption the
        // old and new mirrors may overlap, but only the attached terminal owns
        // input. Do not consume bytes into an unreachable bootstrap queue.
        let reads_input = sessions
            .iter()
            .any(|session| session.access == stream::ACCESS_READ);
        let mut physical = [0; 256];
        let limit = physical.len().min(INPUT_LIMIT - input.len());
        if reads_input && limit != 0 {
            match console.try_read(&mut physical[..limit]) {
                Ok(count) => input.extend(&physical[..count]),
                Err(Error::Status(Status::WOULD_BLOCK)) => {}
                Err(error) => return Err(error),
            }
        }
        let now = hyper_os::time::monotonic_now()?.as_nanoseconds();
        if logs_first {
            for log in &mut logs {
                log.progress(&console, &mut scratch);
            }
        }
        for session in &mut sessions {
            session.receive(&console, &mut input);
            session.progress(&console, &mut input, now);
        }
        if !logs_first {
            for log in &mut logs {
                log.progress(&console, &mut scratch);
            }
        }
        logs_first = !logs_first;
        if !sessions.is_empty() {
            sessions.rotate_left(1);
        }
        if !logs.is_empty() {
            logs.rotate_left(1);
        }
        sessions.retain(|session| !session.dead);
        logs.retain(|log| !log.dead);
        if draining && logs_stopped && logs.is_empty() {
            if let Some(drain) = &drain {
                drain.as_byte_channel().send(b"HYPER-CONSOLE-DRAINED/1")?;
            }
            // The system supervisor owns retirement, even after the drain fence.
            loop {
                std::thread::park();
            }
        }
        let mut wait = Vec::with_capacity(3 + sessions.len() + logs.len());
        if let Some(drain) = &drain {
            wait.push(WaitItem::new(
                drain.as_handle_ref(),
                ObjectSignals::<ByteChannelObject>::READABLE,
            ));
        }
        wait.push(WaitItem::new(
            wake.as_handle_ref(),
            ObjectSignals::<ByteChannelObject>::READABLE,
        ));
        let writable =
            sessions.iter().any(|s| !s.output.is_empty()) || logs.iter().any(Log::has_output);
        if reads_input && input.len() < INPUT_LIMIT {
            let mut signals = ObjectSignals::<ConsoleObject>::READABLE;
            if writable {
                signals = signals.union(ObjectSignals::<ConsoleObject>::WRITABLE);
            }
            wait.push(WaitItem::new(console.as_handle_ref(), signals));
        } else if writable {
            wait.push(WaitItem::new(
                console.as_handle_ref(),
                ObjectSignals::<ConsoleObject>::WRITABLE,
            ));
        }
        for session in &sessions {
            let mut signals = ObjectSignals::<ByteChannelObject>::PEER_CLOSED;
            if session.reply.is_some() || session.read_blocked {
                signals = signals.union(ObjectSignals::<ByteChannelObject>::WRITABLE);
            }
            if session.reply.is_none() && session.output.is_empty() {
                signals = signals.union(ObjectSignals::<ByteChannelObject>::READABLE);
            }
            wait.push(WaitItem::new(session.channel.as_handle_ref(), signals));
        }
        for log in &logs {
            if !log.peer_closed {
                wait.push(WaitItem::new(
                    log.channel.as_handle_ref(),
                    ObjectSignals::<ByteChannelObject>::READABLE
                        .union(ObjectSignals::<ByteChannelObject>::PEER_CLOSED),
                ));
            }
        }
        let deadline = sessions
            .iter()
            // A data reply already waiting for queue space needs WRITABLE,
            // not an expired input deadline that would wake us continuously.
            .filter(|session| !session.read_blocked && session.reply.is_none())
            .filter_map(|session| session.read.map(|read| read.extra))
            .min()
            .unwrap_or(hyper_os::DEADLINE_INFINITE);
        match wait_many(&wait, deadline) {
            Ok(_) | Err(Error::Status(Status::TIMED_OUT)) => {}
            Err(error) => return Err(error),
        }
    }
}
fn main() -> std::process::ExitCode {
    match run() {
        Ok(()) => std::process::ExitCode::SUCCESS,
        Err(_) => std::process::ExitCode::FAILURE,
    }
}
