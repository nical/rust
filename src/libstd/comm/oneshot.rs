// Copyright 2014 The Rust Project Developers. See the COPYRIGHT
// file at the top-level directory of this distribution and at
// http://rust-lang.org/COPYRIGHT.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// http://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

/// Oneshot channels/ports
///
/// This is the initial flavor of channels/ports used for comm module. This is
/// an optimization for the one-use case of a channel. The major optimization of
/// this type is to have one and exactly one allocation when the chan/port pair
/// is created.
///
/// Another possible optimization would be to not use an UnsafeArc box because
/// in theory we know when the shared packet can be deallocated (no real need
/// for the atomic reference counting), but I was having trouble how to destroy
/// the data early in a drop of a Port.
///
/// # Implementation
///
/// Oneshots are implemented around one atomic uint variable. This variable
/// indicates both the state of the port/chan but also contains any tasks
/// blocked on the port. All atomic operations happen on this one word.
///
/// In order to upgrade a oneshot channel, an upgrade is considered a disconnect
/// on behalf of the channel side of things (it can be mentally thought of as
/// consuming the port). This upgrade is then also stored in the shared packet.
/// The one caveat to consider is that when a port sees a disconnected channel
/// it must check for data because there is no "data plus upgrade" state.

use comm::Port;
use ops::Drop;
use kinds::Send;
use result::{Result, Ok, Err};
use option::{Some, None, Option};
use rt::task::{Task, BlockedTask};
use rt::local::Local;
use sync::atomics;
use util;

// Various states you can find a port in.
static EMPTY: uint = 0;
static DATA: uint = 1;
static DISCONNECTED: uint = 3;

pub struct Packet<T> {
    // Internal state of the chan/port pair (stores the blocked task as well)
    state: atomics::AtomicUint,
    // One-shot data slot location
    data: Option<T>,
    // when used for the second time, a oneshot channel must be upgraded, and
    // this contains the slot for the upgrade
    upgrade: MyUpgrade<T>,
}

pub enum Failure<T> {
    Empty,
    Disconnected,
    Upgraded(Port<T>),
}

enum MyUpgrade<T> {
    NothingSent,
    SendUsed,
    GoUp(Port<T>),
}

impl<T: Send> Packet<T> {
    pub fn new() -> Packet<T> {
        Packet {
            data: None,
            upgrade: NothingSent,
            state: atomics::AtomicUint::new(EMPTY),
        }
    }

    pub fn send(&mut self, t: T) -> bool {
        // Sanity check
        match self.upgrade {
            NothingSent => {}
            _ => fail!("sending on a oneshot that's already sent on "),
        }
        assert!(self.data.is_none());
        self.data = Some(t);
        self.upgrade = SendUsed;

        // This atomic swap uses a "Release" memory ordering to ensure that all
        // our previous memory writes are visible to the other thread (notably
        // the write of data/upgrade)
        match self.state.swap(DATA, atomics::Release) {
            // Sent the data, no one was waiting
            EMPTY => true,

            // Couldn't send the data, the port hung up first. We need to be
            // sure to deallocate the sent data (to not leave it stuck in the
            // queue)
            DISCONNECTED => {
                self.data.take_unwrap();
                false
            }

            // Not possible, these are one-use channels
            DATA => unreachable!(),

            // Anything else means that there was a task waiting on the other
            // end. We leave the 'DATA' state inside so it'll pick it up on the
            // other end.
            n => unsafe {
                let t = BlockedTask::cast_from_uint(n);
                t.wake().map(|t| t.reawaken(true));
                true
            }
        }
    }

    // Just tests whether this channel has been sent on or not, this is only
    // safe to use from the sender.
    pub fn sent(&self) -> bool {
        match self.upgrade {
            NothingSent => false,
            _ => true,
        }
    }

    pub fn recv(&mut self) -> Result<T, Failure<T>> {
        // Attempt to not block the task (it's a little expensive). If it looks
        // like we're not empty, then immediately go through to `try_recv`.
        //
        // These atomics use an Acquire memory ordering in order to have all the
        // previous writes of the releasing thread visible to us.
        if self.state.load(atomics::Acquire) == EMPTY {
            let t: ~Task = Local::take();
            t.deschedule(1, |task| {
                let n = unsafe { task.cast_to_uint() };
                match self.state.compare_and_swap(EMPTY, n, atomics::Acquire) {
                    // Nothing on the channel, we legitimately block
                    EMPTY => Ok(()),

                    // If there's data or it's a disconnected channel, then we
                    // failed the cmpxchg, so we just wake ourselves back up
                    DATA | DISCONNECTED => {
                        unsafe { Err(BlockedTask::cast_from_uint(n)) }
                    }

                    // Only one thread is allowed to sleep on this port
                    _ => unreachable!()
                }
            });
        }

        self.try_recv()
    }

    pub fn try_recv(&mut self) -> Result<T, Failure<T>> {
        // see above for why Acquire is used.
        match self.state.load(atomics::Acquire) {
            EMPTY => Err(Empty),

            // We saw some data on the channel, but the channel can be used
            // again to send us an upgrade. As a result, we need to re-insert
            // into the channel that there's no data available (otherwise we'll
            // just see DATA next time). This is done as a cmpxchg because if
            // the state changes under our feet we'd rather just see that state
            // change.
            DATA => {
                self.state.compare_and_swap(DATA, EMPTY, atomics::Acquire);
                match self.data.take() {
                    Some(data) => Ok(data),
                    None => unreachable!(),
                }
            }

            // There's no guarantee that we receive before an upgrade happens,
            // and an upgrade flags the channel as disconnected, so when we see
            // this we first need to check if there's data available and *then*
            // we go through and process the upgrade.
            DISCONNECTED => {
                match self.data.take() {
                    Some(data) => Ok(data),
                    None => {
                        match util::replace(&mut self.upgrade, SendUsed) {
                            SendUsed | NothingSent => Err(Disconnected),
                            GoUp(upgrade) => Err(Upgraded(upgrade))
                        }
                    }
                }
            }
            _ => unreachable!()
        }
    }

    // Returns whether the upgrade was completed. If the upgrade wasn't
    // completed, then the port couldn't get sent to the other half (it will
    // never receive it).
    pub fn upgrade(&mut self, up: Port<T>) -> bool {
        let prev = match self.upgrade {
            NothingSent => NothingSent,
            SendUsed => SendUsed,
            _ => fail!("upgrading again"),
        };
        self.upgrade = GoUp(up);

        // Use a Release memory ordering in order to make sure that our write to
        // `upgrade` is visible to the other thread.
        match self.state.swap(DISCONNECTED, atomics::Release) {
            // If the channel is empty or has data on it, then we're good to go.
            // Senders will check the data before the upgrade (in case we
            // plastered over the DATA state).
            DATA | EMPTY => true,

            // If the other end is already disconnected, then we failed the
            // upgrade. Be sure to trash the port we were given.
            DISCONNECTED => { self.upgrade = prev; false }

            // If someone's waiting, we gotta wake them up
            n => unsafe {
                let t = BlockedTask::cast_from_uint(n);
                t.wake().map(|t| t.reawaken(true));
                true
            }
        }
    }

    pub fn drop_chan(&mut self) {
        match self.state.swap(DISCONNECTED, atomics::SeqCst) {
            // In all of these states, there's nothing for a channel to do.
            DATA | DISCONNECTED | EMPTY => {}

            // If someone's waiting, we gotta wake them up
            n => unsafe {
                let t = BlockedTask::cast_from_uint(n);
                t.wake().map(|t| t.reawaken(true));
            }
        }
    }

    pub fn drop_port(&mut self) {
        // Use an Acquire memory ordering in order to see the data that the
        // senders are sending.
        match self.state.swap(DISCONNECTED, atomics::Acquire) {
            // An empty channel has nothing to do, and a remotely disconnected
            // channel also has nothing to do b/c we're about to run the drop
            // glue
            DISCONNECTED | EMPTY => {}

            // There's data on the channel, so make sure we destroy it promptly.
            // This is why not using an arc is a little difficult (need the box
            // to stay valid while we take the data).
            DATA => { self.data.take_unwrap(); }

            // We're the only ones that can block on this port
            _ => unreachable!()
        }
    }
}

//impl<T: Send> select::Packet for Packet<T> {
//    fn start_selection(&mut self, task: BlockedTask) -> Result<(), BlockedTask>{
//        let n = task.cast_to_uint();
//        match self.state.compare_and_swap(EMPTY, n, atomics::SeqCst) {
//            EMPTY => Ok(()),
//            DISCONNECTED | DATA => Err(unsafe {
//                BlockedTask::cast_from_uint(n)
//            }),
//            _ => unreachable!()
//        }
//    }
//
//    fn abort_selection(&mut self) {
//        match self.state.load(atomics::Acquire) {
//            EMPTY => unreachable!(),
//            // our task was stolen
//            DATA | DISCONNECTED => {}
//
//            // we've got a task
//            n => {
//                match self.state.compare_and_swap(EMPTY, n, atomics::SeqCst) {
//                    EMPTY => {
//                        let t = unsafe { BlockedTask::cast_from_uint(n) };
//                        t.trash();
//                    }
//                    DATA | DISCONNECTED => {} // stolen
//                    _ => unreachable!(),
//                }
//            }
//        }
//    }
//}

#[unsafe_destructor]
impl<T: Send> Drop for Packet<T> {
    fn drop(&mut self) {
        assert_eq!(self.state.load(atomics::Relaxed), DISCONNECTED);
    }
}
