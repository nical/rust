// Copyright 2014 The Rust Project Developers. See the COPYRIGHT
// file at the top-level directory of this distribution and at
// http://rust-lang.org/COPYRIGHT.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// http://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

/// Shared channels
///
/// This is the flavor of channels which are not necessarily optimized for any
/// particular use case, but are the most general in how they are used. Shared
/// channels are cloneable allowing for multiple senders.
///
/// High level implementation details can be found in the comment of the parent
/// module. You'll also note that the implementation of the shared and stream
/// channels are quite similar, and this is no coincidence!

use int;
use iter::Iterator;
use kinds::Send;
use ops::Drop;
use option::{Option, Some, None};
use result::{Ok, Err, Result};
use rt::local::Local;
use rt::task::{Task, BlockedTask};
use rt::thread::Thread;
use sync::atomics;
use vec::OwnedVector;

use mpsc = sync::mpsc_queue;

static DISCONNECTED: int = int::min_value;
static FUDGE: int = 1024;

pub struct Packet<T> {
    queue: mpsc::Queue<T>,
    cnt: atomics::AtomicInt, // How many items are on this channel
    steals: int, // How many times has a port received without blocking?
    to_wake: Option<BlockedTask>, // Task to wake up

    // The number of channels which are currently using this packet.
    channels: atomics::AtomicInt,

    // See the discussion in Port::drop and the channel send methods for what
    // these are used for
    go_home: atomics::AtomicBool,
    sender_drain: atomics::AtomicInt,
}

pub enum Failure {
    Empty,
    Disconnected,
}

impl<T: Send> Packet<T> {
    pub fn new() -> Packet<T> {
        Packet {
            queue: mpsc::Queue::new(),
            cnt: atomics::AtomicInt::new(0),
            steals: 0,
            to_wake: None,
            channels: atomics::AtomicInt::new(1),
            go_home: atomics::AtomicBool::new(false),
            sender_drain: atomics::AtomicInt::new(0),
        }
    }

    pub fn send(&mut self, t: T) -> bool {
        // See Port::drop for what's going on
        if self.go_home.load(atomics::AcqRel) { return false }

        // Note that the multiple sender case is a little tricker
        // semantically than the single sender case. The logic for
        // incrementing is "add and if disconnected store disconnected".
        // This could end up leading some senders to believe that there
        // wasn't a disconnect if in fact there was a disconnect. This means
        // that while one thread is attempting to re-store the disconnected
        // states, other threads could walk through merrily incrementing
        // this very-negative disconnected count. To prevent senders from
        // spuriously attempting to send when the channels is actually
        // disconnected, the count has a ranged check here.
        //
        // This is also done for another reason. Remember that the return
        // value of this function is:
        //
        //  `true` == the data *may* be received, this essentially has no
        //            meaning
        //  `false` == the data will *never* be received, this has a lot of
        //             meaning
        //
        // In the SPSC case, we have a check of 'queue.is_empty()' to see
        // whether the data was actually received, but this same condition
        // means nothing in a multi-producer context. As a result, this
        // preflight check serves as the definitive "this will never be
        // received". Once we get beyond this check, we have permanently
        // entered the realm of "this may be received"
        if self.cnt.load(atomics::AcqRel) < DISCONNECTED + FUDGE {
            return false
        }

        self.queue.push(t);
        match self.cnt.fetch_add(1, atomics::SeqCst) {
            -1 => { self.wakeup(); }

            // In this case, we have possibly failed to send our data, and
            // we need to consider re-popping the data in order to fully
            // destroy it. We must arbitrate among the multiple senders,
            // however, because the queues that we're using are
            // single-consumer queues. In order to do this, all exiting
            // pushers will use an atomic count in order to count those
            // flowing through. Pushers who see 0 are required to drain as
            // much as possible, and then can only exit when they are the
            // only pusher (otherwise they must try again).
            n if n < DISCONNECTED + FUDGE => {
                // see the comment in 'try' for a shared channel for why this
                // window of "not disconnected" is ok.
                self.cnt.store(DISCONNECTED, atomics::SeqCst);

                if self.sender_drain.fetch_add(1, atomics::SeqCst) == 0 {
                    loop {
                        // drain the queue, for info on the thread yield see the
                        // discussion in try_recv
                        loop {
                            match self.queue.pop() {
                                mpsc::Data(..) => {}
                                mpsc::Empty => break,
                                mpsc::Inconsistent => Thread::yield_now(),
                            }
                        }
                        // maybe we're done, if we're not the last ones
                        // here, then we need to go try again.
                        if self.sender_drain.compare_and_swap(
                                1, 0, atomics::SeqCst) == 1 {
                            break
                        }
                    }

                    // At this point, there may still be data on the queue,
                    // but only if the count hasn't been incremented and
                    // some other sender hasn't finished pushing data just
                    // yet. That sender in question will drain its own data.
                }
            }

            // Can't make any assumptions about this case like in the SPSC case.
            _ => {}
        }

        true
    }

    pub fn recv(&mut self) -> Result<T, Failure> {
        // This code is essentially the exact same as that found in the stream
        // case (see stream.rs)
        match self.try_recv() {
            Err(Empty) => {}
            data => return data,
        }

        let task: ~Task = Local::take();
        task.deschedule(1, |task| {
            assert!(self.to_wake.is_none());
            self.to_wake = Some(task);
            let steals = self.steals;
            self.steals = 0;

            match self.cnt.fetch_sub(1 + steals, atomics::SeqCst) {
                DISCONNECTED => {
                    self.cnt.store(DISCONNECTED, atomics::SeqCst);
                    Err(self.to_wake.take_unwrap())
                }
                n if n - steals <= 0 => Ok(()),
                _ => Err(self.to_wake.take_unwrap()),
            }
        });

        match self.try_recv() {
            data @ Ok(..) => { self.steals -= 1; data }
            data => data,
        }
    }

    pub fn try_recv(&mut self) -> Result<T, Failure> {
        let ret = match self.queue.pop() {
            mpsc::Data(t) => Some(t),
            mpsc::Empty => None,

            // This is a bit of an interesting case. The channel is
            // reported as having data available, but our pop() has
            // failed due to the queue being in an inconsistent state.
            // This means that there is some pusher somewhere which has
            // yet to complete, but we are guaranteed that a pop will
            // eventually succeed. In this case, we spin in a yield loop
            // because the remote sender should finish their enqueue
            // operation "very quickly".
            //
            // Note that this yield loop does *not* attempt to do a green
            // yield (regardless of the context), but *always* performs an
            // OS-thread yield. The reasoning for this is that the pusher in
            // question which is causing the inconsistent state is
            // guaranteed to *not* be a blocked task (green tasks can't get
            // pre-empted), so it must be on a different OS thread. Also,
            // `try_recv` is normally a "guaranteed no rescheduling" context
            // in a green-thread situation. By yielding control of the
            // thread, we will hopefully allow time for the remote task on
            // the other OS thread to make progress.
            //
            // Avoiding this yield loop would require a different queue
            // abstraction which provides the guarantee that after M
            // pushes have succeeded, at least M pops will succeed. The
            // current queues guarantee that if there are N active
            // pushes, you can pop N times once all N have finished.
            mpsc::Inconsistent => {
                let data;
                loop {
                    Thread::yield_now();
                    match self.queue.pop() {
                        mpsc::Data(t) => { data = t; break }
                        mpsc::Empty => fail!("inconsistent => empty"),
                        mpsc::Inconsistent => {}
                    }
                }
                Some(data)
            }
        };
        match ret {
            Some(data) => { self.steals += 1; Ok(data) }

            // See the discussion in the stream implementation for why we try
            // again.
            None => {
                match self.cnt.load(atomics::SeqCst) {
                    n if n != DISCONNECTED => Err(Empty),
                    _ => {
                        match self.queue.pop() {
                            mpsc::Data(t) => Ok(t),
                            mpsc::Empty => Err(Disconnected),
                            // with no senders, an inconsistency is impossible.
                            mpsc::Inconsistent => unreachable!(),
                        }
                    }
                }
            }
        }
    }

    // Prepares this shared packet for a channel clone, essentially just bumping
    // a refcount.
    pub fn clone_chan(&mut self) {
        self.channels.fetch_add(1, atomics::SeqCst);
    }

    // Decrement the reference count on a channel. This is called whenever a
    // Chan is dropped and may end up waking up a receiver. It's the receiver's
    // responsibility on the other end to figure out that we've disconnected.
    pub fn drop_chan(&mut self) {
        match self.channels.fetch_sub(1, atomics::SeqCst) {
            1 => {
                match self.cnt.swap(DISCONNECTED, atomics::SeqCst) {
                    -1 => { self.wakeup(); }
                    DISCONNECTED => {}
                    n => { assert!(n >= 0); }
                }
            }
            n if n > 1 => {},
            n => fail!("bad number of channels left {}", n),
        }
    }

    // See the long discussion inside of stream.rs for why the queue is drained,
    // and why it is done in this fashion.
    pub fn drop_port(&mut self) {
        self.go_home.store(true, atomics::Relaxed);
        let mut steals = self.steals;
        while {
            let cnt = self.cnt.compare_and_swap(
                            steals, DISCONNECTED, atomics::SeqCst);
            cnt != DISCONNECTED && cnt != steals
        } {
            // See the discussion in 'try_recv' for why we yield
            // control of this thread.
            loop {
                match self.queue.pop() {
                    mpsc::Data(..) => { steals += 1; }
                    mpsc::Empty | mpsc::Inconsistent => break,
                }
            }
        }
    }

    // This function must have had at least an acquire fence before it to be
    // properly called.
    fn wakeup(&mut self) {
        self.to_wake.take_unwrap().wake().map(|t| t.reawaken(true));
    }
}

#[unsafe_destructor]
impl<T: Send> Drop for Packet<T> {
    fn drop(&mut self) {
        unsafe {
            // Note that this load is not only an assert for correctness about
            // disconnection, but also a proper fence before the read of
            // `to_wake`, so this assert cannot be removed with also removing
            // the `to_wake` assert.
            assert_eq!(self.cnt.load(atomics::SeqCst), DISCONNECTED);
            assert!(self.to_wake.is_none());
            assert_eq!(self.channels.load(atomics::SeqCst), 0);
        }
    }
}
