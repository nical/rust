// Copyright 2014 The Rust Project Developers. See the COPYRIGHT
// file at the top-level directory of this distribution and at
// http://rust-lang.org/COPYRIGHT.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// http://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

fn periodical(n: int) -> Port<bool> {
    let (port, chan) = Chan::new();
    spawn(proc() {
        debug!("periodical {} - begin", n);
        loop {
            for _ in range(1, n) {
                debug!("periodical {} - sending...", n);
                if !chan.try_send(false) {
                    break
                }
                debug!("periodical {} - sent", n);
            }
            if !chan.try_send(true) {
                break
            }
        }
    });
    return port;
}

fn integers() -> Port<int> {
    let (port, chan) = Chan::new();
    spawn(proc() {
        debug!("integers - begin");
        let mut i = 1;
        loop {
            if !chan.try_send(i) {
                break
            }
            i = i + 1;
        }
    });
    return port;
}

pub fn main() {
    let ints = integers();
    let threes = periodical(3);
    let fives = periodical(5);
    for _ in range(1, 100) {
        match (ints.recv(), threes.recv(), fives.recv()) {
            (_, true, true) => println!("FizzBuzz"),
            (_, true, false) => println!("Fizz"),
            (_, false, true) => println!("Buzz"),
            (i, false, false) => println!("{}", i)
        }
    }
}
