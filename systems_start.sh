#!/bin/bash
cargo run -- --sequencer &
cargo run 0 --dbproxy &
cargo run 1 --dbproxy &
cargo run -- --scheduler 