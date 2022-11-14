#!/bin/bash
# cargo run -- --sequencer -c ./o2versioner/conf_scheduler.toml &
# cargo run -- --scheduler -c ./o2versioner/conf_scheduler.toml

cargo run -- --sequencer &
cargo run 0 --dbproxy &
cargo run 1 --dbproxy &
cargo run -- --scheduler