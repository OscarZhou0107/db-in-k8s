#!/bin/bash
cargo run -- --sequencer -c ./o2versioner/conf_scheduler.toml &
cargo run -- --scheduler -c ./o2versioner/conf_scheduler.toml