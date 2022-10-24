FROM rust:latest

# COPY target/debug/o2versioner /bin/o2versioner

# COPY o2versioner/conf.toml /usr/conf.toml

EXPOSE 2077

# CMD ["./bin/o2versioner", "--config", "/usr/conf.toml", "--sequencer"]

WORKDIR /usr/src/o2versioner
COPY . .

# RUN cargo install --path .

# CMD ["o2versioner"]