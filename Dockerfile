FROM ubuntu:18.04

# WORKDIR /usr/src/db-in-k8s

COPY . .

# RUN rm /bin/sh && ln -s /bin/bash /bin/sh
RUN apt update -y && apt upgrade -y
RUN apt install sudo -y
# RUN sudo apt-get install git -y
# RUN sudo apt-get install curl -y
# RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
# # RUN chmod 777 ./rust_install.sh
# # RUN ./rust_install.sh -y
# RUN source $HOME/.cargo/env
# RUN printf '#!/bin/sh\nexport CARGO_HOME=/opt/cargo\nexec /bin/sh "$@"\n' >/usr/local/bin/sh
# RUN chmod +x /usr/local/bin/sh
# # RUN chmod 777 /root/.cargo/bin/
# RUN cargo install --path .

RUN apt-get update && apt-get install -y wget
RUN mkdir -m777 /opt/rust /opt/cargo
ENV RUSTUP_HOME=/opt/rust CARGO_HOME=/opt/cargo PATH=/opt/cargo/bin:$PATH
RUN wget --https-only --secure-protocol=TLSv1_2 -O- https://sh.rustup.rs | sh /dev/stdin -y
RUN rustup target add aarch64-unknown-linux-gnu	
RUN printf '#!/bin/sh\nexport CARGO_HOME=/opt/cargo\nexec /bin/sh "$@"\n' >/usr/local/bin/sh
RUN chmod +x /usr/local/bin/sh
RUN sudo apt-get install gcc -y
RUN cargo update -p lexical-core
RUN cargo build

CMD ["./docker_start.sh"]

