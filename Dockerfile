FROM miakias/system_base:latest

COPY . .
# RUN cargo update -p lexical-core
RUN cargo build

# Specify the port number that needs to be exposed
EXPOSE 2077

# # for version 1.0
# CMD ["./systems_start.sh"]