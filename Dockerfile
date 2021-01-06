FROM rustlang/rust:nightly-stretch as builder

# Two step build: one to build dependences, one to build the re_dms app, which allows for faster docker builds once the dependencies layer has been built (it will rarely change)
# app
ENV app=re_dms

# dependencies
WORKDIR /tmp/${app}
COPY Cargo.toml Cargo.lock ./

# compile dependencies
RUN set -x\
    && mkdir -p src\
    && echo "fn main() {println!(\"broken\")}" > src/main.rs\
    && cargo build --release

# copy source and rebuild
COPY src/ src/
RUN set -x\
    && find target/release -type f -name "$(echo "${app}" | tr '-' '_')*" -exec touch -t 200001010000 {} +\
    && cargo build --release

# Build separate container for actually running what we've built in the previous step - keeps container size down
# FROM rust:slim
# ENV app=re_dms
# RUN apt-get update && apt-get install -y --no-install-recommends postgresql-client
# COPY --from=builder /tmp/${app}/target/release/${app} ${app}

# CMD ["/bin/sh", "-c", "pg_recvlogical --create-slot --start --if-not-exists --fsync-interval=0 --file=- --plugin=test_decoding --slot=re_dms -d postgresql://joshfleck@host.docker.internal/cleo_development | ./${app}"]

