# > Start > Borrowed from: https://github.com/rust-lang/docker-rust-nightly/blob/2896708e58424ce0495c83f0364106e4f93b31bf/buster/Dockerfile
# As of Dec, 2021, we run re_dms on Ubuntu 18.04 which has glibc 2.27. However, Rust official docker images are based on directly on Debian. Ubuntu 18.04 is based
# on Debian Buster, however Buster runs glibc 2.28. Hence, building our executable against a Buster Debian image breaks because it expects glibc 2.28 at
# runtime (and it's not there). We've essentially created our own Rust docker image against Ubuntu 18.04 in order to build our executable guaranteed to be compatible with our deploy target.
FROM buildpack-deps:bionic

ENV RUSTUP_HOME=/usr/local/rustup \
    CARGO_HOME=/usr/local/cargo \
    PATH=/usr/local/cargo/bin:$PATH

RUN set -eux; \
    dpkgArch="$(dpkg --print-architecture)"; \
    case "${dpkgArch##*-}" in \
        amd64) rustArch='x86_64-unknown-linux-gnu' ;; \
        arm64) rustArch='aarch64-unknown-linux-gnu' ;; \
        *) echo >&2 "unsupported architecture: ${dpkgArch}"; exit 1 ;; \
    esac; \
    \
    url="https://static.rust-lang.org/rustup/dist/${rustArch}/rustup-init"; \
    wget "$url"; \
    chmod +x rustup-init; \
    ./rustup-init -y --no-modify-path --default-toolchain nightly; \
    rm rustup-init; \
    chmod -R a+w $RUSTUP_HOME $CARGO_HOME; \
    rustup --version; \
    cargo --version; \
    rustc --version;
# > end

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
    && cargo build --release --features with_rollbar

# copy source and rebuild
COPY src/ src/
RUN set -x\
    && find target/release -type f -name "$(echo "${app}" | tr '-' '_')*" -exec touch -t 200001010000 {} +\
    && cargo build --release --features with_rollbar
