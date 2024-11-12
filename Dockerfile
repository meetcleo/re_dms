FROM buildpack-deps:noble

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
    ./rustup-init -y --no-modify-path --default-toolchain 1.78.0; \
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
    && cargo build --release --features with_sentry

# copy source and rebuild
COPY src/ src/
RUN set -x\
    && find target/release -type f -name "$(echo "${app}" | tr '-' '_')*" -exec touch -t 200001010000 {} +\
    && cargo build --release --features with_sentry
