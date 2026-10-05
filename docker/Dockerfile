# Cross-compiles on the build host's native platform (no QEMU), then copies into a target-platform runtime.
FROM --platform=$BUILDPLATFORM rust:1.87-bookworm AS build
ARG TARGETARCH
RUN case "$TARGETARCH" in \
      amd64) echo x86_64 > /arch ;; arm64) echo aarch64 > /arch ;; *) exit 1 ;; \
    esac \
 && apt-get update && apt-get install -y --no-install-recommends gcc-$(cat /arch | tr _ -)-linux-gnu \
 && rustup target add $(cat /arch)-unknown-linux-gnu
WORKDIR /src
COPY . .
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/src/target \
    ARCH=$(cat /arch) && T=$ARCH-unknown-linux-gnu && TU=$(echo $T | tr a-z- A-Z_) \
 && export CARGO_TARGET_${TU}_LINKER=$ARCH-linux-gnu-gcc CC_$(echo $T | tr - _)=$ARCH-linux-gnu-gcc \
 && cargo build --release --locked --target $T -p bizon-stream -p fake-bqwrite \
 && cp target/$T/release/bizon-stream target/$T/release/fake-bqwrite /

# `--target binary --output type=local,dest=dist` exports just the worker binary.
FROM scratch AS binary
COPY --from=build /bizon-stream /

# debug-nonroot keeps busybox, so `kubectl cp` can fetch `capture` output.
FROM gcr.io/distroless/cc-debian12:debug-nonroot
COPY --from=build /bizon-stream /fake-bqwrite /usr/local/bin/
ENTRYPOINT ["/usr/local/bin/bizon-stream"]
