# syntax=docker/dockerfile:1
FROM rust:1.92-slim-bookworm AS build
RUN apt-get update && apt-get install -y --no-install-recommends protobuf-compiler pkg-config libssl-dev clang cmake git && rm -rf /var/lib/apt/lists/*
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY catalog catalog
COPY cli cli
# Moderate optimization keeps local kind/CI builds practical for DataFusion.
ENV CARGO_PROFILE_RELEASE_OPT_LEVEL=1 CARGO_PROFILE_RELEASE_CODEGEN_UNITS=256 CARGO_PROFILE_RELEASE_DEBUG=0
RUN --mount=type=cache,target=/usr/local/cargo/registry --mount=type=cache,target=/usr/local/cargo/git --mount=type=cache,target=/src/target \
    cargo clean --release -p lyra-catalog -p lyra-catalog-cli && \
    cargo build --locked --release -p lyra-catalog-cli && install -m 0755 target/release/lyra-catalog /usr/local/bin/lyra-catalog

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates && rm -rf /var/lib/apt/lists/*
COPY --from=build /usr/local/bin/lyra-catalog /usr/local/bin/lyra-catalog
USER 10001:10001
EXPOSE 5432 8080
ENTRYPOINT ["/usr/local/bin/lyra-catalog"]
