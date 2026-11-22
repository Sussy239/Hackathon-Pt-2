FROM --platform=linux/amd64 rust:1.83-bookworm AS builder
WORKDIR /app

COPY Cargo.toml ./
RUN mkdir src && echo "fn main() {}" > src/main.rs && cargo build --release && rm -rf src

COPY src ./src
RUN touch src/main.rs && cargo build --release

FROM --platform=linux/amd64 debian:bookworm-slim
RUN useradd -m -u 1000 runner
COPY --from=builder /app/target/release/shopping-sessions /usr/local/bin/shopping-sessions
USER 1000
ENTRYPOINT ["/usr/local/bin/shopping-sessions"]
