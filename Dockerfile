# syntax=docker/dockerfile:1

FROM rust:1.68.2 AS build

WORKDIR /app
COPY . .

# Download dependencies and compile files with optimizations turned on (puts the resulting binary in target/release)
RUN cargo build --release

# Container
FROM gcr.io/distroless/cc

COPY --from=build /app/target/release/tantivy_search /

EXPOSE 8079

ENTRYPOINT ["./tantivy_search"]
