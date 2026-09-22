# ---- build stage ----------------------------------------------------------
# Alpine, so the binary links against musl and is fully static. That is what
# lets the runtime stage be a bare Alpine rather than a distribution that has
# to match this one's libc.
FROM rust:1.98.1-alpine3.24 AS build
WORKDIR /app

# musl-dev provides the C toolchain the ring/rustls build scripts need; the
# rest is what a static link wants at the end.
RUN apk add --no-cache musl-dev pkgconf

# Cache the dependency graph.
COPY Cargo.toml Cargo.lock ./
RUN mkdir -p src && \
    echo "fn main() {}" > src/main.rs && \
    echo "" > src/lib.rs && \
    cargo build --release 2>/dev/null || true

COPY src ./src
COPY migrations ./migrations
# The recipes are compiler inputs (`include_str!` in src/recipes.rs): the
# build fails without them, whatever the checkout looked like.
COPY recipes ./recipes
RUN touch src/main.rs src/lib.rs && cargo build --release

# ---- runtime stage ---------------------------------------------------------
# The build is musl-static, so this stage no longer has to track the builder's
# distribution — the pairing that broke v0.4.0, where a glibc binary met an
# older glibc runtime and the image would not start. Alpine here is a choice
# about size, not a constraint.
#
# The release workflow still boots this image against a real Postgres and makes
# an authenticated MCP call before publishing. That gate, not this comment, is
# what proves the pairing works.
FROM alpine:3.24
RUN apk add --no-cache ca-certificates curl && \
    adduser --system --home /app --disabled-password bus
WORKDIR /app
COPY --from=build /app/target/release/ai-crew-sync /usr/local/bin/ai-crew-sync
USER bus

EXPOSE 8787
HEALTHCHECK --interval=30s --timeout=3s \
    CMD curl -fsS http://localhost:8787/health || exit 1

ENTRYPOINT ["ai-crew-sync"]
CMD ["serve"]
