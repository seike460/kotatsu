# syntax=docker/dockerfile:1
# kotatsud gateway + kotatsu CLI — single image, two entrypoints.
#   docker run kotatsu                        (ENTRYPOINT is kotatsud)
#   docker run --entrypoint kotatsu kotatsu image list

FROM rust:1-bookworm@sha256:59037199c44290f2befcdd58dcc540164763fc296950255aaefeef096a1866b0 AS build
WORKDIR /src
COPY . .
RUN cargo build --release --locked -p kotatsud -p kotatsu-cli

FROM debian:bookworm-slim@sha256:3783cc01769c7b2b1b83a5c5ad96c815348e28ed7da68e2e3687004faa906251
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && install -d -o nobody -g nogroup /var/lib/kotatsu
COPY --from=build /src/target/release/kotatsud /usr/local/bin/kotatsud
COPY --from=build /src/target/release/kotatsu /usr/local/bin/kotatsu
# Default --state-db resolves to $XDG_DATA_HOME/kotatsu/bindings.db.
ENV XDG_DATA_HOME=/var/lib/kotatsu
USER nobody
ENTRYPOINT ["kotatsud"]
