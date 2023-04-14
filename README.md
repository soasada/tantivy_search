# tantivy_search

Load test:

1. `RUSTFLAGS=-g cargo build --release`
2. Install `heaptrack` and `heaptrack-gui`
3. Install https://github.com/ddosify/ddosify
4. Run the application: `heaptrack target/release/tantivy_search`
5. `cd load_testing`
6. `ddosify -config config.json`

Docker:

`docker-compose up --build`

`docker stats tantivy_search`

