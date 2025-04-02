_default:
    @just --list

test-fast:
    cargo test --bin fast -- --show-output

watch-fast:
    watchexec -r -w ./line-filter/src/bin -- just test-fast

history:
    cargo run --bin history -- --redis-url redis://192.168.9.37:6379 --db-path /tmp/my.db

watch-history:
    watchexec -r -w ./line-filter/src/bin -- just history