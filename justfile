_default:
    @just --list

test-fast:
    cargo test --bin fast -- --show-output

watch-fast:
    watchexec -r -w ./line-filter/src/bin -- just test-fast