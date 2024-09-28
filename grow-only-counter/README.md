# Grow-Only Counter

See specs at [Grow-Only Counter](https://fly.io/dist-sys/4/)

## Running

Make sure you have maelstrom installed. Build code with `go build .`. Then test with

```bash
/path/to/maelstrom test -w g-counter --bin ./maelstrom-grow-only-counter --node-count 3 --rate 100 --time-limit 20 --nemesis partition
```
