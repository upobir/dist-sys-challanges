# Single-Node Broadcast

See specs at [Single-Node Broadcast](https://fly.io/dist-sys/3a/)

## Running

Make sure you have maelstrom installed. Build code with `go build .`. Then test with

```bash
/path/to/maelstrom test -w broadcast --bin ./maelstrom-single-node-broadcast --node-count 1 --time-limit 20 --rate 10
```
