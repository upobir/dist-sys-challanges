# Multi-Node Broadcast

See specs at [Multi-Node Broadcast](https://fly.io/dist-sys/3b/)

## Running

Make sure you have maelstrom installed. Build code with `go build .`. Then test with

```bash
/path/to/maelstrom test -w broadcast --bin ./maelstrom-multi-node-broadcast --node-count 5 --time-limit 20 --rate 10
```
