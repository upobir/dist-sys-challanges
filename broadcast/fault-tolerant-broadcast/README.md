# Fault Tolerant Broadcast

See specs at [Fault Tolerant Broadcast](https://fly.io/dist-sys/3c/)

## Running

Make sure you have maelstrom installed. Build code with `go build .`. Then test with

```bash
/path/to/maelstrom test -w broadcast --bin ./maelstrom-fault-tolerant-broadcast --node-count 5 --time-limit 20 --rate 10 --nemesis partition
```
