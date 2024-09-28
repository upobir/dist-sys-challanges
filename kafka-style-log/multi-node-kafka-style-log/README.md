# Multi-Node Kafka-Style Log

See specs at [Multi-Node Kafka-Style Log](https://fly.io/dist-sys/5b/)

## Running

Make sure you have maelstrom installed. Build code with `go build .`. Then test with

```bash
/path/to/maelstrom test -w kafka --bin ./maelstrom-multi-node-kafka-style-log --node-count 2 --concurrency 2n --time-limit 20 --rate 1000
```
