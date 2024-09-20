# Unique ID Generation

See specs at [Unique ID Generation](https://fly.io/dist-sys/2/)

## Running

Make sure you have maelstrom installed. Build code with `go build .`. Then test with

```bash
/path/to/maelstrom test -w unique-ids --bin ./maelstrom-unique-id-generation --time-limit 30 --rate 1000 --node-count 3 --availability total --nemesis partition
```
