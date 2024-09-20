# Unique ID Generation

Make sure you have maelstrom installed. Build code with `go build .`. Then test with

```bash
/path/to/maelstrom test -w unique-ids --bin ./maelstrom-unique-id-generation --time-limit 30 --rate 1000 --node-count 3 --availability total --nemesis partition
```
