# Echo

See specs at [Echo](https://fly.io/dist-sys/1/)

## Running

Make sure you have maelstrom installed. Build code with `go build .`. Then test with

```bash
/path/to/maelstrom test -w echo --bin ./maelstrom-echo --node-count 1 --time-limit 10
```
