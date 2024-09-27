# Efficient Broadcast Part II

See specs at [Efficient Broadcast Part II](https://fly.io/dist-sys/3e/)

## Running

Make sure you have maelstrom installed. Build code with `go build .`. Then test with

```bash
/path/to/maelstrom test -w broadcast --bin ./maelstrom-efficient-broadcast-part-2 --node-count 25 --time-limit 20 --rate 100 --latency 100 
```

Then run `./extract.sh` to extract relevant infos.
