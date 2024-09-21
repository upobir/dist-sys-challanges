#!/usr/bin/env bash

grep ":msgs-per-op" store/latest/jepsen.log

echo

grep -A4 ":stable-latencies" store/latest/jepsen.log
