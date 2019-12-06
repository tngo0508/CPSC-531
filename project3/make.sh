#!/usr/bin/env sh

(time python3 old.py) |& tee result.log | tail -5 | tee time.log

# { time python3 old.py small.bin; } 2> time.txt