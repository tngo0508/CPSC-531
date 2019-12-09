#!/usr/bin/env sh

if [ $# -lt 2 ]; then
  printf "missing file or command\\n"
  exit 0
fi

FILE_NAME=$1
CMD=$2
VERBOSE=$3
OUT=''

if [ -z "$FILE_NAME" ]; then
  printf "missing file (small.bin or large.bin)\\n"
  exit 0
fi

if [ ! -f "$FILE_NAME" ]; then
  printf "%s Not found\\n" "$FILE_NAME"
  exit 0
fi

if [ -z "$CMD" ]; then
  printf "missing command\\n"
  python3 main.py -h
  exit 0
fi

if [ "$CMD" = '-h' ]; then
  python3 main.py -h
  exit 1
fi

case $CMD in
  '-p')
    OUT="primary"
    ;;
  '-s')
    OUT="secondary"
    ;;
  '-c')
    OUT="cluster"
    ;;
  '-q1')
    OUT="query1"
    ;;
  '-q2')
    OUT="query2"
    ;;
  '-q3')
    OUT="query3"
    ;;
  '-q4')
    OUT="query4"
    ;;
esac

DATE=$(date +"%Y-%m-%d_%H-%M-%S")

if [ -z "$VERBOSE" ];then
  (time -p python3 main.py "$FILE_NAME" "$CMD") 2>&1 | tee "$DATE"_"$FILE_NAME"_"$OUT".log;
  wait
  tail -3 "$DATE"_"$FILE_NAME"_"$OUT".log | tee "$DATE"_"$FILE_NAME"_"$OUT"_time.log
  exit 1
else
  (time -p python3 main.py "$FILE_NAME" "$CMD" "$VERBOSE") 2>&1 | tee "$DATE"_"$FILE_NAME"_"$OUT".log;
  wait
  tail -3 "$DATE"_"$FILE_NAME"_"$OUT".log | tee "$DATE"_"$FILE_NAME"_"$OUT"_time.log
  exit 1
fi

