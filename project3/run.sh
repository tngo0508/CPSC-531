#!/usr/bin/env sh

if [ $# -lt 2 ]; then
  printf "missing file or command\n"
  exit 0
fi

FILE_NAME=$1
CMD=$2
VERBOSE=$3

sh project3.sh "$FILE_NAME" "$CMD" "$VERBOSE"
