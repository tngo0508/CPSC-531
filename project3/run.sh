#!/usr/bin/env sh
#title           :cleanup.sh
#description     :This script runs on top of project3.sh
#author		       :Thomas Ngo
#date            :12/09/2019
#usage		       :./run.sh small.bin -h [-v]
#notes           :Run chmod 777 *.sh to give the execute privilege for current user
#==================================================================================
if [ $# -lt 2 ]; then
  printf "missing file or command\n"
  python3 main.py -h
  exit 0
fi

FILE_NAME=$1
CMD=$2
VERBOSE=$3

sh project3.sh "$FILE_NAME" "$CMD" "$VERBOSE"
