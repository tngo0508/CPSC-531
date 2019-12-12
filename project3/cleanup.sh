#!/usr/bin/env sh
#title           :cleanup.sh
#description     :This script will cleanup logs, DBM, and binary files
#author		     :Thomas Ngo
#date            :12/09/2019
#usage		     :bash cleanup.sh or ./cleanup.sh
#notes           :Run chmod 777 *.sh to give the execute privilege for current user
#==================================================================================
rm -f *.log
rm -f *.db
rm -f cluster_index_file.bin
rm -f secondary_index_file.bin
rm -f file*
# rm -f sorted_small.bin