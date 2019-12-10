****************************HOW TO RUN PROGRAM****************************
chmod 777 *.sh
chmod 777 *.py

To display helper menu
./main.py -h

To run query:
./main.py BINARY_FILE COMMAND [VERBOSE]

For query commands (q1, q2, q3, q4), add -v at the end see verbose output

EXAMPLE:
Query 1:
./main.py small.bin -p
./main.py small.bin -q1

Query 2:
if you already ran query 1, skip the next line:
./main.py small.bin -p

./main.py small.bin -q2

Query 3:
./main.py small.bin -s
./main.py small.bin -q3

Query 4:
if you already ran the query 3, skip the next line:
./main.py small.bin -c

./main.py small.bin -q4

*************HOW TO GENERATE LOGS WHEN EXECUTING QUERY COMMAND************
./run.sh binary_file  (q1 | q2 | q3 | q4) -v 

EXAMPLE:
Query 1 verbose output:
./run.sh small.bin q1 -v

****************************HOW TO CLEANUP FILES***************************
./cleanup.sh