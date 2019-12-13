****************************HOW TO RUN PROGRAM****************************
chmod 777 *.sh
chmod 777 *.py

To display helper menu
./main.py -h

To run query:
./main.py BINARY_FILE COMMAND [VERBOSE]

For query commands (q1, q2, q3, q4), add -v at the end to see verbose output

EXAMPLE:
Query 1:
./main.py small.bin -q1

Query 2:
./main.py small.bin -q2

Query 3:
./main.py small.bin -s
./main.py small.bin -q3

Query 4:
./main.py small.bin -c
./main.py small.bin -q4

NOTE: Query 4 is not working as expected. The external sorting does not do the merging phase correctly