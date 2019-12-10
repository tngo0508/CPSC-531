#!/usr/bin/env python3
"""CPSC 531 - Fall 2019 - Project 3

Description: see details at https://docs.google.com/document/d/1RElcA4bT14Wwh-KBfCl4d-_VTnqm1bNpSxK8dnuYy_8/edit

"""

from struct import unpack, pack
from collections import namedtuple
from datetime import date
from datetime import datetime
import dbm.ndbm
import os
import argparse

__author__ = "Thomas Ngo, Danh Pham, Su Htet"
__copyright__ = "Copyright 2019, CPSC 531 - Fall 2019 - Project 3"
__credits__ = "[Thomas Ngo, Danh Pham, Su Htet, Kenytt Avery]"
__email__ = "tngo0508@csu.fullerton.edu"

def get_num_blocks(data_file):
    """Function calculates the total number of disk blocks.

    Arg:
        data_file (str): file is required to calculate

    Returns:
        int: the number of block if it is larger than 0. Otherwise, return 1

    """
    try:
        f = open(data_file)
        f.close()
    except FileNotFoundError:
        print('Cannot open' + data_file)

    # One disk block size = 4096 bytes
    file_size = os.stat(data_file).st_size
    num_blocks = file_size // 4096
    return 1 if num_blocks == 0 else num_blocks


def create_primary_index(data_file):
    """Function creates primary index for query 1 and 2.

    Given a binary file, this function generates an index file

    Arg:
        data_file (str): Original data files. Either large.bin or small.bin

    """
    num_blocks = get_num_blocks(data_file)
    with dbm.ndbm.open('index', 'n') as db:
        offset = 0

        # use auto-increment for primary index start from 1
        for i in range(1, num_blocks + 1):
            db[pack('I', i)] = pack('I', offset)
            offset += 4096


def table_scan(data_file, verbose):
    """Function performs query 1 - Table scan

    Given a DBM files, this function does the table scan.
    Read the file block-by-block, list the SSN, first name, and last name
    of all users  under age 21. 

    Args:
        data_file (str): Original data files. Either large.bin or small.bin
        verbose (str): command to explicitly print out the result tuples

    """
    count = 0
    Person = namedtuple(
        'Person', 'fname, lname, ssn, age')
    try:
        with dbm.ndbm.open('index', 'r') as db:
            with open(data_file, 'rb') as f:
                for record in db.keys():
                    offset = unpack('I', db[record])[0]
                    f.seek(offset)
                    data = f.read(4096)

                    start = 0

                    # The block factor (bfr) = 10. 1 block contains 10 records
                    for _ in range(10):

                        # the record length R = 404 bytes.
                        # 405 is used due to offset in Python when slicing
                        fname, lname, _, _, _, _, day, month, year, ssn, _, _, _ = unpack(
                            '20s20s70s40s80s25s3I12s25s50s50s', data[start:start+405])

                        age = calculate_age(day, month, year)
                        if age < 21:
                            fname = fname.replace(b'\x00', b'')
                            lname = lname.replace(b'\x00', b'')
                            ssn = ssn.replace(b'\x00', b'')
                            if verbose:
                                print(Person(fname, lname, ssn, age))
                            count += 1
                        start += 405
    except dbm.error:
        print('You have not created primary index yet')
        print('Run -p or --primary before executing this command')
        exit(0)
    print("Number of records: {}".format(count))


def calculate_age(day, month, year):
    """Function calculate user's age

    Args:
        day (int): day in DOB
        month (int): month in DOB
        year (int): year in DOB

    Returns:
        int: age of a user

    """
    # see details at
    # https://stackoverflow.com/questions/2217488/age-from-birthdate-in-python
    today = date.today()
    return today.year - year - ((today.month, today.day) < (month, day))


def uniqueness_check(data_file, verbose):
    """Function performs query 2 - Uniqueness check

    The SSN is supposed to be a unique identifier, but it was not UNIQUE in the
    given data files. Read the file block-by-block, using a DBM database to check
    whether the SSN has been seen before.

    Args:
        data_file (str): Original data files. Either large.bin or small.bin
        verbose (str): command to explicitly print out the result tuples

    """
    count = 0
    seen = set()
    try:
        with dbm.ndbm.open('index', 'r') as db:
            with open(data_file, 'rb') as f:
                for record in db.keys():
                    offset = unpack('I', db[record])[0]
                    f.seek(offset)
                    data = f.read(4096)

                    start = 0

                    # The block factor (bfr) = 10. 1 block contains 10 records
                    for _ in range(10):

                        # the record length R = 404 bytes.
                        # 405 is used due to offset in Python when slicing
                        _, _, _, _, _, _, _, _, _, ssn, _, _, _ = unpack(
                            '20s20s70s40s80s25sIII12s25s50s50s', data[start:start+405])

                        ssn = ssn.replace(b'\x00', b'')
                        if ssn not in seen:
                            seen.add(ssn)
                        else:
                            count += 1
                            if verbose:
                                print(ssn)
                        start += 405
    except dbm.error:
        print('You have not created primary index yet')
        print('Run -p or --primary before executing this command')
        exit(0)
    print("Number of records: {}".format(count))


def generate_secondary_index_file(data_file):
    """Function generates an index file which is used to create a DBM file for
    the secondary index.

    Args:
        data_file (str): Original data files. Either large.bin or small.bin

    """
    num_blocks = get_num_blocks(data_file)
    with open('secondary_index_file.bin', 'wb') as secondary_index_file:
        with open(data_file, 'rb') as f:
            record_pointer = 0
            for _ in range(num_blocks):
                record_pointer = f.tell()
                data = f.read(4096)

                start = 0

                # The block factor (bfr) = 10. 1 block contains 10 records
                for _ in range(10):

                    # Read a record from data file
                    entry = data[start:start+405]

                    # check for empty records
                    if not entry:
                        break

                    # The record length R = 404 bytes.
                    # 405 is used due to offset in Python when slicing
                    _, _, _, _, _, _, day, month, year, _, _, _, _ = unpack(
                        '20s20s70s40s80s25s3I12s25s50s50s', entry)

                    # Prepare index entry on secondary index file
                    index_entry = pack('4I', day, month, year, record_pointer)

                    # Write an entry to secondary index file
                    secondary_index_file.write(index_entry)

                    start += 405
                    record_pointer += 405


def create_secondary_index(secondary_index_file):
    """Function creates secondary index on birthdate for query 3
    given a secondary index file

    In the generated DBM, key is the birthdate and value is a list of 
    record pointers that points to the records have the same birthdate

    Args:
        secondary_index_file (str): an index file (binary file) that is
            generated from function generate_secondary_index_file()

    """
    num_blocks = get_num_blocks(secondary_index_file)
    with dbm.ndbm.open('secondary_index', 'n') as secondary_index:
        with open(secondary_index_file, 'rb') as f:
            record_pointer = 0
            for _ in range(num_blocks):
                record_pointer = f.tell()
                data = f.read(4096)

                start = 0

                # The size of each index entry = 16 bytes because each entry
                # contains a birthdate (12 bytes) and a record pointer (4 bytes)
                # which points to original data files.

                # The block factor (bfr) = (4096 // 16) = 256. 
                # 1 block contains 256 records.
                for _ in range(256):
                    entry = data[start:start+16]

                    # Check for empty entry
                    if not entry:
                        break
                    
                    # Each entry = 16 bytes
                    day, month, year, record_pointer = unpack(
                        '4I', entry)

                    birthdate = pack('3I', day, month, year)

                    # Check for records that have same birthdate, then
                    # group them together
                    if birthdate not in secondary_index:
                        secondary_index[birthdate] = pack('I', record_pointer)
                    else:

                        # Append the record pointers that have same birthdate
                        # to a list. We don't need to use a delimiter here to
                        # separate the records because we know that each record
                        # is packed exactly 4 bytes.
                        new_record_pointer = secondary_index[birthdate] + pack(
                            'I', record_pointer)
                        secondary_index[birthdate] = new_record_pointer
                    start += 16
                    record_pointer += 16


def table_scan_on_secondary_index(data_file, verbose):
    """Function performs query 3 - Secondary index

    Use the DBM that is generated from create_secondary_index(), loop through
    all items in the index to find the location on disk of all users under age 
    21. Read only the relevant disk blocks in order to list the SSN, first 
    name, and last name of all users under age 21.

    Args:
        data_file (str): Original data files. Either large.bin or small.bin
        verbose (str): command to explicitly print out the result tuples

    """
    count = 0
    Person = namedtuple(
        'Person', 'fname, lname, ssn, age')
    try:
        with dbm.ndbm.open('secondary_index', 'r') as secondary_index:
            with open(data_file, 'rb') as f:
                for birthdate in secondary_index.keys():
                    day, month, year = unpack('3I', birthdate)
                    age = calculate_age(day, month, year)
                    if age < 21:

                        # Each record pointer is represented as 4 bytes 
                        num_record_pointers = len(secondary_index[birthdate]) // 4
                        format_char = str(num_record_pointers) + 'I'
                        record_pointers = unpack(
                            format_char, secondary_index[birthdate])

                        for record_pointer in record_pointers:
                            f.seek(record_pointer)
                            data = f.read(405)
                            fname, lname, _, _, _, _, _, _, _, ssn, _, _, _ = unpack(
                                '20s20s70s40s80s25sIII12s25s50s50s', data)
                            fname = fname.replace(b'\x00', b'')
                            lname = lname.replace(b'\x00', b'')
                            ssn = ssn.replace(b'\x00', b'')
                            count += 1
                            if verbose:
                                print(Person(fname, lname, ssn, age))
    except dbm.error:
        print('You haven not created secondary index yet')
        print('Run -s or --secondary before executing this command')
        exit(0)
    print("Number of records: {}".format(count))


def generate_cluster_index_file(secondary_index_file):
    """Function generates cluster index file given another index file

    This function sorts the birthdates on seconary index file in ascending
    order w.r.t age (from younger age to older age). Then, it generates a 
    cluster index file which is used to create cluster index.

    Args:
        secondary_index_file (str): index file (binary file) that is
            generated from function generate_secondary_index_file()

    """
    num_blocks = get_num_blocks(secondary_index_file)
    entries = []

    with open(secondary_index_file, 'rb') as f:
        record_pointer = 0
        for _ in range(num_blocks):
            record_pointer = f.tell()
            data = f.read(4096)

            start = 0

            # The size of each index entry = 16 bytes because each entry
            # contains a birthdate (12 bytes) and a record pointer (4 bytes)
            # which points to original data files.

            # The block factor (bfr) = (4096 // 16) = 256. 
            # 1 block contains 256 records.
            for _ in range(256):

                # Each entry = 16 bytes
                entry = data[start:start+16]

                if not entry:
                    break
                
                day, month, year, record_pointer = unpack(
                    '4I', entry)
                birthdate = '{}-{}-{}'.format(day, month, year)
                birthdate_in_bytes = pack('3I', day, month, year)
                record_pointer_in_bytes = pack('I', record_pointer)
                entries.append((datetime.strptime(birthdate, '%d-%m-%Y').date(),
                                birthdate_in_bytes, record_pointer_in_bytes))
                start += 16
                record_pointer += 16

    entries.sort(key=lambda x: x[0], reverse=True)
    if f.closed:
        with open('cluster_index_file.bin', 'wb') as secondary_index_file:
            for entry in entries:
                secondary_index_file.write(entry[1]+entry[2])


def create_cluster_index(cluster_file_name):
    """Function creates cluster index and generates a DBM for query 4

    In addition, it creates clustering index with a separate block cluster for
    each group of records that share the same value for the clustering field.
    See details at Figure 17.2 and 17.3 in Fundamentals of database system 
    (7th) textbook for reference.

    Args:
        cluster_file_name (str): cluster index file

    """
    num_blocks = get_num_blocks(cluster_file_name)
    with dbm.ndbm.open('cluster_index', 'n') as cluster_index:
        with open(cluster_file_name, 'rb') as f:
            offset = 0
            for _ in range(num_blocks):
                data = f.read(4096)

                # Use block anchor for cluster index, this is similar to
                # create_primary_index(). But in this case, the keys are not
                # distinct values as auto-increment values. We need to check for
                # nonkey field so that we don't accidentally reassign an
                # existing key to a new offset value.
                birthdate = data[:12]
                if str(birthdate) not in cluster_file_name:
                    cluster_index[birthdate] = pack('I', offset)
                offset += 4096


def table_scan_on_cluster_index(data_file, verbose):
    """Function performs query 4 - Clustered index

    This function uses clustering index to perform query similar to query 3.
    It lists the SSN, first name, and last name of all users under age 21

    Args:
        data_file (str): Original data files. Either large.bin or small.bin
        verbose (str): command to explicitly print out the result tuples

    """
    count = 0
    Person = namedtuple(
        'Person', 'fname, lname, ssn, age')
    birthdates = []
    start_offsets = []
    try:
        with dbm.ndbm.open('cluster_index', 'r') as cluster_index:
            for birthdate_in_bytes in cluster_index.keys():
                day, month, year = unpack('3I', birthdate_in_bytes)
                age = calculate_age(day, month, year)
                if age < 21:
                    birthdates.append(birthdate_in_bytes)

            sorted_birthdates = []
            for birthdate_in_bytes in birthdates:
                day, month, year = unpack(
                    '3I', birthdate_in_bytes)
                birthdate = '{}-{}-{}'.format(day, month, year)
                birthdate_in_bytes = pack('3I', day, month, year)
                sorted_birthdates.append((datetime.strptime(birthdate, '%d-%m-%Y').date(),
                                        birthdate_in_bytes))

            sorted_birthdates.sort(key=lambda x: x[0], reverse=True)

            start_offsets = [cluster_index[birthdate[1]]
                            for birthdate in sorted_birthdates]
    except dbm.error:
        print('You have not created clustering index yet')
        print('Run -c or --cluster before executing this command')
        exit(0)

    start_offset = unpack('I', start_offsets[0])[0]
    with open('cluster_index_file.bin', 'rb') as cluster_index_file:
        with open(data_file, 'rb') as f:
            
            # Because we create sparse index on an ordering nonkey fields of 
            # data file (secondary index file). From current cluster index, 
            # we need to check both upward and downward direction to get all records
            if start_offset < 4096:
                cluster_index_file.seek(start_offset)
            else:
                # Check preceding block to get all records
                cluster_index_file.seek(start_offset - 4096)

            done = False
            while True:
                cluster_data = cluster_index_file.read(4096)

                start = 0
                for _ in range(256):
                    entry = cluster_data[start:start+16]

                    if not entry:
                        done = True
                        break

                    day, month, year, record_pointer = unpack(
                        '4I', entry)
                    age = calculate_age(day, month, year)

                    if age >= 21:
                        done = True
                        break

                    f.seek(record_pointer)
                    data = f.read(405)
                    fname, lname, _, _, _, _, _, _, _, ssn, _, _, _ = unpack(
                        '20s20s70s40s80s25sIII12s25s50s50s', data)
                    fname = fname.replace(b'\x00', b'')
                    lname = lname.replace(b'\x00', b'')
                    ssn = ssn.replace(b'\x00', b'')
                    count += 1

                    if verbose:
                        print(Person(fname, lname, ssn, age))

                    start += 16
                    record_pointer += 16
                if done:
                    break
    print("Number of records: {}".format(count))


def main(ARGS):
    """Driver function

    Args:
        ARGS (parser object): command line to execute the program

    """
    if ARGS.primary:
        create_primary_index(ARGS.file)

    if ARGS.secondary:
        generate_secondary_index_file(ARGS.file)
        try:
            f = open('secondary_index_file.bin')
            f.close()
        except FileNotFoundError:
            print('Cannot generate secondary index file')
        create_secondary_index('secondary_index_file.bin')

    if ARGS.cluster:
        try:
            f = open('secondary_index_file.bin')
            f.close()
        except FileNotFoundError:
            print('Run -s or --secondary before executing this command')
            exit(0)
        generate_cluster_index_file('secondary_index_file.bin')
        create_cluster_index('cluster_index_file.bin')

    if ARGS.query1:
        table_scan(ARGS.file, ARGS.verbose)

    if ARGS.query2:
        uniqueness_check(ARGS.file, ARGS.verbose)

    if ARGS.query3:
        table_scan_on_secondary_index(ARGS.file, ARGS.verbose)

    if ARGS.query4:
        table_scan_on_cluster_index(ARGS.file, ARGS.verbose)

    print('Done!')
    exit(1)


if __name__ == '__main__':
    PARSER = argparse.ArgumentParser(
        description='Fall 2019 - Advanced Database Project 3')
    PARSER.add_argument('file', help='binary file (small.bin or large.bin)')
    PARSER.add_argument('-v', '--verbose',
                        help='Show output records', action='store_true')

    GROUP = PARSER.add_mutually_exclusive_group()
    GROUP.add_argument('-p', '--primary',
                       help='Create auto increment index', action='store_true')
    GROUP.add_argument(
        '-s', '--secondary', help='Create secondary index on birthdate', action='store_true')
    GROUP.add_argument(
        '-c', '--cluster', help='Create cluster index on birthdate', action='store_true')
    GROUP.add_argument('-q1', '--query1', help='Table scan',
                       action='store_true')
    GROUP.add_argument('-q2', '--query2',
                       help='Uniqueness check', action='store_true')
    GROUP.add_argument('-q3', '--query3',
                       help='Secondary index', action='store_true')
    GROUP.add_argument('-q4', '--query4',
                       help='Cluster index', action='store_true')

    ARGS = PARSER.parse_args()
    if not any([ARGS.primary, ARGS.secondary, ARGS.query1, ARGS.query2, ARGS.query3,
                ARGS.verbose, ARGS.cluster, ARGS.query4]):
        PARSER.print_help()
        print('\nOne of the options is required')
        exit(0)
    main(ARGS)
