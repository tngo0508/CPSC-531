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
import math
import tempfile

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


def create_secondary_index(data_file):
    num_blocks = get_num_blocks(data_file)
    with dbm.ndbm.open('secondary_index', 'n') as secondary_index:
        with open(data_file, 'rb') as f:
            record_pointer = 0
            for _ in range(num_blocks):
                record_pointer = f.tell()
                data = f.read(4096)

                start = 0
                for _ in range(10):
                    _, _, _, _, _, _, day, month, year, _, _, _, _ = unpack(
                        '20s20s70s40s80s25s3I12s25s50s50s', data[start:start+405])

                    birthdate = pack('3I', day, month, year)

                    if birthdate not in secondary_index:
                        secondary_index[birthdate] = pack('I', record_pointer)
                    else:
                        new_record_pointer = secondary_index[birthdate] + pack(
                            'I', record_pointer)
                        secondary_index[birthdate] = new_record_pointer
                    start += 405
                    record_pointer += 405


def table_scan_on_secondary_index(data_file, verbose):
    Person = namedtuple(
        'Person', 'fname, lname, ssn, age')
    count = 0
    with dbm.ndbm.open('secondary_index', 'r') as secondary_index:
        with open(data_file, 'rb') as f:
            for birthdate in secondary_index.keys():
                day, month, year = unpack('3I', birthdate)
                age = calculate_age(day, month, year)
                if age < 21:
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
    print("Number of records: {}".format(count))


def external_sort(data_file):
    i = 1
    j = get_num_blocks(data_file)
    # k = 262144
    k = 131072
    m = math.ceil(j / k)
    sorting_file_num = 1

    # sorting phase
    with open(data_file, 'rb') as f:
        while i <= m:
            lst = []
            for _ in range(k):
                data = f.read(4096)

                if not data:
                    break

                start = 0
                for _ in range(10):
                    # fname, lname, job, comp, addr, phone, day, month, year, ssn, uname, email, url = unpack(
                    #     '20s20s70s40s80s25s3I12s25s50s50s', data[start:start+405])
                    lst.append(
                        unpack('20s20s70s40s80s25s3I12s25s50s50s', data[start:start+405]))
                    start += 405

            lst.sort(key=lambda x:
                     datetime.strptime(str(x[6]) + '-' + str(x[7]) + '-' + str(x[8]),
                                       '%d-%m-%Y'),
                     reverse=True)

            file_name = 'file' + str(sorting_file_num)
            with open(file_name, 'wb') as file:
                count = 0
                for x in lst:
                    fname, lname, job, comp, addr, phone, day, month, year, ssn, uname, email, url = x
                    file.write(pack('20s20s70s40s80s25s3I12s25s50s50s',
                                    fname, lname, job, comp, addr, phone, day, month, year, ssn, uname, email, url))

                    count += 1

                    if count == 10:
                        file.write(pack('x')*46)
                        count = 0

            sorting_file_num += 1
            i += 1

    # merging phase
    i = 1
    p = math.ceil(math.log(m, k - 1))
    j = m

    if i > p:
        os.rename('file1', 'sorted_' + str(data_file))

    while i <= p:
        n = 1
        q = math.ceil(j/(k-1))

        while n <= q:
            num_blocks = get_num_blocks('file1')
            f_ptr = 0
            for _ in range(num_blocks):
                lst = []
                for file_num in range(1, m+1):
                    file_name = 'file' + str(file_num)
                    with open(file_name, 'rb') as f:
                        f.seek(f_ptr)
                        data = f.read(4096)
                        start = 0
                        for _ in range(10):
                            lst.append(
                                unpack('20s20s70s40s80s25s3I12s25s50s50s', data[start:start+405]))
                            start += 405
                f_ptr += 4096

                lst.sort(key=lambda x:
                         datetime.strptime(str(x[6]) + '-' + str(x[7]) + '-' + str(x[8]),
                                           '%d-%m-%Y'),
                         reverse=True)

                output_file = 'sorted_' + data_file
                with open(output_file, 'ab') as file:
                    count = 0
                    for x in lst:
                        fname, lname, job, comp, addr, phone, day, month, year, ssn, uname, email, url = x
                        file.write(pack('20s20s70s40s80s25s3I12s25s50s50s',
                                        fname, lname, job, comp, addr, phone, day, month, year, ssn, uname, email, url))

                        count += 1

                        if count == 10:
                            file.write(pack('x')*46)
                            count = 0
            n += 1
        j = q
        i += 1


def create_cluster_index(sorted_data_file):
    num_blocks = get_num_blocks(sorted_data_file)
    with dbm.ndbm.open('cluster_index', 'n') as cluster_index:
        with open(sorted_data_file, 'rb') as f:
            for _ in range(num_blocks):
                record_pointer = f.tell()
                data = f.read(4096)

                start = 0
                for _ in range(10):
                    _, _, _, _, _, _, day, month, year, _, _, _, _ = unpack(
                        '20s20s70s40s80s25s3I12s25s50s50s', data[start:start+405])

                    birthdate = pack('3I', day, month, year)

                    if str(birthdate) not in cluster_index:
                        cluster_index[birthdate] = pack('I', record_pointer)
                    start += 405
                    record_pointer += 405


def table_scan_on_cluster_index(sorted_data_file, verbose):
    Person = namedtuple(
        'Person', 'fname, lname, ssn, age')
    count = 0
    birthdates = []
    start_offset = 0
    with dbm.ndbm.open('cluster_index', 'r') as cluster_index:
        for birthdate_in_bytes in cluster_index.keys():
            day, month, year = unpack('3I', birthdate_in_bytes)
            age = calculate_age(day, month, year)
            if age < 21:
                birthdates.append(birthdate_in_bytes)

        sorted_birthdates = []
        for birthdate_in_bytes in birthdates:
            day, month, year = unpack(
                '3I', birthdate_in_bytes
            )
            birthdate = '{}-{}-{}'.format(day, month, year)
            birthdate_in_bytes = pack('3I', day, month, year)
            sorted_birthdates.append((datetime.strptime(birthdate, '%d-%m-%Y').date(),
                                        birthdate_in_bytes))

        sorted_birthdates.sort(key=lambda x: x[0], reverse=True)
        start_offset = unpack('I', cluster_index[sorted_birthdates[0][1]])[0]

    with open(sorted_data_file, 'rb') as f:
        f.seek(start_offset)
        while True:
            data = f.peek(46)

            if data == pack('x')*46:
                f.read(46)
                continue

            data = f.read(405)

            fname, lname, _, _, _, _, day, month, year, ssn, _, _, _ = unpack(
                '20s20s70s40s80s25s3I12s25s50s50s', data)
            fname = fname.replace(b'\x00', b'')
            lname = lname.replace(b'\x00', b'')
            ssn = ssn.replace(b'\x00', b'')
            age = calculate_age(day, month, year)

            if age >= 21:
                break
            
            count += 1
            if verbose:
                print(Person(fname, lname, ssn, age))

    print("Number of records: {}".format(count))


def test(data_file):
    num_blocks = get_num_blocks(data_file)
    Person = namedtuple(
        'Person', 'fname, lname, ssn, year')
    count = 0
    with open(data_file, 'rb') as f:
        for _ in range(num_blocks):
            data = f.read(4096)
            start = 0
            for _ in range(10):
                fname, lname, job, comp, addr, phone, day, month, year, ssn, uname, email, url = unpack(
                    '20s20s70s40s80s25s3I12s25s50s50s', data[start:start+405])
                fname = fname.replace(b'\x00', b'')
                lname = lname.replace(b'\x00', b'')
                ssn = ssn.replace(b'\x00', b'')
                # age = calculate_age(day, month, year)
                print(Person(fname, lname, ssn, year))
                start += 405
                count += 1
    print(count)


def main(ARGS):
    """Driver function

    Args:
        ARGS (parser object): command line to execute the program

    """
    if ARGS.primary:
        create_primary_index(ARGS.file)

    if ARGS.secondary:
        create_secondary_index(ARGS.file)

    if ARGS.cluster:
        # external_sort(ARGS.file)
        # create_cluster_index('sorted_large.bin')
        create_cluster_index('sorted_small.bin')
        # test('sorted_large.bin')
        # test('file1')
        # print('------------')
        # test('file2')

    if ARGS.query1:
        table_scan(ARGS.file, ARGS.verbose)

    if ARGS.query2:
        uniqueness_check(ARGS.file, ARGS.verbose)

    if ARGS.query3:
        table_scan_on_secondary_index(ARGS.file, ARGS.verbose)

    if ARGS.query4:
        table_scan_on_cluster_index('sorted_small.bin', ARGS.verbose)

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
