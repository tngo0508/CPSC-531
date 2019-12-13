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
import collections

__author__ = "Thomas Ngo, Danh Pham, Su Htet"
__email__ = "tngo0508@csu.fullerton.edu"


def get_num_blocks(data_file):
    try:
        f = open(data_file)
        f.close()
    except FileNotFoundError:
        print('Cannot open' + data_file)

    # One disk block size = 4096 bytes
    file_size = os.stat(data_file).st_size
    num_blocks = file_size // 4096
    return 1 if num_blocks == 0 else num_blocks


def table_scan(data_file, verbose):
    num_blocks = get_num_blocks(data_file)
    Person = namedtuple(
        'Person', 'fname, lname, ssn, age')
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
                age = calculate_age(day, month, year)
                start += 405
                if age < 21:
                    if verbose:
                        print(Person(fname, lname, ssn, age))
                    count += 1
    print("Number of records: {}".format(count))


def calculate_age(day, month, year):
    # see details at
    # https://stackoverflow.com/questions/2217488/age-from-birthdate-in-python
    today = date.today()
    return today.year - year - ((today.month, today.day) < (month, day))


def uniqueness_check(data_file, verbose):
    count = 0
    seen = set()
    num_blocks = get_num_blocks(data_file)
    with open(data_file, 'rb') as f:
        for _ in range(num_blocks):
            data = f.read(4096)

            start = 0

            for _ in range(10):

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
    if data_file == 'small.bin':
        os.rename('file1', 'sorted_small.bin')
    elif data_file == 'large.bin':
        queue = collections.deque()
        for i in range(1, m + 1):
            queue.append(i)
        merge_num = 1
        while len(queue) > 1:
            file_1 = 'file' + str(queue.popleft())
            file_2 = 'file' + str(queue.popleft())
            queue.append(merge_num)
            merge_file = 'merge' + str(merge_num)
            
            with open(file_1, 'rb') as file1:
                with open(file_2, 'rb') as file2:
                    with open(merge_file, 'ab') as merge1:
                        merge_lst = []
                        data1 = file1.read(405)
                        data2 = file2.read(405)
                        file1_cnt = 1
                        file2_cnt = 1
                        while data1 and data2:
                            _, _, _, _, _, _, day, month, year, _, _, _, _ = unpack(
                                '20s20s70s40s80s25s3I12s25s50s50s', data1)
                            x = datetime(year, month, day)
                            _, _, _, _, _, _, day, month, year, _, _, _, _ = unpack(
                                '20s20s70s40s80s25s3I12s25s50s50s', data2)
                            y = datetime(year, month, day)
                            if x <= y:
                                merge_lst.append(data1)
                                data1 = file1.read(405)
                                file1_cnt += 1
                            else:
                                merge_lst.append(data2)
                                data2 = file2.read(405)
                                file2_cnt += 1
                            
                            if file1_cnt == 10:
                                file1.read(46)
                                file1_cnt = 0
                            if file2_cnt == 10:
                                file2.read(46)
                                file2_cnt = 0

                            if len(merge_lst) == 10:
                                for x in merge_lst:
                                    merge1.write(x)
                                merge1.write(pack('x')*46)
                                merge_lst = []
                        if data1:
                            merge1.write(data1)
                            while data1:
                                if file1_cnt == 10:
                                    data1 = file1.read(46)
                                    file1_cnt = 0
                                else:
                                    data1 = file1.read(405)
                                    file1_cnt += 1
                                merge1.write(data1)
                        
                        if data2:
                            merge1.write(data2)
                            while data2:
                                if file2_cnt == 10:
                                    data2 = file2.read(46)
                                    file2_cnt = 0
                                else:
                                    data2 = file2.read(405)
                                    file2_cnt += 1
                                merge1.write(data2)
            os.remove(file_1)
            os.remove(file_2)
            os.rename(merge_file, 'file'+str(merge_num))
            merge_num += 1
        os.rename('file' + str(merge_num - 1), 'sorted_' + data_file)

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

            if not data:
                break

            fname, lname, _, _, _, _, day, month, year, ssn, _, _, _ = unpack(
                '20s20s70s40s80s25s3I12s25s50s50s', data)
            fname = fname.replace(b'\x00', b'')
            lname = lname.replace(b'\x00', b'')
            ssn = ssn.replace(b'\x00', b'')
            age = calculate_age(day, month, year)
            
            if age < 21:
                count += 1
                if verbose:
                    print(Person(fname, lname, ssn, age))

    print("Number of records: {}".format(count))


def main(ARGS):
    if ARGS.secondary:
        create_secondary_index(ARGS.file)

    if ARGS.cluster:
        external_sort(ARGS.file)
        sorted_file = 'sorted_' + ARGS.file
        try:
            f = open(sorted_file)
            f.close()
        except FileNotFoundError:
            print('Cannot find sorted data file')
        create_cluster_index(sorted_file)

    if ARGS.query1:
        table_scan(ARGS.file, ARGS.verbose)

    if ARGS.query2:
        uniqueness_check(ARGS.file, ARGS.verbose)

    if ARGS.query3:
        table_scan_on_secondary_index(ARGS.file, ARGS.verbose)

    if ARGS.query4:
        sorted_file = 'sorted_' + ARGS.file
        try:
            f = open(sorted_file)
            f.close()
        except FileNotFoundError:
            print('Cannot find sorted data file')
        table_scan_on_cluster_index(sorted_file, ARGS.verbose)

    print('Done!')
    exit(1)


if __name__ == '__main__':
    PARSER = argparse.ArgumentParser(
        description='Fall 2019 - Advanced Database Project 3')
    PARSER.add_argument('file', help='binary file (small.bin or large.bin)')
    PARSER.add_argument('-v', '--verbose',
                        help='Show output records', action='store_true')

    GROUP = PARSER.add_mutually_exclusive_group()
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
    if not any([ARGS.secondary, ARGS.query1, ARGS.query2, ARGS.query3,
                ARGS.verbose, ARGS.cluster, ARGS.query4]):
        PARSER.print_help()
        print('\nOne of the options is required')
        exit(0)
    main(ARGS)
