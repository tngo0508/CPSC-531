#!/usr/bin/env python3
from struct import unpack, pack, calcsize
from collections import namedtuple
from datetime import date
from datetime import datetime
import dbm.ndbm
import sys
import os
import argparse


def create_primary_index(file_name):
    file_size = os.stat(file_name).st_size
    num_blocks = file_size // 4096
    with dbm.ndbm.open('index', 'n') as db:
        offset = 0
        for i in range(1, num_blocks + 1):
            db[pack('I', i)] = pack('I', offset)
            offset += 4096

# query 1


def table_scan(file_name, verbose):
    count = 0
    Person = namedtuple(
        'Person', 'fname, lname, ssn, age')
    with dbm.ndbm.open('index', 'r') as db:
        with open(file_name, 'rb') as f:
            for record in db.keys():
                offset = unpack('I', db[record])[0]
                f.seek(offset)
                data = f.read(4096)

                start = 0
                for _ in range(10):
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
    print("Number of records: {}".format(count))


def calculate_age(day, month, year):
    # from https://stackoverflow.com/questions/2217488/age-from-birthdate-in-python
    today = date.today()
    return today.year - year - ((today.month, today.day) < (month, day))


# query 2
def uniqueness_check(file_name, verbose):
    count = 0
    seen = set()
    with dbm.ndbm.open('index', 'r') as db:
        with open(file_name, 'rb') as f:
            for record in db.keys():
                offset = unpack('I', db[record])[0]
                f.seek(offset)
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


def create_secondary_index(file_name):
    file_size = os.stat(file_name).st_size
    num_blocks = file_size // 4096
    with dbm.ndbm.open('secondary_index', 'n') as secondary_index:
        with open(file_name, 'rb') as f:
            record_pointer = 0
            for _ in range(num_blocks):
                record_pointer = f.tell()
                data = f.read(4096)

                start = 0
                for _ in range(10):
                    _, _, _, _, _, _, day, month, year, _, _, _, _ = unpack(
                        '20s20s70s40s80s25s3I12s25s50s50s', data[start:start+405])

                    birthdate = pack('3h', day, month, year)

                    if birthdate not in secondary_index:
                        secondary_index[birthdate] = pack('I', record_pointer)
                    else:
                        new_record_pointer = secondary_index[birthdate] + pack(
                            'I', record_pointer)
                        secondary_index[birthdate] = new_record_pointer
                    start += 405
                    record_pointer += 405


def table_scan_on_secondary_index(file_name, verbose):
    count = 0
    Person = namedtuple(
        'Person', 'fname, lname, ssn, age')
    with dbm.ndbm.open('secondary_index', 'r') as secondary_index:
        with open(file_name, 'rb') as f:
            for birthdate in secondary_index.keys():
                day, month, year = unpack('3h', birthdate)
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


def generate_secondary_index_file(file_name):
    file_size = os.stat(file_name).st_size
    num_blocks = file_size // 4096
    num_blocks = 1 if num_blocks == 0 else num_blocks
    with open('secondary_index_file.bin', 'wb') as secondary_index_file:
        with open(file_name, 'rb') as f:
            record_pointer = 0
            for _ in range(num_blocks):
                record_pointer = f.tell()
                data = f.read(4096)

                start = 0
                for _ in range(10):
                    entry = data[start:start+405]

                    if not entry:
                        break
                    _, _, _, _, _, _, day, month, year, _, _, _, _ = unpack(
                        '20s20s70s40s80s25s3I12s25s50s50s', entry)

                    index_entry = pack('4I', day, month, year, record_pointer)
                    secondary_index_file.write(index_entry)
                    start += 405
                    record_pointer += 405


def create_secondary_index2(file_name):
    file_size = os.stat(file_name).st_size
    num_blocks = file_size // 4096
    num_blocks = 1 if num_blocks == 0 else num_blocks
    with dbm.ndbm.open('secondary_index', 'n') as secondary_index:
        with open(file_name, 'rb') as f:
            record_pointer = 0
            for _ in range(num_blocks):
                record_pointer = f.tell()
                data = f.read(4096)

                start = 0
                for _ in range(256):
                    entry = data[start:start+16]

                    if not entry:
                        break

                    day, month, year, record_pointer = unpack(
                        '4I', entry)

                    birthdate = pack('3I', day, month, year)
                    if birthdate not in secondary_index:
                        secondary_index[birthdate] = pack('I', record_pointer)
                    else:
                        new_record_pointer = secondary_index[birthdate] + pack(
                            'I', record_pointer)
                        secondary_index[birthdate] = new_record_pointer
                    start += 16
                    record_pointer += 16


def table_scan_on_secondary_index2(file_name, verbose):
    count = 0
    Person = namedtuple(
        'Person', 'fname, lname, ssn, age')
    with dbm.ndbm.open('secondary_index', 'r') as secondary_index:
        with open(file_name, 'rb') as f:
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


def generate_cluster_index_file(file_name):
    file_size = os.stat(file_name).st_size
    num_blocks = file_size // 4096
    num_blocks = 1 if num_blocks == 0 else num_blocks
    entries = []
    
    with open(file_name, 'rb') as f:
        record_pointer = 0
        for _ in range(num_blocks):
            record_pointer = f.tell()
            data = f.read(4096)

            start = 0
            for _ in range(256):
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
    with open('cluster_index_file.bin', 'wb') as secondary_index_file:
        for entry in entries:
            secondary_index_file.write(entry[1]+entry[2])


def create_cluster_index(file_name):
    file_size = os.stat(file_name).st_size
    num_blocks = file_size // 4096
    num_blocks = 1 if num_blocks == 0 else num_blocks

    with dbm.ndbm.open('cluster_index', 'n') as cluster_index:
        with open(file_name, 'rb') as f:
            offset = 0
            for _ in range(num_blocks):
                data = f.read(4096)
                cluster_index[data[:12]] = pack('I', offset)
                offset += 4096


def table_scan_on_cluster_index(file_name, verbose):
    file_size = os.stat(file_name).st_size
    num_blocks = file_size // 4096
    num_blocks = 1 if num_blocks == 0 else num_blocks

    pass


def main(ARGS):
    if ARGS.primary:
        create_primary_index(ARGS.file)
    if ARGS.secondary:
        generate_secondary_index_file(ARGS.file)
        create_secondary_index2('secondary_index_file.bin')
    if ARGS.cluster:
        generate_cluster_index_file('secondary_index_file.bin')
        create_cluster_index('cluster_index_file.bin')
    if ARGS.query1:
        table_scan(ARGS.file, ARGS.verbose)
    if ARGS.query2:
        uniqueness_check(ARGS.file, ARGS.verbose)
    if ARGS.query3:
        table_scan_on_secondary_index2(ARGS.file, ARGS.verbose)
    if ARGS.query4:
        table_scan_on_cluster_index(ARGS.file, ARGS.verbose)


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
        sys.exit(0)
    main(ARGS)
