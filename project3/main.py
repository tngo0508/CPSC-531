#!/usr/bin/env python3
from struct import unpack, pack, calcsize
from collections import namedtuple
from datetime import date
import dbm.ndbm
import sys
import argparse


def create_primary_index(file_name):
    num_blocks = 0
    if file_name == 'small.bin':
        num_blocks = 10
    elif file_name == 'large.bin':
        num_blocks = 1048576
    else:
        raise ValueError('file_name has to be either small.bin or larger.bin')
    with dbm.ndbm.open('index', 'n') as db:
        offset = 0
        for i in range(1, num_blocks + 1):
            db[pack('I', i)] = pack('I', offset)
            offset += 4096

# query 1
def table_scan(file_name):
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
                    fname, lname, job, comp, addr, phone, day, month, year, ssn, uname, email, url = unpack(
                    '20s20s70s40s80s25s3I12s25s50s50s', data[start:start+405])
                    age = calculate_age(day, month, year)
                    if  age < 21:
                        fname = fname.replace(b'\x00', b'')
                        lname = lname.replace(b'\x00', b'')
                        ssn = ssn.replace(b'\x00', b'')
                        print(Person(fname, lname, ssn, age))
                    start += 405


def calculate_age(day, month, year):
    # from https://stackoverflow.com/questions/2217488/age-from-birthdate-in-python
    today = date.today()
    return today.year - year - ((today.month, today.day) < (month, day))


# query 2
def uniqueness_check(file_name):
    seen = set()
    with dbm.ndbm.open('index', 'r') as db:
        with open(file_name, 'rb') as f:
            for record in db.keys():
                offset = unpack('I', db[record])[0]
                f.seek(offset)
                data = f.read(4096)

                start = 0
                for _ in range(10):
                    fname, _, _, _, _, _, _, _, _, ssn, _, _, _ = unpack(
                        '20s20s70s40s80s25sIII12s25s50s50s', data[start:start+405])
                    ssn = ssn.replace(b'\x00', b'')
                    if ssn not in seen:
                        seen.add(ssn)
                    else:
                        print(ssn)
                    start += 405


def create_secondary_index(file_name):
    num_blocks = 0
    if file_name == 'small.bin':
        num_blocks = 10
    elif file_name == 'large.bin':
        num_blocks = 1048576
    else:
        raise ValueError('file_name has to be either small.bin or larger.bin')
    with dbm.ndbm.open('secondaryIndex', 'n') as secondaryIndex:
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

                    if birthdate not in secondaryIndex:
                        secondaryIndex[birthdate] = pack('I', record_pointer)
                    else:
                        new_record_pointer = secondaryIndex[birthdate] + pack('I', record_pointer)
                        secondaryIndex[birthdate] = new_record_pointer
                    start += 405
                    record_pointer += 405


def table_scan_on_secondary_index(file_name):
    Person = namedtuple(
        'Person', 'fname, lname, ssn, age')
    with dbm.ndbm.open('secondaryIndex', 'r') as secondaryIndex:
        with open(file_name, 'rb') as f:
            for birthdate in secondaryIndex.keys():
                day, month, year = unpack('3h', birthdate)
                age = calculate_age(day, month, year)
                if age < 21:
                    num_record_pointers = len(secondaryIndex[birthdate]) // 4
                    format_char = str(num_record_pointers) + 'I'
                    record_pointers = unpack(format_char, secondaryIndex[birthdate])
                    for record_pointer in record_pointers:
                        f.seek(record_pointer)
                        data = f.read(405)
                        fname, lname, _, _, _, _, _, _, _, ssn, _, _, _ = unpack(
                        '20s20s70s40s80s25sIII12s25s50s50s', data)
                        fname = fname.replace(b'\x00', b'')
                        lname = lname.replace(b'\x00', b'')
                        ssn = ssn.replace(b'\x00', b'')
                    print(Person(fname, lname, ssn, age))


def main(args):
    if args.primary:
        create_primary_index(args.file)
    if args.secondary:
        create_secondary_index(args.file)
    if args.query1:
        table_scan(args.file)
    if args.query2:
        uniqueness_check(args.file)
    if args.query3:
        table_scan_on_secondary_index(args.file)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Fall 2019 - Advanced Database Project 3')
    parser.add_argument('file', help='binary file (small.bin or large.bin)')

    group = parser.add_mutually_exclusive_group()
    group.add_argument('-p', '--primary', help='Create auto increment index', action='store_true')
    group.add_argument('-s', '--secondary', help='Create secondary index on birthdate', action='store_true')
    group.add_argument('-q1', '--query1', help='Table scan', action='store_true')
    group.add_argument('-q2', '--query2', help='Uniqueness check', action='store_true')
    group.add_argument('-q3', '--query3', help='Secondary index', action='store_true')

    args = parser.parse_args()
    if not any([args.primary, args.secondary, args.query1, args.query2, args.query3]):
        parser.print_help()
        print('\nOne of the options is required')
        sys.exit(0)
    main(args)
