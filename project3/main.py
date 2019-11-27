import dbm
from struct import unpack
from struct import pack
from collections import namedtuple
from datetime import date
import os
import math


# def create_db(filename):
#     with open(filename, 'rb') as f:
#         j = 0
#         for i in range(1, 101):
#             with dbm.open('test', 'c') as db:
#                 data = f.read(405)
#                 fname, _, _, _, _, _, _, _, _, ssn, _, _, _ = unpack(
#                     '20s20s70s40s80s25sIII12s25s50s50s', data)
#                 print(fname)
#                 db[str(i)] = data
#                 j += 1
#                 if j == 10:
#                     j = 0
#                     f.read(46)

# def table_scan():
#     Person = namedtuple(
#         'Person', 'fname, lname, ssn, day, month, year')
#     with dbm.open('test', 'r') as db:
#         for record in db:
#             data = db.get(record)
#             if len(data) == 405:
#                 fname, lname, job, comp, addr, phone, day, month, year, ssn, uname, email, url = unpack(
#                     '20s20s70s40s80s25sIII12s25s50s50s', data)
#                 if calculate_age(day, month, year) < 21:
#                     fname = fname.replace(b'\x00', b'')
#                     lname = lname.replace(b'\x00', b'')
#                     job = job.replace(b'\x00', b'')
#                     comp = comp.replace(b'\x00', b'')
#                     addr = addr.replace(b'\x00', b'')
#                     phone = phone.replace(b'\x00', b'')
#                     ssn = ssn.replace(b'\x00', b'')
#                     uname = uname.replace(b'\x00', b'')
#                     email = email.replace(b'\x00', b'')
#                     url = url.replace(b'\x00', b'')
#                     print(Person(fname, lname, ssn, day, month, year))

# small.bin
# r = 100 records
# block size B = 4096 bytes
# record length R = 405 bytes
# bfr = B/r = 4096/405 = 10 
# number of blocks = r/bfr = 100/10 = 10 blocks


# large.bin
# r = 10 million
# block size B = 4096 bytes
# record length R = 405 bytes
# bfr = B/R = 4096/405 = 10
# number of blocks = r/bfr = 10 million / 10 = 1 million blocks

def create_db(file_name):
    num_blocks = 0
    if file_name == 'small.bin':
        num_blocks = math.floor(100/10)
    elif file_name == 'large.bin':
        num_blocks = math.floor(10**7/10)
    print(num_blocks)
    with open(file_name, 'rb') as f:
        for i in range(1, num_blocks):
            data = f.read(4096)
            with dbm.open('test', 'c') as db:
                db[str(i)] = data


def table_scan():
    Person = namedtuple(
        'Person', 'fname, lname, ssn, day, month, year')
    with dbm.open('test', 'r') as db:
        for record in db:
            data = db.get(record)
            start = 0
            for _ in range(10):
                fname, lname, job, comp, addr, phone, day, month, year, ssn, uname, email, url = unpack(
                    '20s20s70s40s80s25sIII12s25s50s50s', data[start:start+405])
                if calculate_age(day, month, year) < 21:
                    fname = fname.replace(b'\x00', b'')
                    lname = lname.replace(b'\x00', b'')
                    job = job.replace(b'\x00', b'')
                    comp = comp.replace(b'\x00', b'')
                    addr = addr.replace(b'\x00', b'')
                    phone = phone.replace(b'\x00', b'')
                    ssn = ssn.replace(b'\x00', b'')
                    uname = uname.replace(b'\x00', b'')
                    email = email.replace(b'\x00', b'')
                    url = url.replace(b'\x00', b'')
                    print(Person(fname, lname, ssn, day, month, year))
                start += 405


def calculate_age(day, month, year):
    # from https://stackoverflow.com/questions/2217488/age-from-birthdate-in-python
    today = date.today()
    return today.year - year - ((today.month, today.day) < (month, day))


def uniqueness_check():
    res = []
    seen = set()
    with dbm.open('test', 'r') as db:
        for record in db:
            data = db.get(record)
            if len(data) == 405:
                _, _, _, _, _, _, _, _, _, ssn, _, _, _ = unpack(
                    '20s20s70s40s80s25sIII12s25s50s50s', data)
                ssn = ssn.replace(b'\x00', b'')
                if ssn not in seen:
                    seen.add(ssn)
                else:
                    res.append(ssn)
    return res


create_db('small.bin')
table_scan()
# print(get_len('large.bin'))
# print(uniqueness_check())