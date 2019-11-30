from struct import unpack
from struct import pack
from collections import namedtuple
from datetime import date
import dbm.dumb
import sys


def create_primary_index(file_name):
    num_blocks = 0
    if file_name == 'small.bin':
        num_blocks = 10
    elif file_name == 'large.bin':
        num_blocks = 1048576
    else:
        raise ValueError('file_name has to be either small.bin or larger.bin')
    with dbm.dumb.open('index', 'n') as db:
        offset = 0
        for i in range(1, num_blocks + 1):
            db[str(i)] = str(offset)
            offset += 4096

# query 1
def table_scan(file_name):
    Person = namedtuple(
        'Person', 'fname, lname, ssn, age')
    with dbm.dumb.open('index', 'r') as db:
        with open(file_name, 'rb') as f:
            for record in db:
                offset = int(db.get(record))
                f.seek(offset)
                data = f.read(4096)

                start = 0
                for _ in range(10):
                    fname, lname, job, comp, addr, phone, day, month, year, ssn, uname, email, url = unpack(
                    '20s20s70s40s80s25sIII12s25s50s50s', data[start:start+405])
                    age = calculate_age(day, month, year)
                    if  age < 21:
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
                        # print(Person(fname, lname, ssn, age))
                    start += 405


def calculate_age(day, month, year):
    # from https://stackoverflow.com/questions/2217488/age-from-birthdate-in-python
    today = date.today()
    return today.year - year - ((today.month, today.day) < (month, day))


# query 2
def uniqueness_check(file_name):
    res = set()
    seen = set()
    with dbm.dumb.open('index', 'r') as db:
        with open(file_name, 'rb') as f:
            for record in db:
                offset = int(db.get(record))
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
                        res.add(ssn)
                    start += 405
    return res if res else None


def create_secondary_index(file_name):
    num_blocks = 0
    if file_name == 'small.bin':
        num_blocks = 10
    elif file_name == 'large.bin':
        num_blocks = 1048576
    else:
        raise ValueError('file_name has to be either small.bin or larger.bin')
    with dbm.dumb.open('secondaryIndex', 'n') as secondaryIndex:
        with open(file_name, 'rb') as f:
            record_pointer = 0
            # block_anchor = 0
            for _ in range(num_blocks):
                record_pointer = f.tell()
                data = f.read(4096)

                start = 0
                # record_pointer = block_anchor
                for _ in range(10):
                    fname, lname, job, comp, addr, phone, day, month, year, ssn, uname, email, url = unpack(
                    '20s20s70s40s80s25sIII12s25s50s50s', data[start:start+405])
                    age = calculate_age(day, month, year)
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

                    birthdate = pack('III', day, month, year)

                    if birthdate not in secondaryIndex:
                        secondaryIndex[birthdate] = str(record_pointer)
                    else:
                        new_record_pointer = str(secondaryIndex[birthdate].decode('utf-8')) + ',' + str(record_pointer)
                        secondaryIndex[birthdate] = new_record_pointer
                        # print(new_record_pointer)
                    start += 405
                    record_pointer += 405
                # block_anchor += 4096


def table_scan_on_secondary_index(file_name):
    Person = namedtuple(
        'Person', 'fname, lname, ssn, age')
    with dbm.dumb.open('secondaryIndex', 'r') as secondaryIndex:
        with open(file_name, 'rb') as f:
            for birthdate in secondaryIndex:
                day, month, year = unpack('III', birthdate)
                age = calculate_age(day, month, year)
                if age < 21:
                    record_pointers = secondaryIndex[birthdate].decode('utf-8').split(',')
                    # print(record_pointers)
                    for record_pointer in record_pointers:
                        f.seek(int(record_pointer))
                        data = f.read(405)
                        fname, lname, job, comp, addr, phone, day, month, year, ssn, uname, email, url = unpack(
                        '20s20s70s40s80s25sIII12s25s50s50s', data)
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
                        print(Person(fname, lname, ssn, age))


def test(file_name):
    with open(file_name, 'rb') as f:
        f.seek(704917)
        data = f.read(405)
        fname, lname, job, comp, addr, phone, day, month, year, ssn, uname, email, url = unpack(
        '20s20s70s40s80s25sIII12s25s50s50s', data)
        print(fname)
        print(day, month, year)
        f.seek(3137536)
        data = f.read(405)
        fname, lname, job, comp, addr, phone, day, month, year, ssn, uname, email, url = unpack(
        '20s20s70s40s80s25sIII12s25s50s50s', data)
        print(fname)
        print(day, month, year)


if __name__ == '__main__':
    file_name = sys.argv[1]
    # create_primary_index(file_name)
    create_secondary_index(file_name)
    # table_scan(file_name)
    # table_scan_on_secondary_index(file_name)
    # print(uniqueness_check(file_name))
    # test(file_name)
