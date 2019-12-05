from struct import unpack, pack, calcsize
# from struct import pack
from collections import namedtuple
from datetime import date
import dbm.ndbm
import sys


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
    count = 0
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
                        # print(Person(fname, lname, ssn, age))
                        count += 1
                    start += 405
    print(count)


def calculate_age(day, month, year):
    # from https://stackoverflow.com/questions/2217488/age-from-birthdate-in-python
    today = date.today()
    return today.year - year - ((today.month, today.day) < (month, day))


# query 2
def uniqueness_check(file_name):
    res = set()
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
                        # print(ssn)
                        res.add(ssn)
                    start += 405
    return len(res) if res else None


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
            # block_anchor = 0
            for _ in range(num_blocks):
                record_pointer = f.tell()
                # record_pointer = block_anchor
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
                        # print([unpack('I', x)[0] for x in secondaryIndex[birthdate].split(unpack('x', ))])
                        secondaryIndex[birthdate] = new_record_pointer
                    start += 405
                    record_pointer += 405
                # block_anchor += 4096


def table_scan_on_secondary_index(file_name):
    Person = namedtuple(
        'Person', 'fname, lname, ssn, age')
    count = 0
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
                        count += 1
                    # print(Person(fname, lname, ssn, age))
    print(count)


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
    # create_secondary_index(file_name)
    # table_scan(file_name)
    # table_scan_on_secondary_index(file_name)
    # print(uniqueness_check(file_name))
    # test(file_name)
