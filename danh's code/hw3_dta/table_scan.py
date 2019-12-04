import struct
import dbm
from datetime import date
 #first_name VARCHAR(20) NOT NULL,
  #  last_name VARCHAR(20) NOT NULL,
  #  job VARCHAR(70) NOT NULL,
  #  company VARCHAR(40) NOT NULL,
   # address VARCHAR(80) NOT NULL,
  #  phone VARCHAR(25) NOT NULL,
  #  birthdate DATE NOT NULL,
   # ssn VARCHAR(12) NOT NULL,
   # username VARCHAR(25) NOT NULL,
   # email VARCHAR(50) NOT NULL,
   # url VARCHAR(50) NOT NULL
from collections import namedtuple


emp = namedtuple('emp', 'first_name last_name job company address phone dd mm yyyy ssn username email url')

a= []
secondary_index = []
def removeNULL(person):
  result = namedtuple('User', 'first_name last_name ssn age')
  c = result(
  person.first_name.replace(b'\0', b''),
  person.last_name.replace(b'\0', b''),
  person.ssn.replace(b'\0', b''),
  calculate_age(person.dd, person.mm, person.yyyy))
  return c

def calculate_age(day, month, year):
    # from https://stackoverflow.com/questions/2217488/age-from-birthdate-in-python
    today = date.today()
    return today.year - year - ((today.month, today.day) < (month, day))


def table_scan():import struct
import dbm
from datetime import date
 #first_name VARCHAR(20) NOT NULL,
  #  last_name VARCHAR(20) NOT NULL,
  #  job VARCHAR(70) NOT NULL,
  #  company VARCHAR(40) NOT NULL,
   # address VARCHAR(80) NOT NULL,
  #  phone VARCHAR(25) NOT NULL,
  #  birthdate DATE NOT NULL,
   # ssn VARCHAR(12) NOT NULL,
   # username VARCHAR(25) NOT NULL,
   # email VARCHAR(50) NOT NULL,
   # url VARCHAR(50) NOT NULL
from collections import namedtuple


emp = namedtuple('emp', 'first_name last_name job company address phone dd mm yyyy ssn username email url')

listresult = []

def removeNULL(person):
  result = namedtuple('User', 'first_name last_name ssn age')
  c =result(
  person.first_name.replace(b'\0', b''),
  person.last_name.replace(b'\0', b''),
  person.ssn.replace(b'\0', b''),
  calculate_age(person.dd, person.mm, person.yyyy)) 
  listresult.append(c)
  



def calculate_age(day, month, year):
    # from https://stackoverflow.com/questions/2217488/age-from-birthdate-in-python
    today = date.today()
    return today.year - year - ((today.month, today.day) < (month, day))


def table_scan():
 with dbm.open('db', 'r') as db:
  k = db.firstkey()
  with open('large.bin','rb') as f:
   while k != None:
    start = 0 
    end = 405
    blocksize = struct.unpack('I',db.get(k))
    datafiles= f.read(blocksize[0])
    for _int in range(10):
     a = emp._make(struct.unpack('20s20s70s40s80s25sIII12s25s50s50s', datafiles[start:end]))
     if calculate_age(a.dd, a.mm, a.yyyy) < 21:
      removeNULL(a)
     start+= 405
     end = start+ 405
    k = db.nextkey(k)



table_scan()
print(len(listresult))


 