import struct
import shelve
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
  result = namedtuple('User', 'first_name last_name ssn')
  c = result(
  person.first_name.replace(b'\0', b''),
  person.last_name.replace(b'\0', b''),
  person.ssn.replace(b'\0', b''))
  return c

def calculate_age(day, month, year):
    # from https://stackoverflow.com/questions/2217488/age-from-birthdate-in-python
    today = date.today()
    return today.year - year - ((today.month, today.day) < (month, day))



def makeIndex():
 with shelve.open('primaryIndex', 'c') as primary:
  with open('large.bin','rb') as f:
   blockpointer = 0 
   for i in range(1048576):
    blockpointer = f.tell()
    datafiles= f.read(4096)
    records_perblock = []
    for _int in range(10):
     records_perblock.append(blockpointer)
     blockpointer +=405
    primary[str(i)] = records_perblock
      
def query1():
 counter = 0 
 with shelve.open('primaryIndex', 'r') as primary:
  with open('large.bin','rb') as f:
   for k in primary.keys():
    records_pionter = primary.get(k)
    for record in records_pionter:
     f.seek(int(record))
     datas = f.read(405)
     a = emp._make(struct.unpack('20s20s70s40s80s25sIII12s25s50s50s', datas))
     if calculate_age(a.dd,a.mm,a.yyyy) < 21:
      counter +=1
      #print(removeNULL(a))
 print(counter)
   
def unique_check():
 unique = []
 check = []
 with shelve.open('primaryIndex', 'r') as primary:
  with open('large.bin','rb') as f:
   for k in primary.keys():
    records_pionter = primary.get(k)
    for record in records_pionter:
     f.seek(int(record))
     datas = f.read(405)
     a = emp._make(struct.unpack('20s20s70s40s80s25sIII12s25s50s50s', datas))
     if a.ssn not in check:
      check.append(a.ssn)
     else:
      unique.append(a.ssn)
 return unique


#makeIndex()
query1()
print(unique_check())
