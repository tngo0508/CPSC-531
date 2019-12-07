from struct import unpack, pack, calcsize
from struct import pack
from collections import namedtuple
from datetime import *
import dbm.ndbm
import sys
import shelve 


def calculate_age(day, month, year):
    # from https://stackoverflow.com/questions/2217488/age-from-birthdate-in-python
    today = date.today()
    return today.year - year - ((today.month, today.day) < (month, day))


def cluster_index():
 unsort_list= []
 with dbm.ndbm.open('secondarydb', 'r') as secondaryIndex:
  for k in secondaryIndex.keys():
   day, month, year = unpack('hhh', k)
   birthday = str(day) + '/' +str(month) + '/' +str(year) 
   unsort_list.append((datetime.strptime(birthday,'%d/%m/%Y'),k,secondaryIndex.get(k) ))
 sorted_list = sorted(unsort_list, key = lambda x:x[0])
 with dbm.ndbm.open('clutersIndex', 'c') as clusterindex:
  for x in sorted_list:
   clusterindex[x[1]] = x[2]


def cluster_shelve():
 unsort_list= []
 with shelve.open('secondarydb', 'r') as secondaryIndex:
  for k in secondaryIndex.keys():
   unsort_list.append((datetime.strptime(k,'%d/%m/%Y'),k,secondaryIndex.get(k) ))
 sorted_list = sorted(unsort_list, key = lambda x:x[0])
 with shelve.open('clutersIndex', 'n') as clusterindex:
  for x in sorted_list:
   clusterindex[x[1]] = x[2]

def w_newbin():

 with open('sortedlarge.bin','wb') as newbin:
  with open('large.bin','rb') as f:
   with dbm.ndbm.open('clutersIndex', 'r') as clusterindex:
    for birthdate in clusterindex.keys():
     num_record_pointers = len(clusterindex[birthdate]) // 4
     format_char = str(num_record_pointers) + 'I'
     record_pointers = unpack(format_char, clusterindex[birthdate])
     for record_pointer in record_pointers:
      f.seek(record_pointer)
      newbin.write(f.read(405))




def table_scan_on_secondary_index(file_name):
    Person = namedtuple(
        'Person', 'fname, lname, ssn, age')
    counter = 0 
    with dbm.ndbm.open('clutersIndex', 'r') as secondaryIndex:
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
                        counter+=1
              
                    #print(Person(fname, lname, ssn, age))
    print(counter)

def query_secondaryindex():
 secondary_list = []
 with shelve.open('clutersIndex', 'r') as indexdb:
  for k in indexdb.keys():
   age = k.split('/')
   if calculate_age(int(age[0]),int(age[1]),int(age[2])) < 21:
    locations = indexdb.get(k)
    for loca in locations:
     secondary_list.append(loca)

 print(len(secondary_list))
#cluster_index()
#table_scan_on_secondary_index('large.bin')
#cluster_shelve()
query_secondaryindex()

#w_newbin()
