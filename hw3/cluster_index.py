
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
 datas_list= []
 with shelve.open('clusterIndex', 'c') as cluster:
  with open('large.bin','rb') as f:
   blockpointer = 0 
   for _int in range(1048576):
    blockpointer = f.tell()
    start = 0 
    end = 405
    datafiles= f.read(4096)
    for _int in range(10):
     blockpointer_list= []
     a = emp._make(struct.unpack('20s20s70s40s80s25sIII12s25s50s50s', datafiles[start:end]))
     birthday = str(a.dd)+'/'+ str(a.mm) +'/'+ str(a.yyyy)
     for x in datas_list:
      if birthday in x[1]:
       x[2].append(blockpointer)
     blockpointer_list.append(blockpointer)
     datas_list.append((calculate_age(a.dd,a.mm,a.yyyy),birthday,blockpointer_list))
     start+= 405
     end = start+ 405
     blockpointer +=405
  datas_list.sort(key = lambda x:x[0])
  for x in datas_list:
   cluster[x[1]] = x[2]


def query_secondaryindex():
 secondary_list = []
 with shelve.open('clusterIndex', 'r') as cluster:
  for k in cluster.keys():
   age = k.split('/')
   if calculate_age(int(age[0]),int(age[1]),int(age[2])) < 21:
    locations = cluster.get(k)
    for loca in locations:
     secondary_list.append(loca)

 print(len(secondary_list))
 with open('large.bin','rb') as f :
  for location in secondary_list:
   f.seek(int(location))
   datas = f.read(405)
   a = emp._make(struct.unpack('20s20s70s40s80s25sIII12s25s50s50s', datas))
   
   #print(removeNULL(a))
   


makeIndex()

#query_secondaryindex()

