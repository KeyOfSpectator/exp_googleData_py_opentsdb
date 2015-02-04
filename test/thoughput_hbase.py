#-*-coding:utf-8 -*-  
#!/usr/bin/python  
'''
Created on Dec 31, 2014

@author: keyofspectator
'''
from _ast import Pass
"""
test throughput of database
"""

import os
import csv
import time
import MySQLdb
import threading

#for hbase thrift
from thrift import Thrift  
from thrift.transport import TSocket  
from thrift.transport import TTransport  
from thrift.protocol import TBinaryProtocol  
from hbase import Hbase  
from hbase.ttypes import ColumnDescriptor,Mutation,BatchMutation  


#INPUT_PATH = "/home/chenzhi/Documents/test/throughput/split_equally_1/"

INPUT_PATH = "/home/keyofspectator/ubuntu_exp/throughput/split_equally_1/"

#INPUT_PATH = "/home/keyofspectator/ubuntu_exp/throughput/test_data/"

# INPUT_FILE_LIST = ["0_500000.csv"]
INPUT_FILE_LIST = sorted([name for name in os.listdir(INPUT_PATH) if name.endswith('.csv')])
INSERT_TASK_EVENTS = '''insert into task_events values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'''

#
TSDB_IP = '127.0.0.1'
TSDB_PORT = '4242'

TEST_TIME_OFFSET = 3600  


def get_current_time():
    localtime = time.localtime()
    time_string = time.strftime("%Y-%m-%d %H:%M:%S", localtime)
    return time_string

#abondon
def execute(db, cur, row):
    """
    code that will be operated when trigger
    :param row:
    :return: no return
    """
    # logging insert history, but there are too many insert, so just logging fail insert.
    # logging.info('Insert ' + str(time.time()) + ' ' + str(row[0:6]))
    try:
        cur.execute(INSERT_TASK_EVENTS, row)
        db.commit()
    except Exception, e:
        # logging.info('fail to insert value' + str(row))
        # logging.info(traceback.format_exc())
        pass
    db.rollback()

# for opentsdb
def execute_put_tsdb_cpu(row , metric):
    """
    
    :param row:
    :return: no return
    """
    try:
        #date_s = int(time.time()) + int(row[0])/1000 -TEST_TIME_OFFSET   # data time
        date_s = int(time.time())                                              # current time
        user = string_fix(row[6])
        put_str = "put %s %s %s job=%s task=%s machine=%s event=%s user=%s" % (metric, date_s , row[9] , row[2] , row[3] , row[4] , row[5] , user)
        
        command_str = "echo '%s' | nc -w 30 %s %s" % (put_str , TSDB_IP , TSDB_PORT)
        #insert put to tsdb
        os.system(command_str)
        print command_str
    except Exception, e:
        #logging.info('fail to insert value' + str(row))
        #logging.info(traceback.format_exc())
        Pass
        
def execute_put_tsdb_memory(row , metric):
    """
    
    :param row:
    :return: no return
    """
    try:
        #date_s = int(time.time()) + int(row[0])/1000 -TEST_TIME_OFFSET   # data time
        date_s = int(time.time())                                              # current time
        user = string_fix(row[6])
        put_str = "put %s %s %s job=%s task=%s machine=%s event=%s user=%s" % (metric, date_s , row[10] , row[2] , row[3] , row[4] , row[5] , user)
        
        command_str = "echo '%s' | nc -w 30 %s %s" % (put_str , TSDB_IP , TSDB_PORT)
        #insert put to tsdb
        os.system(command_str)
        print command_str
    except Exception, e:
        #logging.info('fail to insert value' + str(row))
        #logging.info(traceback.format_exc())
        Pass
        
def execute_put_tsdb_disk(row , metric):
    """
    
    :param row:
    :return: no return
    """
    try:
        #date_s = int(time.time()) + int(row[0])/1000 -TEST_TIME_OFFSET   # data time
        date_s = int(time.time())                                              # current time
        user = string_fix(row[6])
        put_str = "put %s %s %s job=%s task=%s machine=%s event=%s user=%s" % (metric, date_s , row[11] , row[2] , row[3] , row[4] , row[5] , user)
        
        command_str = "echo '%s' | nc -w 30 %s %s" % (put_str , TSDB_IP , TSDB_PORT)
        #insert put to tsdb
        os.system(command_str)
        print command_str
    except Exception, e:
        #logging.info('fail to insert value' + str(row))
        #logging.info(traceback.format_exc())
        Pass

def list_2_str(list_tmp):
    """
    list to string
    
    return string
    """
    tmp = ''.join(list_tmp)
    return tmp

#abandon
def string_convert(str):
    """
    delete the last character of string
    return fixed string
    """
    list_tmp = list(str)
    del list_tmp[ len(list_tmp)-1 ]
    return list_2_str(list_tmp)

def string_fix(str):
    """
    delete the illegal characters in string , illegal characters: +,=
    """
    list_tmp = list(str)
    i = 0
    while i < len(list_tmp):
        if list_tmp[i] == '+' or list_tmp[i] =='=':
            del list_tmp[i]
            i = i-1
        i = i+1
    return list_2_str(list_tmp) 
#

"""
for base thrift
"""

class HbaseWriter:  
  
        """ 
                IP地址 
                端口 
                表名 
        """  
        def __init__(self,address,port,table='google-data'):  
                self.tableName = table  
  
                #建立与hbase的连接  
                self.transport=TTransport.TBufferedTransport(TSocket.TSocket(address,port))  
  
                self.protocol = TBinaryProtocol.TBinaryProtocol(self.transport)  
  
                self.client=Hbase.Client(self.protocol)  
                self.transport.open()  
  
                tables = self.client.getTableNames()  
  
                if self.tableName not in tables:  
                        print "not google-data tables"  
                        self.__createTable()  
  
                #self.write("hell,babay!!!")  
                #self.read()  
  
        #关闭  
        def __del__(self):  
                self.transport.close()  
  
        #建表  
        def __createTable(self):  
                col1 = ColumnDescriptor(name="time:",maxVersions=1)  
                col2 = ColumnDescriptor(name="missing:",maxVersions=1)  
                col3 = ColumnDescriptor(name="job:",maxVersions=1)  
                col4 = ColumnDescriptor(name="task:",maxVersions=1)  
                col5 = ColumnDescriptor(name="machine:",maxVersions=1)  
                col6 = ColumnDescriptor(name="event:",maxVersions=1)  
                col7 = ColumnDescriptor(name="user:",maxVersions=1)  
                col8 = ColumnDescriptor(name="scheduling:",maxVersions=1)  
                col9 = ColumnDescriptor(name="priority:",maxVersions=1)  
                col10 = ColumnDescriptor(name="cpu:",maxVersions=1)  
                col11 = ColumnDescriptor(name="memory:",maxVersions=1)  
                col12 = ColumnDescriptor(name="disk:",maxVersions=1)  
                col13 = ColumnDescriptor(name="different:",maxVersions=1)  
                self.client.createTable(self.tableName,[col1,col2,col3,col4,col5,col6,col7,col8,col9,col10,col11,col12,col13])  
  
  
        def write(self, row_name, time , missing , job, task , machine , event , user , scheduling , priority , cpu , memory , disk , different):  
                mutations=[Mutation(column="time:",value=time),Mutation(column="missing:",value=missing) ,Mutation(column="job:",value=job) ,Mutation(column="task:",value=task) ,Mutation(column="machine:",value=machine) ,Mutation(column="event:",value=event) ,Mutation(column="user:",value=user) ,Mutation(column="scheduling:",value=scheduling) ,Mutation(column="priority:",value=priority) ,Mutation(column="cpu:",value=cpu) ,Mutation(column="memory:",value=memory) ,Mutation(column="disk:",value=disk) ,Mutation(column="different:",value=different)]  
                self.client.mutateRow(self.tableName,row_name,mutations , {})  
  
        def read(self):  
                scannerId = self.client.scannerOpen(self.tableName,"",["person:",] , {})  
                while True:  
                        try:  
                                result = self.client.scannerGet(scannerId)  
                        except:  
                                break  
                        #contents = result.columns["contents:"].value  
                        print result
                        #print contents  
                self.client.scannerClose(scannerId)  
  


def simulate( input_file):
    time.sleep(0.1) # force current thread to release time slice, so other threads can gain time slice
    print threading.currentThread().name, 'start at', get_current_time()

    # you must create a Cursor object. It will let
    #  you execute all the queries you need
    #cur = db.cursor()
    reader = csv.reader(input_file, delimiter=',', quoting=csv.QUOTE_NONE)
    row = reader.next()
    
    #hbase thrift create connection
    hbase_client = HbaseWriter("localhost","9090","google-data-57M")  
    row_index = 0
    try:
        while row is not None:
            #execute_time = time.time()
            
            """
            mysql
            """
            #execute(db, cur, row)
            
            """
            opentsdb
            """
            #execute_put_tsdb_cpu(row , 'google.data.cpu')
            #execute_put_tsdb_memory(row , 'google.data.memory')
            #execute_put_tsdb_disk(row , 'google.data.disk')
            
            """
            hbase
            """
            hbase_client.write(str(row_index), row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[9], row[10], row[11], row[12])
            print row_index
            row_index = row_index+1
            row = reader.next()
    except StopIteration:
        pass
    input_file.close()
    #db.close()
    print 'Insert %s ended.' % threading.current_thread().name

if __name__ == "__main__":
    start_time = time.time()

    thread_pool = []
    # prepare for simulate
    for fileName in INPUT_FILE_LIST:
        
        """
        db = MySQLdb.connect(host="localhost",  # your host, usually localhost
                          user="root",  # your username
                          passwd="123",  # your password
                          db="GoogleCluster")  # name of the data base
                          
        """
        
        
        open_file = open(INPUT_PATH + fileName, 'r')
        t = threading.Thread(target=simulate, name=fileName, args=( open_file,))
        thread_pool.append(t)

    for thread in thread_pool:
        thread.start()


    # wait all threads end.
    for thread in thread_pool:
        thread.join()

    print 'All insert end and cost', str(time.time() - start_time), 's.'
