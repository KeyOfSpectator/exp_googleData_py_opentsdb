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

#INPUT_PATH = "/home/chenzhi/Documents/test/throughput/split_equally_1/"

#INPUT_PATH = "/home/keyofspectator/ubuntu_exp/throughput/split_equally_1/"

INPUT_PATH = "/home/keyofspectator/ubuntu_exp/throughput/test_data/tsdb_single/"

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

#
def execute_put_tsdb_cpu(row , metric):
    """
    
    :param row:
    :return: no return
    """
    try:
        #date_s = int(time.time()) + int(row[0])/1000 -TEST_TIME_OFFSET   # data time
        date_s = int(time.time())                                              # current time
        #user = string_fix(row[6])
        user = row[6] # for fixed data
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
        #user = string_fix(row[6])
        user = row[6] # for fixed data
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
        #user = string_fix(row[6])
        user = row[6] # for fixed data
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

def simulate( input_file ):
    time.sleep(0.1) # force current thread to release time slice, so other threads can gain time slice
    print threading.currentThread().name, 'start at', get_current_time()

    # you must create a Cursor object. It will let
    #  you execute all the queries you need
    #cur = db.cursor()
    reader = csv.reader(input_file, delimiter=',', quoting=csv.QUOTE_NONE)
    row = reader.next()
    try:
        while row is not None:
            execute_time = time.time()
            
            
            #execute(db, cur, row)
            execute_put_tsdb_cpu(row , 'google.data.cpu')
            execute_put_tsdb_memory(row , 'google.data.memory')
            execute_put_tsdb_disk(row , 'google.data.disk')
            
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
