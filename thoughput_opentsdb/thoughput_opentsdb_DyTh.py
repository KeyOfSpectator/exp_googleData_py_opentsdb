#!/usr/bin/env python
'''
Created on Feb 4, 2015

@author: keyofspectator
'''

import sys
import os
import csv
import time
import MySQLdb
import threading


TSDB_PORT = '4242'

#
def execute_put_tsdb_cpu(row , metric , TSDB_IP):
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
#         print command_str
    except Exception, e:
        #logging.info('fail to insert value' + str(row))
        #logging.info(traceback.format_exc())
        pass
        
def execute_put_tsdb_memory(row , metric , TSDB_IP):
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
#         print command_str
    except Exception, e:
        #logging.info('fail to insert value' + str(row))
        #logging.info(traceback.format_exc())
        pass
        
def execute_put_tsdb_disk(row , metric , TSDB_IP):
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
#         print command_str
    except Exception, e:
        #logging.info('fail to insert value' + str(row))
        #logging.info(traceback.format_exc())
        pass

def get_current_time():
    localtime = time.localtime()
    time_string = time.strftime("%Y-%m-%d %H:%M:%S", localtime)
    return time_string

def simulate( input_file , TSDB_IP):
    time.sleep(0.1) # force current thread to release time slice, so other threads can gain time slice
#     print threading.currentThread().name, 'start at', get_current_time()

    # you must create a Cursor object. It will let
    #  you execute all the queries you need
    #cur = db.cursor()
    reader = csv.reader(input_file, delimiter=',', quoting=csv.QUOTE_NONE)
    row = reader.next()
    try:
        while row is not None:
            execute_time = time.time()
            
            
            #execute(db, cur, row)
            execute_put_tsdb_cpu(row , 'google.data.cpu' , TSDB_IP)
            execute_put_tsdb_memory(row , 'google.data.memory' , TSDB_IP)
            execute_put_tsdb_disk(row , 'google.data.disk' , TSDB_IP)
            
            row = reader.next()
    except StopIteration:
        pass
    input_file.close()
    #db.close()
    print 'Insert %s ended.' % threading.current_thread().name


def main(argv):
    """
    param:
    
    argv[1] = FolderPath
    argv[2] = ThreadNum
    """
    
    _FolderPath = argv[1]
    _ThreadNum_str = argv[2]
    #_TSDB_port = argv[3]
    _TSDB_port = "172.18.9.100"
    
    print "/*"
    print " * exp_thoughput_opentsdb"
    print " * Data 50W Line"
    print " * "
    print " * FolderPath = " + _FolderPath
    print " * Thread Num = " + _ThreadNum_str
    print " * /"
    
    _ThreadNum = int(_ThreadNum_str)
    
    
    start_time = time.time()

    thread_pool = []
    
    for i in range(_ThreadNum):
        open_file = open(_FolderPath + i + ".csv", 'r')
        t = threading.Thread(target = simulate, name = i, args=( open_file,))
        thread_pool.append(t)
        
    for thread in thread_pool:
        thread.start()

    # wait all threads end.
    for thread in thread_pool:
        thread.join()

    print 'Insert total time :  ', str(time.time() - start_time), 's'
    print "thoughput : "  + (500000 / (time.time() - start_time)) + " opt/s" 
        
    print ""
        
        
if __name__ == '__main__':
    main(sys.argv)
