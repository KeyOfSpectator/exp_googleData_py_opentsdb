#!/usr/bin/env python
'''
Created on Feb 4, 2015

@author: keyofspectator
'''

import sys
import os
import csv
import time
import threading


TSDB_PORT = '4242'
TEST_TIME_OFFSET = 200000000  


def list_2_str(list_tmp):
    """
    list to string
    
    return string
    """
    tmp = ''.join(list_tmp)
    return tmp


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

def fix_user_name(row):
    """
    fix the user name 
    delete the illegal characters in string , illegal characters: +,=
    """
    row[6] = string_fix(row[6])
    return row

def fixed_execute(input_file , output_file):
    
    reader = csv.reader(input_file, delimiter=',', quoting=csv.QUOTE_NONE)
    row = reader.next()
    spanwriter = csv.writer(output_file , delimiter=',', quotechar='|' , quoting=csv.QUOTE_MINIMAL)
    
    #    
    
    try:
        while row is not None:
            row = fix_user_name(row)
            spanwriter.writerow(row)
            row = reader.next()
    except StopIteration:
        pass
    input_file.close()
    output_file.close()
#     print 'fixed end'

#
def execute_put_tsdb_cpu(row , metric , TSDB_IP , time_stamp):
    """
    
    :param row:
    :return: no return
    """
    try:
        #date_s = int(time.time()) + int(row[0])/1000 -TEST_TIME_OFFSET   # data time
        date_s = int(time.time())                                              # current time
        #user = string_fix(row[6])
        user = row[6] # for fixed data
        put_str = "%s %s %s job=%s task=%s machine=%s event=%s user=%s" % (metric, time_stamp , row[9] , row[2] , row[3] , row[4] , row[5] , user)
        
#        command_str = "echo '%s' | nc -w 30 %s %s" % (put_str , TSDB_IP , TSDB_PORT)
        #insert put to tsdb
#        os.system(command_str)
#         print command_str
        if  row[9] != '' :
           print put_str
    except Exception, e:
        #logging.info('fail to insert value' + str(row))
        #logging.info(traceback.format_exc())
        pass
        
def execute_put_tsdb_memory(row , metric , TSDB_IP , time_stamp):
    """
    
    :param row:
    :return: no return
    """
    try:
        #date_s = int(time.time()) + int(row[0])/1000 -TEST_TIME_OFFSET   # data time
        date_s = int(time.time())                                              # current time
        #user = string_fix(row[6])
        user = row[6] # for fixed data

        put_str = "%s %s %s job=%s task=%s machine=%s event=%s user=%s" % (metric, time_stamp , row[10] , row[2] , row[3] , row[4] , row[5] , user)
        
#        command_str = "echo '%s' | nc -w 30 %s %s" % (put_str , TSDB_IP , TSDB_PORT)
        #insert put to tsdb
#        os.system(command_str)
#         print command_str
        if row[10] != "":
           print put_str
    except Exception, e:
        #logging.info('fail to insert value' + str(row))
        #logging.info(traceback.format_exc())
        pass
        
def execute_put_tsdb_disk(row , metric , TSDB_IP , time_stamp):
    """
    
    :param row:
    :return: no return
    """
    try:
        #date_s = int(time.time()) + int(row[0])/1000 -TEST_TIME_OFFSET   # data time
        date_s = int(time.time())                                              # current time
        #user = string_fix(row[6])
        user = row[6] # for fixed data
        put_str = "%s %s %s job=%s task=%s machine=%s event=%s user=%s" % (metric, time_stamp , row[11] , row[2] , row[3] , row[4] , row[5] , user)
        
#        command_str = "echo '%s' | nc -w 30 %s %s" % (put_str , TSDB_IP , TSDB_PORT)
        #insert put to tsdb
#        os.system(command_str)
#         print command_str
        if row[11] != "" :
           print put_str
    except Exception, e:
        #logging.info('fail to insert value' + str(row))
        #logging.info(traceback.format_exc())
        pass

def get_current_time():
    localtime = time.localtime()
    time_string = time.strftime("%Y-%m-%d %H:%M:%S", localtime)
    return time_string

def simulate( input_file , TSDB_IP , thread_id , data_line_everyThread , time_base):
    time.sleep(0.1) # force current thread to release time slice, so other threads can gain time slice
#     print threading.currentThread().name, 'start at', get_current_time()

    # you must create a Cursor object. It will let
    #  you execute all the queries you need
    #cur = db.cursor()
    reader = csv.reader(input_file, delimiter=',', quoting=csv.QUOTE_NONE)
    row = reader.next()

    data_start_base = thread_id * data_line_everyThread
    time_stamp = data_start_base

    try:
        while row is not None:
            execute_time = time.time()
            
            # this thread excute the data from (thread_id * data_line_everyThread) ~ (thread_id * data_line_everyThread + data_line_everyThread)
            

            #execute(db, cur, row)
            

            execute_put_tsdb_cpu(row , 'google.data.event.cpu' , TSDB_IP , time_stamp )
            execute_put_tsdb_memory(row , 'google.data.event.memory' , TSDB_IP , time_stamp)
            execute_put_tsdb_disk(row , 'google.data.event.disk' , TSDB_IP , time_stamp)
            
            time_stamp = time_stamp + 1
            #print row
            
            row = reader.next()
            
    except StopIteration:
        pass
    input_file.close()
    #db.close()
    print 'Insert %s ended.' % threading.current_thread().name


def main(argv):
    """
    param:
    
    argv[1] = FolderPath (end with /)
    argv[2] = ThreadNum
    argv[3] = Data_line
    """
    
    _FolderPath = argv[1]
    _ThreadNum_str = argv[2]
    _data_line_str = argv[3]
    #_TSDB_port = argv[3]
    _TSDB_IP = "127.0.0.1"

#info
"""    
    print "/*"
    print " * exp_thoughput_opentsdb"
    print " * Data 50W Line"
    print " * "
    print " * FolderPath = " + _FolderPath
    print " * Thread Num = " + _ThreadNum_str
    print " * Data line = " + _data_line_str
    print " * /"
"""

    _ThreadNum = int(_ThreadNum_str)
    _data_line = int(_data_line_str)
    
    #fix data
    for i in range(_ThreadNum):
        
        _INPUT_PATH_FIX = _FolderPath + str(i) + ".csv"
        _OUTPUT_PATH_FIX = _FolderPath +  str(i) + "_tmp"+ ".csv"
        
        input_file = open(_INPUT_PATH_FIX, 'r')
        output_file = open(_OUTPUT_PATH_FIX , 'wb')
        
        fixed_execute( input_file , output_file )
    
    # for cal the thoughput 
    start_time = time.time()

    # for every thread 
    _time_base = int(time.time()) - 40000
    _data_line_everyThread = _data_line/_ThreadNum + 1 

    thread_pool = []
    
    for i in range(_ThreadNum):
        open_file = open(_OUTPUT_PATH_FIX)
        t = threading.Thread(target = simulate, name = i, args=( open_file, _TSDB_IP , i , _data_line_everyThread , _time_base))
        thread_pool.append(t)
        
    for thread in thread_pool:
        thread.start()

    # wait all threads end.
    for thread in thread_pool:
        thread.join()

#info
"""
    print 'Insert total time :  ', str(time.time() - start_time), 's'
    print "thoughput : "  + str(500000 / (time.time() - start_time)) + " opt/s" 
        
    print ""
"""     
        
if __name__ == '__main__':
    main(sys.argv)
