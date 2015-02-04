'''
@author: zhichen
@date: 19, Dec, 2014

Simulate the process of GoogleCluster data's generation.
This the improved version. Compared with primary version, only one process insert data into database,
due to 300 files have been combined together.

'''


import os
import csv
import logging
import MySQLdb
import traceback
import time
import threading
from django.template.defaultfilters import length
from _curses import A_ALTCHARSET
from guppy.heapy.RemoteConstants import LOCALHOST

#INPUT_PATH = "/home/keyofspectator/ubuntu_exp/"

INPUT_PATH = "/home/keyofspectator/ubuntu_exp/throughput/test_data/"


LOG_PATH = '/home/keyofspectator/ubuntu_exp/log'
#INPUT_FILE_LIST = ["41.csv"]
#
INPUT_FILE_LIST = sorted([name for name in os.listdir(INPUT_PATH) if name.endswith('.csv')])

TIME_CONVERT = 1000 * 1000 * 300
INSERT_PERIOD = 120
SCALE_TIMES = 1 # how many times to scale
SCALE_RANGE = 9
SCALE_COLUMN_INDEX = 5 # which column to scale, use event_type column now
INSERT_TASK_EVENTS = '''insert into task_events values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'''
INSERT_TASK_USAGE = '''insert into task_usage values (%s, %s, %s, %s, %s, %s, %s,
                                                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'''
TSDB_IP = '127.0.0.1'
TSDB_PORT = '4242'

TEST_TIME_OFFSET = 3600  
                                                       
#Schema_value = []                                                


def get_current_time():
    localtime = time.localtime()
    time_string = time.strftime("%Y-%m-%d %H:%M:%S", localtime)
    return time_string


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
        logging.info('fail to insert value' + str(row))
        logging.info(traceback.format_exc())
    db.rollback()

def execute_put_tsdb_cpu(row , metric):
    """
    
    :param row:
    :return: no return
    """
    try:
        #date_s = int(time.time()) + int(row[0])/1000 -TEST_TIME_OFFSET   # data time
        date_s = int(time.time())                                              # current time
        #user = string_fix(row[6])
        user = row[6] #for fixed data
        
        put_str = "put %s %s %s job=%s task=%s machine=%s event=%s user=%s" % (metric, date_s , row[9] , row[2] , row[3] , row[4] , row[5] , user)
        
        command_str = "echo '%s' | nc -w 30 %s %s" % (put_str , TSDB_IP , TSDB_PORT)
        #insert put to tsdb
        os.system(command_str)
        print command_str
    except Exception, e:
        logging.info('fail to insert value' + str(row))
        logging.info(traceback.format_exc())

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


def simulate():
    #print 'Start to insert %s ...' % threading.current_thread().name
    logging.info('Start to insert %s ...' % threading.current_thread().name)


    #db = MySQLdb.connect(host="localhost",  # your host, usually localhost
     #                 user="root",  # your username
     #                 passwd="123",  # your password
     #                 db="GoogleCluster")  # name of the data base

    # you must create a Cursor object. It will let
    #  you execute all the queries you need
    #cur = db.cursor()

    with open(INPUT_PATH + threading.currentThread().name, 'r') as inFile:
        reader = csv.reader(inFile, delimiter=',', quoting=csv.QUOTE_NONE)
        row = reader.next()
        try:
            insert_start = time.time()
            total_wait_time = 0
            insert_count = 0
            # print insert_start
            time_period = 1
            # time_period determine the scaled time the records be inserted in this while loop belong to
            while row is not None:
                # judge if the timestamp belong to the scaled time this while loop should operate,
                # if it is, insert this record into database, if not, judge if current time exceed
                # the time_period this while loop should operate, if it is, continue to insert and
                # increment the time_period, if not, finish this while loop and wait until time has
                # reached next time_period.
                if float(row[0]) / TIME_CONVERT > INSERT_PERIOD:
                    break
                if float(row[0]) / TIME_CONVERT < time_period or time.time() > insert_start + time_period:
                    #print 'insert'
                    if time.time() > insert_start + time_period:
                        time_period += 1
                    for i in range(SCALE_TIMES):
                        scaled_row = row
                        scaled_row[SCALE_COLUMN_INDEX] = int(row[SCALE_COLUMN_INDEX]) + i * SCALE_RANGE
                        # execute(db, cur, scaled_row)
                        
                        execute_put_tsdb_cpu(scaled_row , 'google.data.cpu')
                        
                        
                        # test value
                        
                        #print  'Value:'
                        #for i in range(len(scaled_row)):
                        #    print "the %s   : %s  " % (i , scaled_row[i])
                        
                        insert_count += 1
                    row = reader.next()
                else:
                    #print 'wait'
                    wait_start = time.time()
                    time_period += 1
                    while time.time() < insert_start + (time_period - 1):
                        pass
                    total_wait_time += (time.time() - wait_start)
        except StopIteration:
            pass
    #db.close()
    print 'Insert %s ended.' % threading.current_thread().name
    print 'Insert %s records.' % insert_count
    print 'Total wait time', total_wait_time
    logging.info('Insert %s ended.' % threading.current_thread().name)


start_time = time.time()
print 'thread %s is running...' % threading.current_thread().name
logging.basicConfig(filename=LOG_PATH + get_current_time() + '.log',level=logging.DEBUG)
logging.info('thread %s is running...' % threading.current_thread().name)
thread_pool = []
for fileName in INPUT_FILE_LIST:
    t = threading.Thread(target=simulate, name=fileName)
    t.start()
    thread_pool.append(t)

# wait all threads end.
for thread in thread_pool:
    thread.join()

print 'All insert end and cost', str(time.time() - start_time), 's.'