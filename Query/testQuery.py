"""

"""
import MySQLdb
import time

FIVE_MINUTE = 300 * 1000 * 1000
SELECT_TIME = """
select time from task_events
where jobID=5285926325 and task_index=0 and event_type=3;"""
SELECT_RANG_QUERY = """
select * from task_usage
where jobID=5285926325 and task_index=0
and start_time>=%s and start_time<=%s;
"""

if __name__ == "__main__":
    start_time = time.time()
    db = MySQLdb.connect(host="localhost",  # your host, usually localhost
                      user="root",  # your username
                      passwd="123",  # your password
                      db="GoogleCluster")  # name of the data base
    cur = db.cursor()
    cur.execute(SELECT_TIME)
    result = cur.fetchall()
    for index in range(len(result)):
        timestamp = result[index][0]
        cur.execute(SELECT_RANG_QUERY % (long(timestamp)-FIVE_MINUTE, long(timestamp)+FIVE_MINUTE))
        query_result = cur.fetchall()
        for i in range(len(query_result)):
            print query_result[i]
    print time.time() - start_time
