# -*- coding: utf-8 -*-
'''
Created on Jan 9, 2015

@author: keyofspectator
'''
import pycurl
import os

# default to www.baidu.com to test
def httpCall(url, probe_key):
    """
    @todo: 不能连上时,c.perform()或抛出异常，需要处理
        这种情况下（1）再次尝试（2）把数据标记为出错数据(再加个标记？)
    """
    c = pycurl.Curl()
    #c.setopt(c.URL, 'http://www.baidu.com/')
    c.setopt(c.URL, url)
    c.setopt(c.CONNECTTIMEOUT, 60)
    c.setopt(c.TIMEOUT, 600)
    #将输出重定向到null，防止输出扰乱log
    c.setopt(c.FILE, open(os.devnull,'w'))
    """设置次选项，防止出现DNS解析超时时引起libcurl crash，
    导致'longjmp causes uninitialized stack frame'，而停止程序
    """
    c.setopt(c.NOSIGNAL, 1)
    #c.setopt(c.WRITEDATA, buffer)
    try:
        c.perform()
        results = '{"metric_name":"%s", "response_time" : %s ,"body_size" : %s, "host": "%s", "probe_key":"%s"}'\
                % ("HttpMetric",\
                c.getinfo(c.TOTAL_TIME),\
                c.getinfo(c.SIZE_DOWNLOAD),\
                url,\
                probe_key)
    except Exception:
        #先特殊处理下
        results = '{"metric_name":"%s", "response_time" : %d ,"body_size" : %d, "host": "%s", "probe_key":"%s"}'\
                % ("HttpMetric",\
                2000000,\
                2000000,\
                url,\
                probe_key)
    return results

if __name__ == '__main__':
    #print httpCall("http://www.baidu.com", "1e46afa2-6176-3cd3-9750-3015846723df")
    print httpCall("http://localhost:4242/api/query?start=10s-ago&m=sum:rate:proc.loadavg.1m", "1e46afa2-6176-3cd3-9750-3015846723df")