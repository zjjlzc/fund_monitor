# -*- coding:utf-8 -*-  
"""
--------------------------------
    @Author: Dyson
    @Contact: Weaver1990@163.com
    @file: api51.py
    @time: 2017/11/24 11:35
--------------------------------
"""
import sys
import os
import urllib2

import pandas as pd
import numpy as np

sys.path.append(sys.prefix + "\\Lib\\MyWheels")
reload(sys)
sys.setdefaultencoding('utf8')
import set_log  # log_obj.debug(文本)  "\x1B[1;32;41m (文本)\x1B[0m"

log_obj = set_log.Logger('api51.log', set_log.logging.WARNING,
                         set_log.logging.DEBUG)
log_obj.cleanup('api51.log', if_cleanup=True)  # 是否需要在每次运行程序前清空Log文件

user_key = '1923eae2a3054d4c92a7eb74d7f65396'

class api51(object):
    def __init__(self):
        pass

    def connect(self, appcode, querys, path = '/kline'):
        host = 'http://stock.api51.cn'
        method = 'GET'
        #appcode = '343dff93ee6549daab7f1d6b8e244027'
        #'candle_mode=0&candle_period=1&data_count=10&date=date&end_date=end_date&fields=fields&get_type=offset&min_time=min_time&prod_code=000001.SS&search_direction=1&start_date=start_date'
        print "api51 querys:\n", '\n'.join(['%s=%s' %(key,querys[key]) for key in querys])
        querys = '&'.join(['%s=%s' %(key,querys[key]) for key in querys])
        bodys = {}
        url = host + path + '?' + querys

        request = urllib2.Request(url)
        request.add_header('Authorization', 'APPCODE ' + appcode)

        print url
        try_times = 1 # 重试次数
        while try_times < 6:
            try:
                response = urllib2.urlopen(request)
                print "api51 respone:", response.code

                content = response.read()

                if (content):
                    data = pd.json.loads(content)
                    return data
            except:
                print u'没有接受到api51的数据，即将进行第%s次重试' %try_times
            finally:
                try_times = try_times + 1


if __name__ == '__main__':
    api51 = api51()
    # http://stock.api51.cn/kline?candle_mode=0&candle_period=6&data_count=1000&get_type=offset&prod_code=000001.SS&search_direction=1
    d = {
        'prod_code':'GN3568', # '000001.SZ',
        'candle_mode':'0',
        'data_count':'1000',
        'get_type':'offset',
        # 'end_date':'20170419',
        'search_direction':'1',
        # 'get_type':'range',
        'candle_period':'6',
        # 'start_date':'20161020',
    }
    print api51.connect(user_key, d)