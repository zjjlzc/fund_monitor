# -*- coding:utf-8 -*-
"""
--------------------------------
    @Author: Dyson
    @Contact: Weaver1990@163.com
    @file: text_spider.py
    @time: 2017/10/25 11:14
--------------------------------
"""
import sys
import os
import datetime

sys.path.append(sys.prefix + "\\Lib\\MyWheels")
reload(sys)
sys.setdefaultencoding('utf8')


if __name__ == '__main__':
    end_date = datetime.datetime.now() - datetime.timedelta(days=1)  # datetime.datetime(year=2018, month=1, day=10)
    file_path = u"%s\%s微信文章.xlsx" % (os.getcwd(), end_date.strftime('%Y%m%d'))
    if os.path.exists(file_path):
        os.remove(file_path)

    os.system("scrapy crawl 0013")