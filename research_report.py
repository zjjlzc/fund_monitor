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
import shutil

sys.path.append(sys.prefix + "\\Lib\\MyWheels")
reload(sys)
sys.setdefaultencoding('utf8')


if __name__ == '__main__':
    file_path = os.getcwd() + u'/研报/'
    if os.path.exists(file_path):
        shutil.rmtree(file_path)
    os.system("scrapy crawl 0012 -a file_name=stock_json2.json") # -a date1=2017-10-01 -a date2=2018-01-11
