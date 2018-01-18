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
    os.system("scrapy crawl 0001")
    # os.system("scrapy crawl 0007")
    # os.system("scrapy crawl 0001 -a method=daily")
    # os.system("scrapy crawl 0014")