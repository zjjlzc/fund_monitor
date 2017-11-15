# -*- coding:utf-8 -*-  
"""
--------------------------------
    @Author: Dyson
    @Contact: Weaver1990@163.com
    @file: setup.py
    @time: 2017/4/18 16:04
--------------------------------
"""
from setuptools import setup, find_packages

setup(name='scrapy-mymodule',
  entry_points={
    'scrapy.commands': [
      'monitor=fund_monitor.commands:monitor',
      'content=fund_monitor.commands:content'
    ],
  },
)