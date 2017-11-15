# -*- coding:utf-8 -*-  
"""
--------------------------------
    @Author: Dyson
    @Contact: Weaver1990@163.com
    @file: monitor.py
    @time: 2017/4/18 14:05
--------------------------------
"""
import sys
import os

sys.path.append(sys.prefix + "\\Lib\\MyWheels")
reload(sys)
sys.setdefaultencoding('utf8')
import mysql_connecter
mysql_con = mysql_connecter.mysql_connecter()

from scrapy.commands import ScrapyCommand
from scrapy.crawler import CrawlerRunner
from scrapy.utils.conf import arglist_to_dict


class Command(ScrapyCommand):
    requires_project = True

    def syntax(self):
        return '[options]'

    def short_desc(self):
        return 'Runs all of the spiders'

    def add_options(self, parser):
        ScrapyCommand.add_options(self, parser)
        parser.add_option("-a", dest="spargs", action="append", default=[], metavar="NAME=VALUE",
                          help="set spider argument (may be repeated)")
        parser.add_option("-o", "--output", metavar="FILE",
                          help="dump scraped items into FILE (use - for stdout)")
        parser.add_option("-t", "--output-format", metavar="FORMAT",
                          help="format to use for dumping items with -o")

    def process_options(self, args, opts):
        ScrapyCommand.process_options(self, args, opts)
        try:
            opts.spargs = arglist_to_dict(opts.spargs)
        except ValueError:
            raise UsageError("Invalid -a value, use -a NAME=VALUE", print_help=False)

    def run(self, args, opts):
        # settings = get_project_settings()

        spider_loader = self.crawler_process.spider_loader
        spider_list = mysql_con.connect("SELECT spider_id FROM spider_list WHERE `type` = 'monitor'", host='localhost',user='spider',password = 'startspider', dbname = 'spider')
        spider_list = [l0[0] for l0 in spider_list]
        for spidername in args or spider_loader.list():
            if spidername in spider_list:
                print "*********monitor spidername************" + spidername
                self.crawler_process.crawl(spidername, **opts.spargs)

        self.crawler_process.start()