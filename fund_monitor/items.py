# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# http://doc.scrapy.org/en/latest/topics/items.html

import scrapy
import sys
sys.path.append(sys.prefix + "\\Lib\\MyWheels")
reload(sys)
sys.setdefaultencoding('utf8')

class FundMonitorItem(scrapy.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
    #title = scrapy.Field()

    fund_code = scrapy.Field()
    fund_name = scrapy.Field()
    url = scrapy.Field()
    plate_name = scrapy.Field()


