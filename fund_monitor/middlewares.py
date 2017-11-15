# -*- coding: utf-8 -*-

# Define here the models for your spider middleware
#
# See documentation in:
# http://doc.scrapy.org/en/latest/topics/spider-middleware.html
import re

import requests
from scrapy import signals

import random
from scrapy import signals


class AnnouncementsMonitorSpiderMiddleware(object):
    # Not all methods need to be defined. If a method is not defined,
    # scrapy acts as if the spider middleware does not modify the
    # passed objects.

    @classmethod
    def from_crawler(cls, crawler):
        # This method is used by Scrapy to create your spiders.
        s = cls()
        crawler.signals.connect(s.spider_opened, signal=signals.spider_opened)
        return s

    def process_spider_input(response, spider):
        # Called for each response that goes through the spider
        # middleware and into the spider.

        # Should return None or raise an exception.
        return None

    def process_spider_output(response, result, spider):
        # Called with the results returned from the Spider, after
        # it has processed the response.

        # Must return an iterable of Request, dict or Item objects.
        for i in result:
            yield i

    def process_spider_exception(response, exception, spider):
        # Called when a spider or process_spider_input() method
        # (from other spider middleware) raises an exception.

        # Should return either None or an iterable of Response, dict
        # or Item objects.
        pass

    def process_start_requests(start_requests, spider):
        # Called with the start requests of the spider, and works
        # similarly to the process_spider_output() method, except
        # that it doesn’t have a response associated.

        # Must return only requests (not items).
        for r in start_requests:
            yield r

    def spider_opened(self, spider):
        spider.logger.info('Spider opened: %s' % spider.name)


"""
class MyproxiesSpiderMiddleware(object):

    def __init__(self, ip=''):
        self.ip = ip

    def process_request(self, request, spider):
        while True:
            try:
                resp = requests.get('http://api.goubanjia.com/api/get.shtml?order=15802e180b819c13b343a22dd81e78d0&num=100&area=%E4%B8%AD%E5%9B%BD&carrier=0&protocol=1&an1=1&sp1=1&sort=1&system=1&distinct=0&rettype=1&seprator=%0D%0A',
                                    timeout = 30)
                proxies_list = re.split('\s+', resp.text)
                proxy = proxies_list[random.randrange(1,100,1)]
                # 验证代理
                proxies = {"http": "http://" + proxy, "https": "http://" + proxy}
                resp0 = requests.get("http://www.qq.com/robots.txt", proxies=proxies, timeout = 30)
                s_code = resp0.status_code
                if s_code == 200:
                    break
            except:
                pass

        request.meta["proxy"] = "http://" + proxy
        print 'request.meta["proxy"]: ', request.meta["proxy"]
"""