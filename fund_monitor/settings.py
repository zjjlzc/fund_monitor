# -*- coding: utf-8 -*-

# Scrapy settings for fund_monitor project
#
# For simplicity, this file contains only settings considered important or
# commonly used. You can find more settings consulting the documentation:
#
#     http://doc.scrapy.org/en/latest/topics/settings.html
#     http://scrapy.readthedocs.org/en/latest/topics/downloader-middleware.html
#     http://scrapy.readthedocs.org/en/latest/topics/spider-middleware.html

import sys
import random
sys.path.append(sys.prefix + "\\Lib\\MyWheels")
reload(sys)
sys.setdefaultencoding('utf8')

DOWNLOAD_TIMEOUT = 60

#import mysql_connecter
#mysql_connecter = mysql_connecter.mysql_connecter()

# 读入不需要读取的数据库key
#BAN_KEYS = [l[0] for l in mysql_connecter.connect("SELECT `key` FROM monitor") if l]

BOT_NAME = 'fund_monitor'

SPIDER_MODULES = ['fund_monitor.spiders']
NEWSPIDER_MODULE = 'fund_monitor.spiders'


# Crawl responsibly by identifying yourself (and your website) on the user-agent
#USER_AGENT = 'fund_monitor (+http://www.yourdomain.com)'

# Obey robots.txt rules
ROBOTSTXT_OBEY = False

# 防止溢出
SPEED_TOTAL_ITEMS=100
SPEED_T_RESPONSE=0.25
SPEED_ITEMS_PER_DETAIL=100
SPEED_PIPELINE_ASYNC_DELAY=3
SPEED_SPIDER_BLOCKING_DELAY=0.2

# Configure maximum concurrent requests performed by Scrapy (default: 16)
#CONCURRENT_REQUESTS = 32

# Configure a delay for requests for the same website (default: 0)
# See http://scrapy.readthedocs.org/en/latest/topics/settings.html#download-delay
# See also autothrottle settings and docs
DOWNLOAD_DELAY = random.randint(4,10)/2.0
# The download delay setting will honor only one of:
#CONCURRENT_REQUESTS_PER_DOMAIN = 16
#CONCURRENT_REQUESTS_PER_IP = 16

# Disable cookies (enabled by default)
#COOKIES_ENABLED = False

# Disable Telnet Console (enabled by default)
#TELNETCONSOLE_ENABLED = False

# Override the default request headers:
DEFAULT_REQUEST_HEADERS = {
   'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
   "User-Agent": "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/34.0.1847.131 Safari/537.36",
}

#Mysql数据库的配置信息
MYSQL_HOST = 'localhost'
MYSQL_DBNAME = 'spider'         #数据库名字，请修改
MYSQL_USER = 'spider'             #数据库账号，请修改
MYSQL_PASSWD = 'startspider'         #数据库密码，请修改
MYSQL_PORT = 3306               #数据库端口

ITEM_PIPELINES = {
    'fund_monitor.pipelines.FundMonitorPipeline': 300,#保存到mysql数据库
    #'webCrawler_scrapy.pipelines.JsonWithEncodingPipeline': 300,#保存到文件中
}

# 一次运行所有爬虫
COMMANDS_MODULE = 'fund_monitor.commands'

# 默认的重复请求检测过滤，可自己实现RFPDupeFilter的子类，覆写其request_fingerprint方法。
DUPEFILTER_CLASS = 'scrapy.dupefilters.RFPDupeFilter'

# scrapy默认使用LIFO队列存储请求，即以深度优先方式进行抓取。通过以上设置，以广度优先方式进行抓取。
DEPTH_PRIORITY = 1
SCHEDULER_DISK_QUEUE = 'scrapy.squeues.PickleFifoDiskQueue'
SCHEDULER_MEMORY_QUEUE = 'scrapy.squeues.FifoMemoryQueue'

# Enable or disable spider middlewares
# See http://scrapy.readthedocs.org/en/latest/topics/spider-middleware.html
#SPIDER_MIDDLEWARES = {
#    'fund_monitor.middlewares.FundMonitorSpiderMiddleware': 543,
#}

# Enable or disable downloader middlewares
# See http://scrapy.readthedocs.org/en/latest/topics/downloader-middleware.html
#DOWNLOADER_MIDDLEWARES = {
#    'fund_monitor.middlewares.MyCustomDownloaderMiddleware': 543,
#}

# Enable or disable extensions
# See http://scrapy.readthedocs.org/en/latest/topics/extensions.html
#EXTENSIONS = {
#    'scrapy.extensions.telnet.TelnetConsole': None,
#}

# Configure item pipelines
# See http://scrapy.readthedocs.org/en/latest/topics/item-pipeline.html
#ITEM_PIPELINES = {
#    'fund_monitor.pipelines.FundMonitorPipeline': 300,
#}

# Enable and configure the AutoThrottle extension (disabled by default)
# See http://doc.scrapy.org/en/latest/topics/autothrottle.html
#AUTOTHROTTLE_ENABLED = True
# The initial download delay
#AUTOTHROTTLE_START_DELAY = 5
# The maximum download delay to be set in case of high latencies
#AUTOTHROTTLE_MAX_DELAY = 60
# The average number of requests Scrapy should be sending in parallel to
# each remote server
#AUTOTHROTTLE_TARGET_CONCURRENCY = 1.0
# Enable showing throttling stats for every response received:
#AUTOTHROTTLE_DEBUG = False

# Enable and configure HTTP caching (disabled by default)
# See http://scrapy.readthedocs.org/en/latest/topics/downloader-middleware.html#httpcache-middleware-settings
#HTTPCACHE_ENABLED = True
#HTTPCACHE_EXPIRATION_SECS = 0
#HTTPCACHE_DIR = 'httpcache'
#HTTPCACHE_IGNORE_HTTP_CODES = []
#HTTPCACHE_STORAGE = 'scrapy.extensions.httpcache.FilesystemCacheStorage'

# ENCODING
FEED_EXPORT_ENCODING = 'utf-8'
"""
DOWNLOADER_MIDDLEWARES = {
#    'myproxies.middlewares.MyCustomDownloaderMiddleware': 543,
    'scrapy.contrib.downloadermiddleware.httpproxy.HttpProxyMiddleware': 543,
    'fund_monitor.middlewares.MyproxiesSpiderMiddleware': 125
}
"""