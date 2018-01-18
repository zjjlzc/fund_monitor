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

# GET 请求 的 gsdata 签名示例
# in the Authorization header.
import sys, os, base64, datetime, hashlib, hmac
import requests # pip install requests
# ************* 请求参数 *************
method = 'GET'
service = '/weixin/v1/articles'
host = 'api.gsdata.cn'

request_parameters = "per-page=50&wx_name=gh_fe1d2be7e8db" #&

# 签名密钥
def sign(key, msg):
    return hmac.new(key, msg.encode('utf-8'), hashlib.sha256).digest()

def getSignatureKey(key, dateStamp, serviceName):
    kDate = sign(('GSDATA' + key).encode('utf-8'), dateStamp)
    kService = sign(kDate, serviceName)
    kSigning = sign(kService, 'gsdata_request')
    return kSigning

# 开放平台中的应用信息
app_id = '265'
secret_key = 'OO52xbHAb1oArgTtP37O-v-6vKBzI6Da'
if app_id is None or secret_key is None:
    print 'No access key is available.'
    sys.exit()

# 创建时间信息
t = datetime.datetime.utcnow()
gsdate = t.strftime('%Y%m%dT%H%M%SZ')
datestamp = t.strftime('%Y%m%d') # Date w/o time, used in credential scope


# ************* TASK 1: 创建规范请求 *************
# Step 1 确认 HTTP 请求方法（GET、PUT、POST 等）.

# Step 2: 创建规范URI——从域到查询的URI的一部分
canonical_uri = service

# Step 3: 创建规范查询字符串。在这个例子中 (a GET request),
# 按字符代码点以升序顺序对参数名称进行排序。例如，以大写字母 F 开头的参数名称排在以小写字母 b 开头的参数名称之前。
# 请勿对 RFC 3986 定义的任何非预留字符进行 URI 编码，这些字符包括：A-Z、a-z、0-9、连字符 (-)、下划线 (_)、句点 (.) 和波浪符 ( ~ )。
# 使用 %XY 对所有其他字符进行百分比编码，其中“X”和“Y”为十六进制字符（0-9 和大写字母 A-F）。例如，空格字符必须编码为 %20（不像某些编码方案那样使用“+”），扩展 UTF-8 字符必须采用格式 %XY%ZA%BC。
canonical_querystring = request_parameters

# Step 4: 添加规范标头.
# 规范标头包括您要包含在签名请求中的所有 HTTP 标头的列表
# 要创建规范标头列表，请将所有标头名称转换为小写形式并删除前导空格和尾随空格。将标头值中的连续空格转换为单个空格
# 追加小写标头名称，后跟冒号
# 追加该标头的值的逗号分隔列表。请勿对有多个值的标头进行值排序
canonical_headers = 'host:' + host + '\n' + 'x-gsdata-date:' + gsdate

# Step 5: 添加已签名的标头.
#该值是您包含在规范标头中的标头列表。通过添加此标头列表，您可以向 GSDATA 告知请求中的哪些标头是签名过程的一部分以及在验证请求时 GSDATA 可以忽略哪些标头
# host 标头必须作为已签名标头包括在内。如果包括日期或 x-gsdata-date 标头，则还必须包括在已签名标头列表中的标头。
# 要创建已签名标头列表，请将所有标头名称转换为小写形式，按字符代码对其进行排序，并使用分号来分隔这些标头名称。
signed_headers = 'host;x-gsdata-date'

# Step 6: 使用 SHA256 等哈希 (摘要) 函数以基于 HTTP 或 HTTPS 请求正文中的负载创建哈希值.
# 如果负载为空，则使用空字符串作为哈希函数的输入.
payload_hash = hashlib.sha256('').hexdigest()

# Step 7: 要构建完整的规范请求，请将来自每个步骤的所有组成部分组合为单个字符串
canonical_request = method + '\n' + canonical_uri + '\n' + canonical_querystring + '\n' + canonical_headers + '\n' + signed_headers +'\n'+ payload_hash
print 'canonical_request: %s\n' % canonical_request

# ************* TASK 2: 创建要签名的字符串*************
# 以算法名称开头，后跟换行符。该值是您用于计算规范请求摘要的哈希[SHA256]
algorithm = 'GSDATA-HMAC-SHA256'
string_to_sign = algorithm + '\n' +  gsdate + '\n' +  hashlib.sha256(canonical_request).hexdigest()


# ************* TASK 3: 计算签名 *************
# 使用上面定义的函数创建签名密钥.
signing_key = getSignatureKey(secret_key, datestamp, service)

# Sign the string_to_sign using the signing_key
signature = hmac.new(signing_key, (string_to_sign).encode('utf-8'), hashlib.sha256).hexdigest()


# ************* TASK 4: 向请求添加签名信息 *************
# The signing information can be either in a query string value or in
# a header named Authorization. This code shows how to use a header.
# Create authorization header and add to request headers
authorization_header = algorithm + ' ' + 'AppKey=' + app_id + ', ' + 'SignedHeaders=' + signed_headers + ', ' + 'Signature=' + signature

# The request can include any headers, but MUST include "host", "x-gsdata-date",
# and (for this scenario) "Authorization". "host" and "x-gsdata-date" must
# be included in the canonical_headers and signed_headers, as noted
# earlier. Order here is not significant.
# Python note: The 'host' header is added automatically by the Python 'requests' library.
headers = {'x-gsdata-date':gsdate, 'Authorization':authorization_header}


# ************* SEND THE REQUEST *************
request_url = 'http://'+host+service + '?' + canonical_querystring

print '\nBEGIN REQUEST++++++++++++++++++++++++++++++++++++'
print 'Request URL = ' + request_url
r = requests.get(request_url, headers=headers)

print '\nRESPONSE++++++++++++++++++++++++++++++++++++'
print 'Response code: %d\n' % r.status_code
with open('weichat_data.txt', 'w') as f:
    f.write(r.text)

# if __name__ == '__main__':
#     end_date = datetime.datetime.now() - datetime.timedelta(days=1)  # datetime.datetime(year=2018, month=1, day=10)
#     file_path = u"%s\%s微信文章.xlsx" % (os.getcwd(), end_date.strftime('%Y%m%d'))
#     if os.path.exists(file_path):
#         os.remove(file_path)
#
#     os.system("scrapy crawl 0013")