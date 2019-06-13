#coding=UTF-8

'''
	通过 python 模拟生成用户行为日志
'''

import random
import time

url_paths = [
	"class/112.html",
	"class/128.html",
	"class/145.html",
	"class/322.html",
	"class/146.html",
	"class/131.html",
	"class/130.html",
	"learn/1044",
	"course/list"
]

ip_slices = [132,45,5,65,84,251,58,111,30,58,96,42,2,54,36]

http_referers = [
	"https://www.baidu.com/s?wd={query}",
	"https://www.sogou.com/web?query={query}",
	"http://cn.bing.com/search?q={query}",
	"https://search.yahoo.com/search?p={query}"
]

search_keywords = [
	"spark sql实战",
	"spark streaming实战",
	"hadoop实战",
	"storm实战",
	"大数据面试"
]

status_codes = ["200","404","500"]

def sample_status_code():
	return random.sample(status_codes,1)[0]

def sample_url():
	return random.sample(url_paths,1)[0]
	
def sample_ip():
	slice = random.sample(ip_slices,4)
	return ".".join([str(item) for item in slice])
	
def sample_referer():
	if random.uniform(0,1) > 0.2:
		return "-"
		
	refer_str = random.sample(http_referers,1)
	query_str = random.sample(search_keywords,1)
	return refer_str[0].format(query=query_str[0])

def generate_log(count = 10):
	time_str = time.strftime("%Y-%m-%d %H:%M:%S",time.localtime())
	
	'''
		通过追加的方式写入日志 （a+）
	'''
	f = open("/opt/data/python_log/access.log","a+")
	
	while count >= 1:
		query_log = "{ip}\t{time}\t\"GET /{url} HTTP/1.1\"\t{status}\t{sample_referer}".format(url=sample_url(),ip=sample_ip(),sample_referer=sample_referer(),status=sample_status_code(),time=time_str)
		print query_log
		
		f.write(query_log + "\n")
		count = count -1
		


'''
	主函数，程序入口
'''
if __name__=='__main__':
	generate_log(100)