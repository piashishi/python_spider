from urllib2 import Request, urlopen, URLError, HTTPError  
import urlparse
import re
import os
import time
import socket
import threading
import httplib
import Queue

os.environ['http_proxy'] = '87.254.212.121:8080'
os.environ['https_proxy'] = '87.254.212.121:8080'


#make monkey path for read
def patch_http_response_read(func):
    def inner(*args):
        try:
            return func(*args)
        except httplib.IncompleteRead, e:
            return e.partial
    return inner

httplib.HTTPResponse.read = patch_http_response_read(httplib.HTTPResponse.read)

class SpiderQueue():
	URLQueue = Queue.Queue(10000)
	urlMap = set([])
	allowWebSite = "ifanr"

	@staticmethod
	def pushTask(task):
		if SpiderQueue.validateTask(task):
			SpiderQueue.URLQueue.put_nowait(task)
			SpiderQueue.urlMap.add(task.getURL())

#this fuction could be refactor in furture if need.
	@staticmethod
	def validateTask(task):
		if task.getURL() in SpiderQueue.urlMap:
			return False
		elif SpiderQueue.allowWebSite not in task.getURL():
			return False
		else:
			return True

	@staticmethod
	def getTask():
		SpiderQueue.URLQueue.get()

	@staticmethod
	def taskDone():
		SpiderQueue.URLQueue.task_done()

	@staticmethod
	def join():
		SpiderQueue.URLQueue.join()

	@staticmethod
	def debugQueue():
		print "queue[%d], set [%d]" %(SpiderQueue.URLQueue.qsize(), len(SpiderQueue.urlMap))


class Task:
	def __init__(self, url):
		self.url = url

	def doWork(self, content):
		raise NotImplementedError;

	def getURL(self):
		return self.url

class ImageTask(Task):
	def doWork(self):
		fileName = str(time.time()).replace('.', '') + "_" + threading.current_thread().name +\
			 	"." + self.url.split('.')[-1]
		with open("picture/" + fileName, "wb") as file:
			print "write image:%s to disk:%s" %(self.url, fileName)
			file.write(content)

class URLTask(Task):
	def doWork(self, content):
		urlRE = re.compile(r'href=\"(.*?)\"')     #get all href link
		subURLs = map(lambda x, y = self.url:  x if x.startswith("http") else urlparse.urljoin(x, y), 
			urlRE.findall(content))

 		#let https go
		subURLsList = [x for x in subURLs if "https" not in x ]
		for url in subURLsList:
			if url.endswith("png") or url.endswith("jpg") or url.endswith("gif"):
				SpiderQueue.pushTask(ImageTask(url))
			else:
				SpiderQueue.pushTask(URLTask(url))

		imgSrcRE = re.compile(r'img.*?src=\"(.*?)\"')  #get all image label
		imgURLs = map(lambda x, y = self.url:  x if x.startswith("http") else urlparse.urljoin(x, y), 
			imgSrcRE.findall(content))
		for url in imgURLs:
			SpiderQueue.pushTask(ImageTask(url))

class monitorThread(threading.Thread):
	def __init__ (self):
		threading.Thread.__init__(self)

	def run(self):
		while True:
			SpiderQueue.debugQueue()
			time.sleep(2)


class spiderWorker(threading.Thread):
	def __init__ (self):
		threading.Thread.__init__(self)	
		socket.setdefaulttimeout(10)

	def readHead(self,url):
		headers = {
			'User-Agent':'Mozilla/5.0 (Windows; U; Windows NT 6.1; en-US; rv:1.9.1.6) Gecko/20091201 Firefox/3.5.6'
			}
		req = Request(url, headers=headers)
		req.get_method = lambda : 'HEAD'
		try:
			response  = urlopen(req, timeout=10)
		except Exception, e:
			print "[%s] Get Head exception:%s" %(threading.current_thread().name, e)
			return False
		ct = response.info()['Content-Type']
		if "text/html" in ct or "image/" in  ct:
			return True
		else:
			return False


	def getURLContent(self, url):
		URLContent = ""
		if self.readHead(url):
			headers = {
				'User-Agent':'Mozilla/5.0 (Windows; U; Windows NT 6.1; en-US; rv:1.9.1.6) Gecko/20091201 Firefox/3.5.6'
				}
			req = Request(url, headers=headers)
	
			try:
				response  = urlopen(req, timeout=10)
			except Exception, e:
				print "[%s] Open exception:%s" %(threading.current_thread().name, e)
				return URLContent
			else:
				#print response.getcode()
				try:
					URLContent = response.read()
				except Exception, e:
					print "[%s] Read exception:%s" %(threading.current_thread().name, e)
					return URLContent
			return URLContent
		else:
			return URLContent

	def parse(self, task):
		content = self.getURLContent(task.getURL())
		if len(content) >0:
			task.doWork(content)
		else:
			print "[%s] handle URL:%s failed!" %(threading.current_thread().name, task.getURL())

		
	def run(self):
		while True:
			task = SpiderQueue.getTask()
			self.parse(task)
			SpiderQueue.taskDone()

threadPool = []
for i in range(20):
	urlWorker = spiderWorker()
	threadPool.append(urlWorker)
	urlWorker.start()

SpiderQueue.pushTask(URLTask("http://www.ifanr.com"))
SpiderQueue.join()

for worker in threadPool:
	worker.join()