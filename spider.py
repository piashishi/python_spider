from __future__ import with_statement

import eventlet
from eventlet.green.urllib2 import Request, urlopen, URLError, HTTPError
import urlparse
import re
import os
import time
import socket
import threading
import httplib
import traceback
import Queue

os.environ['http_proxy'] = '10.144.1.10:8080'
os.environ['https_proxy'] = '10.144.1.10:8080'


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
    queueDict = {
        'URL': Queue.Queue(10000),
        'IMAGE': Queue.Queue(10000)
    }

    handleCount = 0
    urlMap = set([])
    allowWebSite = "chengdu"

    @staticmethod
    def pushTask(task):
        if SpiderQueue.validateTask(task):
            SpiderQueue.queueDict[task.getType()].put_nowait(task)
            SpiderQueue.urlMap.add(task.getURL())

    @staticmethod
    def queueEmpty(taskType):
        return  SpiderQueue.queueDict[taskType].empty()

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
    def getTask(taskType):
        return  SpiderQueue.queueDict[taskType].get()

    @staticmethod
    def taskDone(taskType):
        SpiderQueue.queueDict[taskType].task_done()

    @staticmethod
    def join():
        SpiderQueue.queueDict["URL"].join()
        SpiderQueue.queueDict["IMAGE"].join()


    @staticmethod
    def debugQueue(timeval):
        speed = 0
        if SpiderQueue.handleCount == 0:
            SpiderQueue.handleCount = len(SpiderQueue.urlMap) - SpiderQueue.queueDict["URL"].qsize() - SpiderQueue.queueDict["IMAGE"].qsize()
            speed = SpiderQueue.handleCount/timeval
        else:
            currentCount = len(SpiderQueue.urlMap) - SpiderQueue.queueDict["URL"].qsize() - SpiderQueue.queueDict["IMAGE"].qsize()
            speed = (currentCount - SpiderQueue.handleCount)/timeval
            SpiderQueue.handleCount = currentCount

        print "URL Qeue Size:[%d], Image Queue Size:[%d], Set [%d], handle Speed:[%d]" %( SpiderQueue.queueDict["URL"].qsize(),
            SpiderQueue.queueDict["IMAGE"].qsize(), len(SpiderQueue.urlMap), speed)


class Task:
    def __init__(self, url):
        self.url = url
        self.retry = 0

    def doWork(self, content):
        raise NotImplementedError;

    def getURL(self):
        return self.url

    def getType(self):
        raise NotImplementedError;

    def retryAdd(self):
        self.retry += 1

    def getRetry(self):
        return self.retry


class ImageTask(Task):
    def doWork(self, content):
        # fileName = str(time.time()).replace('.', '') + "_" + threading.current_thread().name +\
        #          "." + self.url.split('.')[-1]
        fileName = str(time.time()).replace('.', '') + "_" + threading.current_thread().name +\
                 "." + "png"
        with open("picture/" + fileName, "wb") as file:
            file.write(content)

    def getType(self):
        return "IMAGE"

class URLTask(Task):
    def doWork(self, content):
        urlRE = re.compile(r'href=\"(.*?)\"')     #get all href link
        subURLs = map(lambda x, y = self.url:  x if x.startswith("http") else urlparse.urljoin(y, x),
            urlRE.findall(content)) 

         #let https go
        subURLsList = [x for x in subURLs if "https" not in x and "mailto" not in x]
        for url in subURLsList:
            if url.endswith("png") or url.endswith("jpg") or url.endswith("gif"):
                SpiderQueue.pushTask(ImageTask(url))
            else:
                SpiderQueue.pushTask(URLTask(url))

        imgSrcRE = re.compile(r'img.*?src=\"(.*?)\"')  #get all image label
        imgURLs = map(lambda x, y = self.url:  x if x.startswith("http") else urlparse.urljoin(y, x),
            imgSrcRE.findall(content))
        for xurl in imgURLs:
            SpiderQueue.pushTask(ImageTask(xurl))

    def getType(self):
        return "URL"

class monitorThread(threading.Thread):
    def __init__ (self):
        threading.Thread.__init__(self)

    def run(self):
        while True:
            time.sleep(5)
            SpiderQueue.debugQueue(5)



class spiderWorker(threading.Thread):
    def __init__ (self, workerType):
        threading.Thread.__init__(self) 
        self.workerType = workerType
        self.pool = eventlet.GreenPool()
        self.workerType = workerType

    def readHead(self,url):
        headers = {
            'User-Agent':'Mozilla/5.0 (Windows; U; Windows NT 6.1; en-US; rv:1.9.1.6) Gecko/20091201 Firefox/3.5.6'
            }
        req = Request(url, headers=headers)
        req.get_method = lambda : 'HEAD'
        try:
            response  = urlopen(req, timeout=10)
        except socket.timeout, e:
            raise e
        except Exception, e:
            print "[%s] Get URL Head %s failed, exception:%s" %(threading.current_thread().name, url, e)
            return False
        ct = response.info()['Content-Type']
        if "text/html" in ct or "image/" in  ct:
            return True
        else:
            print "[%s] URL:%s not support Content-Type:%s" %(threading.current_thread().name, url, ct)
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
            except socket.timeout, e:
                raise e
            except Exception, e:
                print "[%s] Open URL:%s failed, exception:%s" %(threading.current_thread().name, url, e)
                #traceback.print_exc()
                return URLContent
            else:
                #print response.getcode()
                try:
                    URLContent = response.read()
                except socket.timeout, e:
                    raise e
                except Exception, e:
                    print "[%s] Read URL:%s failed, exception:%s" %(threading.current_thread().name, url, e)
                    return URLContent
            return URLContent
        else:
            return URLContent

    def parse(self, task):
        try:
            content = self.getURLContent(task.getURL())
        except socket.timeout, e:
            task.retryAdd()
            if not task.getRetry() > 3:
                SpiderQueue.pushTask(task)
            else:
                print "[%s] Fetch URL:%s failed, timeout" %(threading.current_thread().name, url)
        else:
            if len(content) >0:
                task.doWork(content)


    def run(self):
        while True:
            while not SpiderQueue.queueEmpty(self.workerType):
                task = SpiderQueue.getTask(self.workerType)
                eventlet.sleep(100000/1000000.0)
                self.pool.spawn_n(self.parse, task)
                SpiderQueue.taskDone(self.workerType)
            self.pool.waitall()


SpiderQueue.pushTask(URLTask("http://www.upchengdu.com/"))
threadPool = []
urlWorker = spiderWorker("URL")
threadPool.append(urlWorker)
urlWorker.start()

urlWorker = spiderWorker("IMAGE")
threadPool.append(urlWorker)
urlWorker.start()

monitorT = monitorThread()
monitorT.start()
threadPool.append(urlWorker)


SpiderQueue.join()