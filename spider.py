from __future__ import with_statement

import eventlet
from eventlet.green import urllib2
import urlparse
import re
import os
import time
import socket
import threading
import httplib
import traceback
import Queue
import win32crypt
import cookielib
import sqlite3

#os.environ['http_proxy'] = '10.144.1.10:8080'
#os.environ['https_proxy'] = '10.144.1.10:8080'
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

        titleRe = re.compile(r'<title>(.*?)</title>')
        title = titleRe.search(content)
        if title:
            with open("log.txt", "a+") as log:
                log.write(self.url + ":" + title.group(1) +"\n")


        urlRE = re.compile(r'href=\"(.*?)\"')     #get all href link
        subURLs = map(lambda x, y = self.url:  x if x.startswith("http") else urlparse.urljoin(y, x),
            urlRE.findall(content)) 

         #let https go
        subURLsList = [x for x in subURLs if "https" not in x and "mailto" not in x]
        for url in subURLsList:
            if url.endswith("png") or url.endswith("jpg") or url.endswith("gif"):
                SpiderQueue.pushTask(ImageTask(url))
            else:
                with open("demo.html", "a+") as file:
                    file.write(url + "\n")
                #SpiderQueue.pushTask(URLTask(url))

        imgSrcRE = re.compile(r'<img.*?src=\"(.*?)\"') 
        #some image files location were put in zoomfile attrubte.
        #<img id="aimg_11537" aid="11537" src="static/image/common/none.gif" 
        #   zoomfile="data/attachment/forum/201608/23/162246wmnnlnywll3nntlk.jpg" 
        #   file="data/attachment/forum/201608/23/162246wmnnlnywll3nntlk.jpg" 
        #   class="zoom" onclick="zoom(this, this.src, 0, 0, 0)" width="600" alt="image.jpg" 
        #   title="image.jpg" w="3264" />


        imgFileRe = re.compile(r'<img.*?zoomfile=\"(.*?)\"')
        imgURLs = map(lambda x, y = self.url:  x if x.startswith("http") else urlparse.urljoin(y, x),
            filter(lambda x : "none.gif" not in x, imgSrcRE.findall(content)))
        img2URLs = map(lambda x, y = self.url:  x if x.startswith("http") else urlparse.urljoin(y, x),
            imgFileRe.findall(content))
        for xurl in imgURLs:
            SpiderQueue.pushTask(ImageTask(xurl))
        for xurl in img2URLs:
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
        self.opener = urllib2.build_opener()


    def buildCookiesOpener(self, domain=None):
        #get cookies from chrome 
        cookie_file_path = os.path.join(os.environ['LOCALAPPDATA'], 
                                r'Google\Chrome\User Data\Default\Cookies')
        if not os.path.exists(cookie_file_path):
            raise Exception('Cookies file not exist!')

        sql = 'select host_key, name, encrypted_value, path from cookies'
        if domain:
            sql += ' where host_key like "%{}%"'.format(domain)
        with sqlite3.connect(cookie_file_path) as conn:
            rows = conn.execute(sql)

        cookiejar = cookielib.CookieJar()
        for row in rows:
            #get encrypted value
            pwdHash = str(row[2])  
            try:
                ret = win32crypt.CryptUnprotectData(pwdHash, None, None, None, 0)
            except:
                print 'Fail to decrypt chrome cookies'
                sys.exit(-1)

            cookie_item = cookielib.Cookie(version=0, name=row[1], value=ret[1],
                         port=None, port_specified=None,domain=row[0], 
                         domain_specified=None, domain_initial_dot=None,path=row[3], 
                         path_specified=None,secure=None,expires=None,
                         discard=None,comment=None,comment_url=None,rest=None,rfc2109=False,
                         )
            cookiejar.set_cookie(cookie_item)    # Apply each cookie_item to cookiejar
        self.opener =  urllib2.build_opener(urllib2.HTTPCookieProcessor(cookiejar))

    def getURLContent(self, url):
        URLContent = ""
        retry = 0
        while retry < 3:
            retry += 1
            headers = {
                
            }
            self.opener.addheaders = [('User-Agent','Mozilla/5.0 (Windows; U; Windows NT 6.1; en-US; rv:1.9.1.6) Gecko/20091201 Firefox/3.5.6')]
            try:
                response  = self.opener.open(url, timeout=10)
                URLContent = response.read()
            except Exception, e:
                #print "[%s] get URL:%s failed, exception:%s" %(threading.current_thread().name, url, e)
                continue
            else:
                break
        return URLContent


    def parse(self, task):
        content = self.getURLContent(task.getURL())
        if len(content) >0:
            task.doWork(content)
        else:
            print "[%s] Fetch URL:%s failed, timeout" %(threading.current_thread().name, task.getURL())


    def run(self):
        while True:
            while not SpiderQueue.queueEmpty(self.workerType):
                task = SpiderQueue.getTask(self.workerType)
                eventlet.sleep(100000/1000000.0)
                self.pool.spawn_n(self.parse, task)
                SpiderQueue.taskDone(self.workerType)
            self.pool.waitall()


SpiderQueue.pushTask(URLTask("http://www.upchengdu.com/forum.php?mod=viewthread&tid=2867"))
threadPool = []
urlWorker = spiderWorker("URL")
try:
    urlWorker.buildCookiesOpener("upchengdu")
except Exception, e:
    print "add cookies handler failed: %s" %str(e)
threadPool.append(urlWorker)
urlWorker.start()

urlWorker = spiderWorker("IMAGE")
try:
    urlWorker.buildCookiesOpener("upchengdu")
except Exception, e:
    print "add cookies handler failed: %s" %str(e)
threadPool.append(urlWorker)
urlWorker.start()

monitorT = monitorThread()
monitorT.start()
threadPool.append(urlWorker)

SpiderQueue.join()