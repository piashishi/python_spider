from __future__ import with_statement
from bs4 import BeautifulSoup
from socket import timeout

import eventlet
from eventlet.green import urllib2

import urlparse
import re
import os
import time
import socket
import threading
from threading import Timer
import httplib
import traceback
import win32crypt
import cookielib


os.environ['http_proxy'] = '87.254.212.121:8080'
os.environ['https_proxy'] = '87.254.212.121:8080'

#base class
class worker(threading.Thread):
    def __init__ (self, manager):
        threading.Thread.__init__(self) 
        self.pool = eventlet.GreenPool()
        self.totalTask = 0
        self.successTask = 0
        self.failTask = 0
        self.timeoutTask = 0
        self.greenTaskArray = []
        self.manager = manager


    def doWork(self, url, content=None):
        raise NotImplementedError

    def TaskType(self):
        raise NotImplementedError


    def taskFinished(self, gt):
        if gt.wait():
            self.successTask += 1
        else:
            self.failTask += 1
        self.greenTaskArray.remove(gt)

      
    def getRuningTaskNumber(self):
        return self.pool.running()

    def run(self):
        index = 0
        while True and not self.manager.exit:
            try:
                task = self.manager.getTask(self.TaskType())
                if task:
                    self.totalTask += 1
                    gt = self.pool.spawn(self.doWork, url=task[0], content=task[1])
                    gt.link(self.taskFinished)
                    self.greenTaskArray.append(gt)
                    self.manager.taskDone(self.TaskType())
                    eventlet.sleep(100000/1000000.0) #sleep 0.1 Second
                else:
                    self.pool.waitall()
            except Exception as e:
                print "[%s] handle failed, exception:%s" %(threading.current_thread().name, e)


#work for fetch HTML Content
class URLFetchWorker(worker):

    #make monkey path for read
    def patch_http_response_read(func):
        def inner(*args):
            try:
                return func(*args)
            except httplib.IncompleteRead, e:
                return e.partial
        return inner

    httplib.HTTPResponse.read = patch_http_response_read(httplib.HTTPResponse.read)

    def __init__ (self, manager, useCookies=False):
        super(URLFetchWorker, self).__init__(manager)
        self.timeoutTask = 0
        if useCookies:
            self.setCookies()
        else:
            self.opener = urllib2.build_opener()

    #the cookies for Chrome
    def setCookies(self):
        cookie_file_path = os.path.join(os.environ['LOCALAPPDATA'], 
                                r'Google\Chrome\User Data\Default\Cookies')
        if not os.path.exists(cookie_file_path):
            raise Exception('Cookies file not exist!')

        domain = re.compile(r'http://(.*?)/').findall(webSite)[0]


        sql = 'select host_key, name, encrypted_value, path from cookies'
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
                response  = self.opener.open(url, timeout=30)
                URLContent = response.read()
                url = response.geturl()

            except (urllib2.HTTPError, urllib2.URLError) as error:
                print "[%s] get URL:%s failed, exception:%s" %(threading.current_thread().name, url, error)
                break
            except timeout:
                if retry == 3:
                    print "[%s] get URL:%s failed: Read Timeout" %(threading.current_thread().name, url)
                    self.timeoutTask += 1
                    self.manager.pushTask("URLFetchTask", url)
                    break;
                else:
                    continue;
            except Exception, error:
                print "[%s] get URL:%s failed, exception:%s" %(threading.current_thread().name, url, error)
                self.manager.pushTask("URLFetchTask", url)
                break
        return url, URLContent


    def doWork(self, url, content=None):
        newUrl, content = self.getURLContent(url)
        if len(content) >0:
            self.manager.pushTask("ContenttotalTask", newUrl, content)
            self.manager.pushTask("ContentParserTask", newUrl, content)
            return True
        else:
            return False

    def TaskType(self):
        return "URLFetchTask"

#worker for download HTML content
class ContentDownloadWorker(worker):
    #dir for store downloaded content, like css, html, image etc
    def __init__ (self, manager, dir):
        super(ContentDownloadWorker, self).__init__(manager)
        self.dir = dir

    #The purpose for this function is add "Name" for URL which aren't end with ".html", iE, http://xxx.com/view.php?aid=2210

    #Right now, only support below scenarios 
    def transformURL(self, url):
        if "view.php?aid" in url:
            #redirect to another html
            return url.replace("view.php?aid=", "") + ".html"
        elif url.endswith("/"): #directory
            return url + "index.html"
        else:
            return url

    #parse URL to get directory and filename
    def getDirectoryAndHtmlName(self, URL):
        pathList = re.sub(self.manager.webSite,"", URL).split("/")
        if pathList[-1] == None or "." not in pathList[-1]:
            print "Can't support such URL:%s right now, ignored.%s" %(URL)
            return None,None
        else: 
            path = ""
            for directory in pathList[1:-1]:
                path = path  + directory + "/"
            return path, pathList[-1]

    #Convert absolute links in content to relative link
    def transformContentLinks(self, content):
        #convert src="http://xxxx.com/path/index.html" to "src="/path/index.html"
        tmp = re.sub('src="' + self.manager.webSite, 'src="', content)
        #convert src="href="http://xxxx.com/path/index.html" to "href="/path/index/html"
        return re.sub('href="' + self.manager.webSite, 'href="', tmp)


    def doWork(self, url, content):
        newContent = self.transformContentLinks(content)
        path, targetFile = self.getDirectoryAndHtmlName(self.transformURL(url))
        if path and not os.path.isdir(self.dir + path):
            os.makedirs(self.dir + path)

        if targetFile:
            with open(self.dir + path+targetFile, "wb") as file:
                file.write(newContent)
            return True
        else:
            return False

    def TaskType(self):
        return "ContenttotalTask"



#worker for parse HTML Content            
class ContentParserWorker(worker):
    class MiME:
        def parseContent(self, URL, content):
            pass;

    class CSSMIME(MiME):
        #Actually need parse css conent, right now, just pass
        def parseContent(self, URL, content):
            pattern = re.compile(r'url\("(.*?)"\)')
            urlList = pattern.findall(content)
            if len(urlList):
                subURLs = map(lambda x, y = URL:  x if x.startswith("http") else urlparse.urljoin(y, x), urlList)

                 #Ignore https/mails link
                subURLsList = [x for x in subURLs if "https" not in x and "mailto" not in x]
                return subURLsList
            else:
                return None


    class URLMIME(MiME):
        def parseContent(self, URL, content):
            urlList = []
            #soup = BeautifulSoup(content.decode('gb2312','ignore'), "lxml")
            soup = BeautifulSoup(content, "lxml")
            for img in soup.find_all(['img', 'script']):
                tmp = img.get('src')
                if tmp: 
                    urlList.append(tmp)

            for link in soup.find_all(['a', 'link']):
                tmp = link.get('href')
                if tmp:     
                    urlList.append(tmp)

            if len(urlList):
                subURLs = map(lambda x, y = URL:  x if x.startswith("http") else urlparse.urljoin(y, x), urlList)

                 #Ignore https/mails link
                subURLsList = [x for x in subURLs if "https" not in x and "mailto" not in x]
                return subURLsList
            else:
                return None

    #Just handle picture, html, and CSS/Javascript
    def getURLType(self, URL):
        url = URL.lower()
        if url.endswith("png") or url.endswith("jpg") or url.endswith("gif"):
            return ContentParserWorker.MiME()
        elif url.endswith('js'):
              return ContentParserWorker.MiME()
        elif url.endswith("css"):
            return ContentParserWorker.CSSMIME()
        elif url.endswith("html"): #directory
            return ContentParserWorker.URLMIME()
        else:
            return None

    def parseContent(self, url, content):
        MIME = self.getURLType(url)
        if MIME:
            urlList = MIME.parseContent(url, content)
            if urlList:
                for url in urlList:
                    self.manager.pushTask("URLFetchTask", url)

    def doWork(self, url, content):
        if self.manager.isfetchChildRenURLs:
            if self.manager.isSpecifiedURLFetched:
                return True
            else:
                self.manager.isSpecifiedURLFetched = True
                self.parseContent(url, content)
                return True
        else:
            self.parseContent(url, content)
            return True

    def TaskType(self):
            return "ContentParserTask"