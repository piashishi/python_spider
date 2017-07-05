from spiderManager import SpiderManager
from spiderWorker import URLFetchWorker, ContentParserWorker, ContentDownloadWorker

#webSite: spider object
#isfetchChildRenURLs: does spider sublinks, default is false, mean just fetch webiste and its links.
manager = SpiderManager(webSite="http://www.scu-ifc.org", isfetchChildRenURLs=False);

#URLFetchWorker for download URL from internet
for x in range(1):
    Worker = URLFetchWorker(manager, useCookies=False)
    manager.threadArray.append(Worker)
    Worker.start()

#ContentParserWorker for parse HTML and get subURL and subLinks
for x in range(1):
    Worker = ContentParserWorker(manager)
    manager.threadArray.append(Worker)
    Worker.start()

#ContentDownloadWorker for download HTML to local disk.
for x in range(1):
    Worker = ContentDownloadWorker(manager, dir="D:/scu/")
    manager.threadArray.append(Worker)
    Worker.start()


manager.join()