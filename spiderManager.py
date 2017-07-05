from apscheduler.schedulers.background import BackgroundScheduler

import threading
import Queue


class SpiderManager():

    def __init__ (self, webSite, isfetchChildRenURLs=False):
        self.urlMap = set([])

        self.TaskQueue = {
            'URLFetchTask': Queue.Queue(1000),
            'ContenttotalTask': Queue.Queue(1000),
            'ContentParserTask': Queue.Queue(1000),
        }

        self.isSpecifiedURLFetched = False;
        self.exitCounter = 0
        self.exit = False
        self.webSite = webSite
        self.isfetchChildRenURLs = isfetchChildRenURLs
        self.threadArray = []
        self.scheduler = BackgroundScheduler()
        self.scheduler.add_job(self.checkWorkerStatus, 'interval', seconds=5)
        self.scheduler.add_job(self.debugWorker, 'interval',seconds=15)

        #Add URL to queue
        self.pushTask("URLFetchTask", webSite)
        self.scheduler.start()


    #this fuction could be refactor in furture if need.
    def validateURL(self, URL):
        if self.webSite not in URL: 
            return False
        elif URL in self.urlMap:
            return False
        else:
            return True

    def pushTask(self, TaskType, URL, Content=None):
        if TaskType == 'URLFetchTask':
            if self.validateURL(URL):
                self.TaskQueue[TaskType].put((URL, Content))
                self.urlMap.add(URL)
                return True
            else:
                return False
        else:
            self.TaskQueue[TaskType].put((URL, Content))
            return True

    def getTask(self, TaskType):
        if not self.TaskQueue[TaskType].empty():
            return self.TaskQueue[TaskType].get()
        else:
            return None

    def taskDone(self,taskType):
        self.TaskQueue[taskType].task_done()

    def join(self):
        for thread in self.threadArray:
            thread.join()

    #check each work onging task, if the sum for onging task is 0
    #exitCounter add 1, if exitCounter equal 3 ,then exit.
    def checkWorkerStatus(self):
        runningTask = 0
        for thread in self.threadArray:
            runningTask += thread.getRuningTaskNumber()
        if runningTask != 0:
            self.exitCounter = 0
        else:
            self.exitCounter += 1
            if self.exitCounter == 3:
                print "Finished Task, Process Exit"
                self.scheduler.shutdown(wait=False)
                self.exit = True


    def debugWorker(self):
        print "URL Queue Size:[%d], Download Queue Size:[%d], Parse Queue size: [%d], Total Request URLs: [%d]" \
            %(self.TaskQueue["URLFetchTask"].qsize(), \
            self.TaskQueue["ContenttotalTask"].qsize(), \
            self.TaskQueue["ContentParserTask"].qsize(), \
            len(self.urlMap))  
        for thread in self.threadArray:
            print "[%s] total task[%d], success Task[%d], timeoutTask[%d], onGongTask[%d]" \
                     %(thread, thread.totalTask, thread.successTask, thread.timeoutTask, thread.getRuningTaskNumber())