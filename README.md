## python_spider
### 0. Can directly run at Python2.7 on windows.
### 1. The program is a demo for study python.Functionality:  fetch/clone HTML/Website.
### 2. The demo include manager part and worker part:
  #### 2.1: spiderManager take responsibility for manager work queue and monitor woker threads status
  #### 2.2: spiderWorker include: URLFetchWorker, ContentParserWorker and ContentDownloadWorker
    URLFetchWorker: thread for GET URL content from Internet, support cookies(only for windows chrome).
    ContentParserWorker: thread for parse HTTP body(support html, css) and output: URL/Image/Script links
    ContentDownloadWorker: write content body to local.
  2.3 You can also defind a new worker.
3. Consider performance, Each worker is a thread and a coroutine (by Eventlet).
4. Todo: Consider anti-spider mechanism and performance, the program could be run at server machines.
   Breif solution is introduce master/slave spiderManager, tasks delivery to master manager firstly and
   master manager delivery tasks to slaves base on load banlance algorithm.
  
