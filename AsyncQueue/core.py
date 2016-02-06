from threading import Thread, Lock, Condition
from Queue import Queue
import heapq
import time
from uuid import uuid1
import logging
 
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger('ymt')

class Enum(set):
    def __getattr__(self, name):
        if name in self:
            return name
        raise AttributeError

class Worker(Thread):

    def __init__(self, name, holder, *a, **ka):
        Thread.__init__(self, *a, **ka)
        self._name   = name
        self._holder = holder

    def getName(self):
        return self._name
    
    def getHolder(self):
        return self._holder

    def run(self):
        while True:
            uid, job = self._holder.get()
            
            if job.getName() == 'END': 
                break

            if job.getStatus() == Job.STATE_CANCELLED:
                logger.debug('Ignore %s as it is cancelled', job.getName())    
            else:
                logger.debug('Working on %s', job.getName())
                job.perform()
                job.setStatus(Job.STATE_DONE)
            
            self._holder.notifyDone(uid)

class Job(object):
    
    STATE_PENDING   = 0
    STATE_STARTED   = 1
    STATE_DONE      = 2
    STATE_CANCELLED = 3

    def __init__(self, name):
        self._name   = name
        self._status = self.STATE_PENDING

    def getName(self):
        return self._name

    def getStatus(self):
        return self._status

    def perform(self):
        time.sleep(2)

    def setStatus(self, status):
        self._status = status

    def __repr__(self):
        return 'Job (%s)' % self._name

class Holder(object):
    def __init__(self):
        
        self.lock      = Lock()
        self.condition = Condition(self.lock)
        
        self.items     = []
        self.places    = {}
        
        self.id_gen    = uuid1

    def get(self):
        with self.lock:
            if len(self.items) == 0:
                self.condition.wait()
            item = heapq.heappop(self.items)
            # if cancelled??
            return item[1]

    def put(self, item, priority):
        with self.lock:
            
            uid = str(self.id_gen())

            heapq.heappush(self.items, (priority, (uid, item)))
            self.places[uid] = (item, Condition(self.lock))
            
            if len(self.items) == 1:
                self.condition.notify()
            
            return uid

    def notifyDone(self, uid):
        with self.lock:
            print ('notif for', self.places[uid][0])
            self.places[uid][1].notifyAll()

    def cancel(self, uid):
        with self.lock:
            self.places[uid][0].setStatus(Job.STATE_CANCELLED)

    def isFinished(self, uid):
        with self.lock:
            if (not self.places[uid][0].getStatus() == Job.STATE_DONE and
                not self.places[uid][0].getStatus() == Job.STATE_CANCELLED):
                self.places[uid][1].wait()
        return True

    def getStatus(self, uid):
        return self.places[uid][0].getStatus()

holder = Holder()
worker = Worker('Worker', holder)
#worker.start()

N, t = 10, []
for i in xrange(N):
    uid = holder.put(Job('abc_%s' % i), 20-i)
    t.append((i, uid))

holder.cancel(t[4][1])
holder.cancel(t[5][1])
holder.cancel(t[6][1])

holder.put(Job('END'), 30)

logger.debug('Submitted %s jobs.', N)

class Waiter(Thread):

    def __init__(self, name, h, t, *a, **ka):
        Thread.__init__(self, *a, **ka)
        self.name = name
        self.h    = h
        self.t    = t
    def run(self):
        for i in reversed(t):
            logger.debug('%s learns finished => abc_%s - %s/%s', self.name,  i[0], holder.isFinished(i[1]), holder.getStatus(i[1]))

wa1 = Waiter('WA1', holder, t)

#wa1.start()

e = Enum(['DOG', 'CAT', 'BIRD'])

x = e.DOG

print x==e.DOG


