from threading import Thread, Lock, Condition
from uuid import uuid1
import heapq
import time
import logging

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger('ymt.queue')

class Enum(set):
    def __getattr__(self, name):
        if name in self:
            return name
        raise AttributeError


STATES = Enum(['PENDING', 'STARTED', 'COMPLETE'])


class Status(object):
    def __init__(self):
        self.state  = STATES.PENDING
        self.result = None
        self.error  = None
        self.status = ''

    def getStatus(self):
        return self.status

    def setStatus(self, msg):
        self.status = msg


class Job(object):
    def __init__(self, func, func_args, func_kwargs):
        self.function = func
        self.args     = func_args
        self.kwargs   = func_kwargs

class JobInfo(object):
    def __init__(self, uid, job, condition, status):
        self.uid       = uid
        self.job       = job
        self.condition = condition
        self.status    = status


class Buffer(object):
    def __init__(self):

        self._counter    = 0
        
        self.lock        = Lock()
        self.condition   = Condition(self.lock)
        
        self.items       = []
        self.items_index = {}
        
        self.id_gen      = uuid1

    def get(self):
        with self.lock:
            if len(self.items) == 0:
                self.condition.wait()
            
            item     = heapq.heappop(self.items)
            job_info = item[2]
            
            job_info.status.state = STATES.STARTED

            return job_info

    def put(self, job, priority):
        with self.lock:
            
            uid       = str(self.id_gen())
            condition = Condition(self.lock)
            status    = Status()

            job_info  = JobInfo(uid, job, condition, status)

            heapq.heappush(self.items, (priority, self._counter, job_info))
            self.items_index[uid] = job_info
            
            if len(self.items) == 1:
                self.condition.notify()

            self._counter += 1
            
            return uid

    def notifyDone(self, uid):
        with self.lock:
            job_info = self.items_index[uid]
            job_info.status.state = STATES.COMPLETE
            job_info.condition.notifyAll()

    def isEmpty(self):
        return (len(self.items) == 0)

    def isFinished(self, uid, timeout=None):
        with self.lock:
            job_info = self.items_index[uid]
            if job_info.status.state != STATES.COMPLETE:
                job_info.condition.wait(timeout)
                return (job_info.status.state == STATES.COMPLETE, job_info.status.getStatus())
        return (True, job_info.status.getStatus())

    def getResult(self, uid):
        job_info = self.items_index[uid]
        
        assert job_info.status.state == STATES.COMPLETE, 'Job (id=%s) not complete yet' % uid

        return (job_info.status.result, job_info.status.error)


class Worker(Thread):
    def __init__(self, name, buffer, *args, **kwargs):
        Thread.__init__(self, *args, **kwargs)
        
        self.name   = name
        self.buffer = buffer

    def run(self):
        while True:
            job_info = self.buffer.get()
            job      = job_info.job
            status   = job_info.status

            if job is None:
                break

            try:
                status.result, status.error = job.function(status, *job.args, **job.kwargs)
            except:
                logger.debug('Something went wrong', exc_info=True)
            
            self.buffer.notifyDone(job_info.uid)


# Input  : status + args/kwargs
# Output : (result, error)

def addNumber(status, a, b, *args, **kwargs):
    
    status.setStatus('Installing Part A')
    time.sleep(3)
    status.setStatus('Installing Part B')
    time.sleep(3)
    status.setStatus('Combining Part A and B')
    time.sleep(3)
    status.setStatus('Calculating')
    time.sleep(3)

    result = a + b
    error  = None
    
    return (result, error)


buf = Buffer()

worker = Worker('Worker', buf)
worker.start()

reqs = []
for i in xrange(10, 16):
    job = Job(addNumber, (15, i), {})
    reqs.append(buf.put(job, 100))

buf.put(None, 100)

for req in reqs:
    logger.debug('Check result of "%s"', req)
    
    log_buf = set()
    done, msg = buf.isFinished(req, 1)
    while not done:
        if msg not in log_buf:
            logger.debug(' ... %s', msg)
        log_buf.add(msg)
        done, msg = buf.isFinished(req, 1)

    res = buf.getResult(req)
    logger.debug('Result is %s', str(res))

