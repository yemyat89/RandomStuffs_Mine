from threading import Thread, Lock, Condition
from uuid import uuid1
import heapq
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
        self.status = 'HOLA'

    def getStatus(self):
        return self.status

    def setStatus(self, msg):
        self.status = msg


class Job(object):
    def __init__(self, func, func_args, func_kwargs):
        self.function = func
        self.args     = func_args
        self.kwargs   = func_kwargs

class FuncResult(object):
    def __init__(self, result=None, error=None):
        self.result = result
        self.error  = error


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
            
            while len(self.items) == 0:
                self.condition.wait()

            item = heapq.heappop(self.items)
            
            priority, job_info = (item[0], item[2])
            
            job_info.status.state = STATES.STARTED

            return (priority, job_info)

    def put(self, job, priority):
        with self.lock:
            
            uid       = str(self.id_gen())
            condition = Condition(self.lock)
            status    = Status()

            job_info  = JobInfo(uid, job, condition, status)

            heapq.heappush(self.items, (priority, self._counter, job_info))
            self.items_index[uid] = job_info
            
            if len(self.items) == 1:
                self.condition.notifyAll()

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
    def __init__(self, name, buf, *args, **kwargs):
        Thread.__init__(self, *args, **kwargs)
        
        self.name   = name
        self.buffer = buf

    def run(self):
        while True:
            priority, job_info = self.buffer.get()

            job      = job_info.job
            status   = job_info.status

            if job is None:
                logger.debug('"%s" is shutting down.', self.name)

                self.buffer.put(job, priority)
                break

            try:
                res = job.function(status, *job.args, **job.kwargs)
                
                if isinstance(res, FuncResult):
                    status.result = res.result
                    status.error  = res.error
                else:
                    logger.debug('Provided function does not return required type. '
                                 'Parsing into default form')
                    
                    status.result = res
                    status.error  = None
            except:
                logger.debug('Something went wrong', exc_info=True)
            
            self.buffer.notifyDone(job_info.uid)


def createBufferWithWorker(worker_count=1):
    buf = Buffer()

    for count in xrange(1, worker_count + 1):
        worker = Worker('Worker-%s' % count, buf)
        worker.start()

    return buf


class ProgressWriter(object):
    def write(self, msg, *args):
        logger.debug(msg, *args)


# ---------------------------------------------------

# Input  : status + args/kwargs
# Output : FuncResult(result, error)
