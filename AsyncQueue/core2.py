from threading import Thread, Lock, Condition
from uuid import uuid1
import heapq
import logging
import sys

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger('ymt.queue')

class Enum(set):
    def __getattr__(self, name):
        if name in self:
            return name
        raise AttributeError


STATES = Enum(['PENDING', 'STARTED', 'COMPLETE', 'CANCELLED'])


class Status(object):
    def __init__(self):
        self.state  = STATES.PENDING
        self.result = None
        self.error  = None
        self.status = 'Pending in queue.'

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
        self.started     = {}
        
        self.id_gen      = uuid1

    def getHighestPriority(self):
        with self.lock:
            return self.items[0][0] if self.items else None

    def get(self):
        with self.lock:
            
            while len(self.items) == 0:
                self.condition.wait()

            item = heapq.heappop(self.items)
            
            priority, job_info = (item[0], item[2])
            
            job_info.status.state = STATES.STARTED
            self.started[job_info.uid] = job_info

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
            print len(self.started)
            del self.started[uid]
            print len(self.started)
            job_info.condition.notifyAll()

    def getStartedYetNotified(self):
        with self.lock:
            res = {}
            for k, v in self.started.iteritems():
                if v.job is not None:
                    res[k] = v
            return res

    def isEmpty(self):
        return (len(self.items) == 0)

    def cancel(self, uid):
        with self.lock:
            job_info = self.items_index[uid]
            #if job_info.status.state != STATES.STARTED:
            #    raise ValueError('Cannot cancel')
            
            job_info.status.state = STATES.CANCELLED
            job_info.status.setStatus('Job cancelled.')

    def isFinished(self, uid, timeout=None):
        with self.lock:
            job_info = self.items_index[uid]
            
            if (job_info.status.state != STATES.COMPLETE and job_info.status.state != STATES.CANCELLED):
                job_info.condition.wait(timeout)
                
                return (job_info.status.state == STATES.COMPLETE or job_info.status.state == STATES.CANCELLED, 
                        job_info.status.getStatus())
        
        return (True, job_info.status.getStatus())

    def getResult(self, uid):
        job_info = self.items_index[uid]
        
        assert (job_info.status.state == STATES.COMPLETE or job_info.status.state == STATES.CANCELLED), 'Job (id=%s) not complete yet' % uid

        return (job_info.status.result, job_info.status.error)


class Worker(Thread):
    def __init__(self, name, buf, *args, **kwargs):
        Thread.__init__(self, *args, **kwargs)
        
        self.name   = name
        self.buffer = buf

        self.daemon = True

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


class BufferWithWorker(object):

    def __init__(self, worker_count):
        self.buf          = Buffer()
        self.worker_count = worker_count
        self.workers      = []
        self.open         = True

        for count in xrange(1, worker_count + 1):
            self.workers.append(Worker('Worker-%s' % count, self.buf))
            self.workers[-1].start()

    def put(self, job, priority):
        if not self.open:
            raise ValueError('Already terminating')
        return self.buf.put(job, priority)

    def isFinished(self, uid, timeout=None):
        return self.buf.isFinished(uid, timeout)

    def getResult(self, uid):
        return self.buf.getResult(uid)

    def waitUntilAllDone(self):
        self.exitGracefully(after_current=False)

    def exitGracefully(self, after_current=True, timeout=None):
        self.open = False

        if after_current:
            priority = self.buf.getHighestPriority() - 1
        else:
            priority = sys.maxint

        for _ in xrange(self.worker_count):
            self.buf.put(None, priority)

        for worker in self.workers:
            worker.join(timeout)
            if worker.isAlive():
                break

        remains = self.buf.getStartedYetNotified().keys()
        if remains:
            logger.debug('Some in current. Killing %s', len(remains))
        while not self.buf.isEmpty():
            _, job_info = self.buf.get()
            remains.append(job_info.uid)

        for uid in remains:
            self.buf.cancel(uid)


class ProgressWriter(object):
    def write(self, msg, *args):
        logger.debug(msg, *args)


# ---------------------------------------------------

# Input  : status + args/kwargs
# Output : FuncResult(result, error)
