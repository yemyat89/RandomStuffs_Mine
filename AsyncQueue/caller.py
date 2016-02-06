from threading import Thread
from core2 import (createBufferWithWorker, Job, FuncResult, ProgressWriter)
import time
import logging

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger('ymt.caller')


class ProgressWriterFile(ProgressWriter):
    def __init__(self, filepath):
        self.path = filepath
        with open(self.path, 'a') as f:
            f.write('-------Started----------\n')

    def write(self, msg, *args):
        with open(self.path, 'a') as f:
            f.write('%s\n' % (msg % args))


class Waiter(Thread):
    def __init__(self, name, buf, req, progress_writer, *args, **kwargs):
        Thread.__init__(self, *args, **kwargs)
        self.name = name
        self.buf  = buf
        self.req  = req
        self.pgw  = progress_writer

    def run(self):
        self.pgw.write('[%s] Check request "%s"', self.name, str(req))

        log_buf = set()
        done, msg = self.buf.isFinished(self.req, 1)
        while not done:
            if msg not in log_buf:
                self.pgw.write('[%s] ... %s', self.name, msg)
            log_buf.add(msg)
            done, msg = buf.isFinished(self.req, 1)

        res = buf.getResult(self.req)
        self.pgw.write('[%s] Result is %s', self.name, str(res))


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
    
    return FuncResult(result=result, error=error)


buf = createBufferWithWorker(6)

reqs = []
for i in xrange(10, 16):
    job = Job(addNumber, (15, i), {})
    reqs.append(buf.put(job, 100))

# END request
buf.put(None, 100)

for i, req in enumerate(reqs):
    pgw = ProgressWriterFile('out/output_%s' % req)
    waiter = Waiter('Waiter_%s' % i, buf, req, pgw)
    waiter.start()
