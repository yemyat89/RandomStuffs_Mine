import threading
import heapq
import time

class MyQueue(object):

    def __init__(self):
        self._items = []
        self.l = threading.Lock()
        self.c = threading.Condition(self.l)

    def get(self):
        with self.l:
            if len(self._items) == 0:
                self.c.wait()
            
            item = heapq.heappop(self._items)
            return item[-1]

    def put(self, priority, item):
        with self.l:
            heapq.heappush(self._items, (priority, len(self._items), item))
            if len(self._items) == 1:
                print 'pop'
                self.c.notifyAll()


class Consumer(threading.Thread):
    def __init__(self, q, *a, **ka):
        threading.Thread.__init__(self, *a, **ka)
        self.q = q
    def run(self):
        while True:
            time.sleep(3)
            x = 'pp'
            #x = self.q.get()
            #if x == 'END':
            #    break
            print x


q = MyQueue()

q.put(9, 'a9')
q.put(2, 'a2')
q.put(11, 'a11')
q.put(8, 'a8.1')
q.put(8, 'a8.2')
q.put(8, 'a8.4')
q.put(8, 'a8.3')
q.put(8, 'a8.5')
q.put(4, 'a4')
q.put(7, 'a7')
q.put(3, 'a3')
q.put(1, 'a1')

c = Consumer(q)
c.run()

print 'asd'

time.sleep(5)

q.put(100, 'END')

c.join()