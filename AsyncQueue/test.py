from threading import Thread
import time


class A(Thread):
    def __init__(self, *args, **kwargs):
        Thread.__init__(self, *args, **kwargs)
        self.daemon = True

    def run(self):
        c = 0
        
        while True:
            if c == 20:
                break
            
            print 'hello'
            time.sleep(2)
            
            c +=1


if __name__ == '__main__':
    try:
        a = A()
        a.start()

        while a.isAlive():
            a.join(2)
    except KeyboardInterrupt:
        print 'waiting to quit'

        if a.isAlive():
            a.join(3)

        if a.isAlive():
            print 'still working so terminate abruptly'
