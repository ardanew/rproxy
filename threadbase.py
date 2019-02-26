from threading import Thread, Event


class SelectThreadBase(Thread):
    def __init__(self):
        Thread.__init__(self)
        self.stopped = Event()

    def add(self, pig):
        pass

    def stop(self):
        self.stopped.set()
        self.join()
