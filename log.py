import sys
from datetime import datetime

log_class = None

class Log:
    def __init__(self, file_name):
        self.log_file = open(file_name, 'w')

    def log(self, message):
        msg = str(datetime.now().time()) + ": " + str(message) + '\n'
        self.log_file.write(msg)
        self.log_file.flush()
        sys.stdout.write(msg)
        sys.stdout.flush()


def init_log(file_name):
    global log_class
    log_class = Log(file_name)


def log(message):
    global log_class
    log_class.log(message)
