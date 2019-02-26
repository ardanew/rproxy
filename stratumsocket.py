from msgreceiver import MsgReceiver
import Queue
from log import *
from socket import socket
from typing import Tuple, List


class StratumSocket(object):
    def __init__(self, s, address):  # type: (socket, Tuple[str, int])->None
        self.socket = s  # type: socket
        self.address = address
        self.msgsToSend = Queue.Queue()
        self.msgReceiver = MsgReceiver()

    def set_blocking(self, on):
        if on:
            self.socket.setblocking(1)
        else:
            self.socket.setblocking(0)

    def remove(self, queues):  # remove socket from three select queues
        for q in queues:
            if self in q:
                q.remove(self)

    def __str__(self):
        if self.socket is not None:
            return self.__class__.__name__ + str(self.address) + ' qsz=' + str(self.msgsToSend.qsize())
        else:
            return self.__class__.__name__ + str(('Disconnected', 0)) + str(self.msgsToSend.qsize())

    def fileno(self):  # to use in select()
        return self.socket.fileno()

    def receive(self):
        ok, messages = self.msgReceiver.receive(self.socket)
        if ok:
            for msg in messages:
                self.log_received_message(msg)
        return ok, messages

    def add_messages(self, messages, queues):
        for msg in messages:
            self.msgsToSend.put(msg)
        write_queue = queues[1]
        if self not in write_queue:
            write_queue.append(self)
        return queues

    def send_messages(self, queues): # type: (List[List[StratumSocket], List[StratumSocket], List[StratumSocket]])->Tuple[bool, List[List[StratumSocket], List[StratumSocket], List[StratumSocket]]]
        write_queue = queues[1]
        while not self.msgsToSend.empty():
            msg = self.msgsToSend.get_nowait()
            try:
                self.socket.send(msg)
            except Exception as e:
                log('Exception in send_messages, e = ' + str(e))
                for q in queues:
                    if self in q:
                        q.remove(self)
                return False, queues  # TODO if cannot send must leave message in queue
            else:
                self.log_sent_message(msg)
        write_queue.remove(self)
        return True, queues

    def log_sent_message(self, _):  # derived classes should override this
        print '_?_'

    def log_received_message(self, _):  # derived classes should override this
        print '_?_'

    def on_disconnected(self):
        print 'DERIVED CLASSES MUST OVERRIDE THIS'


class AuthorizedStratumSocket(StratumSocket):  # bee for SocketPoolThread
    def __init__(self, s, address):
        StratumSocket.__init__(self, s, address)
        self.authorized = False
        self.subscribed = False
