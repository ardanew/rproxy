from stratumsocket import StratumSocket
#from pig import Pig
from typing import List, Tuple
from socket import socket
from default_settings import DefaultSettings
import json
from log import log
import time


class Repeater(StratumSocket):
    def __init__(self, pig, socket, address, defaults):  # type: (Pig, socket, Tuple[str, int], DefaultSettings)->None
        StratumSocket.__init__(self, socket, address)
        self.pig = pig
        self.set_blocking(False)
        self.connected = False
        self.defaults = defaults  # type: DefaultSettings
        self.last_message_from_pool_time = 0
        self.total_submits = 0
        self.last_submit_id = -1

    def __str__(self):
        return 'Repeater(' + str(self.pig.pig_id) + ' ' + str(self.address) + ', ' + str(self.pig) + ')'

    def log(self, message):
        if self.defaults.write_log:
            log(message)

    def on_connected(self, queues):  # type: (List[List[StratumSocket], List[StratumSocket], List[StratumSocket]])->None
        self.connected = True
        self.update_time()
        if not self.msgsToSend.empty() and self not in queues[1]:
            queues[1].append(self)

    def update_time(self):
        self.last_message_from_pool_time = time.clock() # datetime.now().time()

    def do_ping(self, queues):
        current_time = time.clock()
        if self.connected and current_time > self.last_message_from_pool_time + 60:
            self.log(str(self) + ' pinging')
            self.add_messages(['\n'], queues)
        self.update_time()

    def receive(self):
        return StratumSocket.receive(self)

    def add_messages(self, messages, queues):
        for msg in messages:
            self.msgsToSend.put(msg)
        if self.connected:
            write_queue = queues[1]
            if self not in write_queue:
                write_queue.append(self)
        return queues

    def modify_messages(self, messages):  # type: (List[str])->List[str]
        self.update_time()  # message from pig was received, update 'last time' for ping
        modified = []  # type: List[str]
        for message in messages:
            try:
                parsed = json.loads(message)
                if 'method' not in parsed:
                    modified.append(message)
                    continue
                if parsed['method'] == 'mining.authorize':
                    parsed['params'][0] = self.defaults.pool_login
                    parsed['params'][1] = self.defaults.pool_password
                    modified.append(json.dumps(parsed) + '\n')
                else:
                    if parsed['method'] == 'mining.submit':  # update statistics
                        if 'id' in parsed:
                            self.last_submit_id = parsed['id']
                        self.total_submits += 1
                    modified.append(message)
            except Exception as e:
                log('Exception while parsing message ' + message)
                modified.append(message)
        return modified

    def log_sent_message(self, msg):
        self.log('s   to_pool id_' + str(self.pig.pig_id) + ' : <- ' + str(msg).replace('\n', ''))

    def log_received_message(self, msg):
        self.log('r from_pool id_' + str(self.pig.pig_id) + ' : -> ' + str(msg).replace('\n', ''))
