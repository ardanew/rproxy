from stratumsocket import StratumSocket
from log import log
from typing import List
from repeater import Repeater
from socket import *
from default_settings import DefaultSettings
import json


class Pig(StratumSocket):
    def __init__(self, socket, address, defaults):
        StratumSocket.__init__(self, socket, address)
        self.set_blocking(False)
        self.defaults = defaults  # type: DefaultSettings
        self.repeater = self.create_repeater(defaults)  # type: Repeater
        self.total_notifies = 0
        self.failed_submits = 0
        self.pig_id = -1

    def __str__(self):
        return 'Pig' + str(self.pig_id) + ' ' +  str(self.address)

    def log(self, message):
        if self.defaults.write_log:
            log(message)

    def create_repeater(self, defaults):  # type: (DefaultSettings)->Repeater
        s = socket(AF_INET, SOCK_STREAM)
        address = ('0.0.0.0', 0)
        repeater = Repeater(self, s, address, defaults)
        return repeater

    def disconnect(self, queues):  # type: (List[List[StratumSocket], List[StratumSocket], List[StratumSocket]])->None
        self.remove(queues)
        try:
            self.socket.close()
        except Exception as e:
            self.log(str(self) + ' exception when doing socket.close(), e = ' + e.message)
        else:
            self.log(str(self) + ' disconnected')

        self.repeater.remove(queues)
        try:
            self.repeater.socket.close()
        except Exception as e:
            self.log(str(self.repeater) + ' exception when doing repeater.socket.close(), e = ' + e.message)
        else:
            self.log(str(self.repeater) + ' disconnected')

    def modify_messages(self, messages):  # type: (List[str]) -> List[str]
        modified = []  # type: List[str]
        for message in messages:
            try:
                parsed = json.loads(message)
                if 'method' in parsed and parsed['method'] == 'mining.notify':
                    self.total_notifies += 1  # update statistics
                    if self.defaults.mute_notifies:
                        continue
                if 'id' in parsed and parsed['id'] == self.repeater.last_submit_id:  # update statistics
                    if 'result' in parsed and not parsed['result']:
                        self.failed_submits += 1
                modified.append(message)
            except Exception as e:
                log('Exception while parsing message ' + message)
                modified.append(message)
        return modified

    def log_sent_message(self, msg):
        self.log('s    to_pig id_' + str(self.pig_id) + ' : -> ' + str(msg).replace('\n', ''))

    def log_received_message(self, msg):
        self.log('r  from_pig id_' + str(self.pig_id) + ' : <- ' + str(msg).replace('\n', ''))
