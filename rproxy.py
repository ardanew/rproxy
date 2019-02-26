from log import log, init_log
from sys import argv
from default_settings import DefaultSettings, ProxySettings
from db import Db
from socket import *
from select import select
from pig import Pig
from repeater import  Repeater
from typing import List
from stratumsocket import StratumSocket
import errno
from emulation import SocketPoolThread, SocketPigsThread
from threadbase import SelectThreadBase
import time


release = True


class RProxy(SelectThreadBase):
    def __init__(self):
        SelectThreadBase.__init__(self)
        self.proxy_settings = ProxySettings()
        self.proxy_settings.read_settings()
        init_log(self.proxy_settings.log_file_name)
        self.defaults = DefaultSettings()
        self.db = None
        self.proxy_socket = socket(AF_INET, SOCK_STREAM)
        if release:
            self.proxy_address = (self.proxy_settings.proxy_address, self.proxy_settings.proxy_port)
            self.default_settings_file = 'default_settings.json'
        else:
            self.proxy_address = ('127.0.0.1', 9898)
            self.default_settings_file = 'debug_settings.json'
        self.queues = [[], [], []]  # type: List[List[StratumSocket], List[StratumSocket], List[StratumSocket]]
        self.pigs = []  # type: List[Pig]
        self.repeaters_to_connect = []  # type: List[Repeater]
        self.last_update_time = time.time()

    def init(self, db_name):
        self.defaults.read_settings(self.default_settings_file)
        self.db = Db(db_name)
        self.db.connect()

    def start_listen(self):
        log('Proxy : start listening...')
        self.proxy_socket.bind(self.proxy_address)
        self.proxy_socket.listen(50)
        self.proxy_socket.setblocking(False)
        self.queues[0].append(self.proxy_socket)

    def accept_new_pig(self):
        self.defaults.read_settings(self.default_settings_file)  # get defaults for new pig
        s, address = self.proxy_socket.accept()
        pig = Pig(s, address, self.defaults)
        self.pigs.append(pig)
        self.queues[0].append(pig)  # always listen for messages from pig
        pig.pig_id = self.db.on_pig_accepted(address, self.defaults)
        log('Proxy : new pig' + str(pig.pig_id) + ' connected from ' + str(address))
        self.repeaters_to_connect.append(pig.repeater)  # schedule connect in connect_repeaters()

    def connect_repeaters(self):
        to_remove = []  # type: List[Repeater]
        for repeater in self.repeaters_to_connect:
            conn_res = repeater.socket.connect_ex((self.defaults.pool_address, self.defaults.pool_port))
            # errno.WSAEWOULDBLOCK(windows) = 10035, EALREADY(linux) = 114?
            if conn_res in (errno.EINPROGRESS, errno.EWOULDBLOCK, errno.EALREADY):  # retry later
                continue

            to_remove.append(repeater)
            if conn_res == errno.EISCONN or conn_res == 0:  # connected
                pool_address = (repeater.defaults.pool_address, repeater.defaults.pool_port)
                self.db.on_repeater_connected(repeater.pig.address, pool_address)
                self.queues[0].append(repeater)  # start receiving messages from pool
                repeater.on_connected(self.queues)  # start send pending messages if any
            else:  # error
                repeater.pig.disconnect(self.queues)
                self.db.on_pig_disconnected(repeater.pig.address)
            for removing in to_remove:
                self.repeaters_to_connect.remove(removing)

    def do_ping(self):
        for pig in self.pigs:
            pig.repeater.do_ping(self.queues)

    def refresh_from_db(self):
        rows = self.db.get_all_pigs()
        for row in rows:
            pig_addr = row[2]
            pig_port = row[3]
            write_log = row[4] != 0
            mute_notifies = row[5] != 0
            kill = row[7] != 0
            pig = next((x for x in self.pigs if x.address == (pig_addr, pig_port)), None)
            if pig == None:
                continue
            pig.defaults.write_log = write_log
            pig.defaults.mute_notifies = mute_notifies
            pig.repeater.defaults.write_log = write_log
            pig.repeater.defaults.mute_notifies = mute_notifies
            if kill:
                log('Manually killing pig ' + str(pig))
                pig.disconnect(self.queues)
                self.db.on_pig_disconnected(pig.address)

    def update_statistics(self):
        for pig in self.pigs:
            try:
                self.db.update_statistics(pig.address, pig.total_notifies, pig.repeater.total_submits,
                                          pig.failed_submits)
            except:
                continue
        self.db.do_commit_after_update_statistics()

    def main_loop(self):
        r, w, _ = select(self.queues[0], self.queues[1], self.queues[2], 0.4)

        current_time = time.time()
        if current_time - self.last_update_time > self.proxy_settings.update_timeout_seconds:  # update
            self.refresh_from_db()
            self.update_statistics()
            self.last_update_time = current_time

        self.connect_repeaters()  # check if it is required to connect some repeater(s)
        self.do_ping()  # schedule send 'ping' to pool(s)

        for s in w:
            ok, self.queues = s.send_messages(self.queues)
            if not ok:
                if s in self.pigs:
                    s.disconnect(self.queues)
                    self.db.on_pig_disconnected(s.address)
                else:  # repeater
                    s.pig.disconnect(self.queues)
                    self.db.on_pig_disconnected(s.pig.address)
        for s in r:
            if s == self.proxy_socket:
                self.accept_new_pig()
            else:  # message(s) from pig or receiver
                ok, messages = s.receive()
                if ok:
                    if s in self.pigs:  # type: Pig
                        messages = s.repeater.modify_messages(messages)
                        s.repeater.add_messages(messages, self.queues)
                    else:  # type: Repeater
                        messages = s.pig.modify_messages(messages)
                        s.pig.add_messages(messages, self.queues)
                else:
                    if s in self.pigs:
                        s.disconnect(self.queues)
                        self.db.on_pig_disconnected(s.address)
                    else:
                        s.pig.disconnect(self.queues)
                        self.db.on_pig_disconnected(s.pig.address)

    def do(self):
        self.start_listen()
        while not self.stopped.wait(0):
            self.main_loop()


if __name__ == '__main__':
    r_proxy = RProxy()
    db_name = 'rproxy.sqlite'
    if len(argv) < 2:
        log('Database name not specified in parameters, using name ' + db_name)
    else:
        db_name = argv[1]
    r_proxy.init(db_name)
    r_proxy.start()

    if not release:
        poolThread = SocketPoolThread((r_proxy.defaults.pool_address, r_proxy.defaults.pool_port),
            r_proxy.defaults.pool_login, r_proxy.defaults.pool_password)
        poolThread.start()
        pigsThread = SocketPigsThread(r_proxy.proxy_address)
        pigsThread.start()
    else:
        poolThread = None
        pigsThread = None

    r_proxy.do()
    raw_input()
    r_proxy.stop()

    if not release:
        poolThread.stop()
        pigsThread.stop()
