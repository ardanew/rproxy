from pig import *
import random
import select
import json
from socket import *
from threadbase import SelectThreadBase


class Emulation(object):
    def __init__(self):
        pass

    @staticmethod
    def random_hex(min_int, max_int):
        return "%0.2x" % random.randint(min_int, max_int)

    @staticmethod
    def build_submit(submit_id, job_id, time):
        extra_nonce2 = Emulation.random_hex(0x10000000, 0xffffffff)
        nonce = Emulation.random_hex(0x10000000, 0xffffffff)
        return '{"id":' + str(submit_id) + ',"method":"mining.submit","params":["testcript.nhdev","' \
               + str(job_id) + '","' + extra_nonce2 \
               + '","' + str(time) + '","' + nonce + '"]}\n'

    @staticmethod
    def build_subscribe():
        return '{"id":1,"method":"mining.subscribe","params":["NiceHash/1.0.0"]}\n'

    @staticmethod
    def build_auth():
        return '{"id":2,"method":"mining.authorize","params":["testcript.nhdev","d=1048576"]}\n'

    @staticmethod
    def construct_notification(job_id, passed_time):
        clear = False
        #if passed_time == 4.0:
        #    clear = True
        #else:
        #    clear = False
        return '{"id":null,"method":"mining.notify","params":["' + str(job_id) + \
               '","0b9bf20c137d0709b12b390021604932898b7c8c0022706c' \
               '0000000000000000","01000000010000000000000000000000' \
               '000000000000000000000000000000000000000000ffffffff45' \
               '03be6408fabe6d6d1711914f6ef9a2d1155f452fce3f9e4f3ce59' \
               'b6ee4545f99f48bb9c515fa90b20100000000000000",' \
               '"89fb0c2f736c7573682f00000000030efda54c000000001976a9147c1' \
               '54ed1dc59609e3d26abb2df2ea3d587cd8c4188ac000000000' \
               '00000002c6a4c2952534b424c4f434b3a64b946507d4ea2844538' \
               'e397dcf3c672f4b22aa3746d49798a03a92bb99aa35c000000' \
               '0000000000266a24aa21a9ed902f69bbf7d3190f0a7820fb12d0257' \
               '532a172ad66282acf7cf16fe22935a3c600000000",' \
               '["04436949c4b84eff5ecb2e400a1b4857bc86b002dd38da3700bcf95709cfc1f7",' \
               '"0f48493d841425cfe05b9d580cc46d6dde744bb7b4a985ff1057e4afddb78690",' \
               '"c868956b15a9839ad68a798c10526cbfccfa17939f663ffe047bc8eec5d17bd3",' \
               '"65e' \
               'a165351e0a10a8249320dacf6b85daf8235916f2c84b01599eb9245302d30",' \
               '"e8954c9ae89608d9b27bed33cf676cf82c3ef47' \
               'b2d535d61ef14e912fc20dbb1","fd206aff02a3a7f80f89f2bafe49f06c00e4d665b6268d5d8fd8eb7c8f607f71",' \
               '"3f526e0a' \
               '3638199b664d5efff0e4164334e7171dbf4911541226e6d45636ea71",' \
               '"55bfbd1966f5ee50a3793df4dcc4738e764cc333bd09' \
               '16aa3f5b03a8c422dc13","73e4cbfc1c4b9776c51a6f99e5b121071b322a77b0112995de776186b455957b",' \
               '"f616cfd2424eb' \
               'a7960bcef7ba92e1e66e28f7d08bcbfbc39e35906ee21eaac96",' \
               '"27801dafa1111b4ca182d42b672ea5f2d7d0840aae2eb970f' \
               'cf2ef0a92dc39d7",' \
               '"8c2c4ca393f1e32c231960fd5735e54832667c3551c3360c78602c9e70180b81"],' \
               '"20000000","17272d92","' + str(passed_time) + '",' + str(clear).lower() + ']}\n'


class AuthorizedStratumSocket(StratumSocket):  # bee for SocketPoolThread
    def __init__(self, s, address):
        StratumSocket.__init__(self, s, address)
        self.authorized = False
        self.subscribed = False


class SocketBeeEmul(AuthorizedStratumSocket):
    def log_sent_message(self, msg):
        pass
        #log('PoolEmul : -> ' + str(msg).replace('\n', ''))

    def log_received_message(self, _):
        pass


class SocketPoolThread(SelectThreadBase, StratumSocket):
    def __init__(self, pool_address, login, password):
        SelectThreadBase.__init__(self)
        s = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP)
        s.bind(pool_address)
        s.listen(20)
        StratumSocket.__init__(self, s, pool_address)
        self.set_blocking(False)
        self.queues = [[self.socket], [], []]
        self.job_id = 1
        self.login = login
        self.password = password
        self.unauthorized_bees = []  # unauthorized and unsubscribed bees

    def run(self):
        dt = 0.1
        passed_time = 0
        try:
            while not self.stopped.wait(0):
                r, w, _ = select.select(self.queues[0], self.queues[1], self.queues[2], dt)
                for s in w:
                    s.send_messages(self.queues)
                for s in r:
                    if s == self.socket:  # accept bee
                        client, address = self.socket.accept()
                        bee = SocketBeeEmul(client, address)
                        if bee not in self.queues[0]:
                            log('External pool : repeater connected ' + str(bee.address) + ', wait for auth/subscribe')
                            if bee not in self.unauthorized_bees:
                                self.unauthorized_bees.append(bee)  # TODO sometimes remove from here
                            self.queues[0].append(bee)
                    else:  # s == bee
                        ok, messages = s.receive()
                        if ok:
                            for msg in messages:
                                try:
                                    parsed = json.loads(msg)
                                except Exception as e:
                                    log('Pool : exception e = ' + str(e))
                                    continue
                                self.on_message_from_bee(s, parsed)
                        else:
                            log('External bool : bee disconnected ' + str(s))
                            for q in self.queues:
                                if s in q:
                                    q.remove(s)

                # each 5 seconds send notification to authorized bees
                # if SocketPigsThread.is_close(9.0, int(9.0)):
                #    pass
                if passed_time > 5:
                    for s in self.queues[0]:
                        if s == self.socket:
                            continue
                        if s.authorized and s.subscribed:
                            notification = Emulation.construct_notification(self.job_id, int(passed_time))
                            self.job_id += 1
                            s.add_messages([notification], self.queues)
                            passed_time = 0
                passed_time += dt
        except Exception as e:
            log('SocketThreadPool ex = ' + str(e))
            return

    def on_message_from_bee(self, bee, parsed):
        if 'method' in parsed:
            if 'id' in parsed:
                if parsed['method'] == 'mining.authorize':
                    if 'params' in parsed and parsed['params'][0] == self.login \
                            and parsed['params'][1] == self.password:
                        bee.authorized = True
                        bee.add_messages(['{"id":' + str(parsed['id']) + ',"result":true,"error":null}\n'], self.queues)
                    else:
                        log(str(bee) + ' was failed auth, disconnecting bee...')
                        bee.on_disconnected()  # auth failed
                        for q in self.queues:
                            if bee in q:
                                q.remove(bee)
                elif parsed['method'] == 'mining.subscribe':
                    bee.subscribed = True
                    bee.add_messages(['{"id":' + str(parsed['id']) +
                                      ',"result":[[["mining.set_difficulty","1"],["mining.notify","1"]],'
                                      '"09650702643604",4],"error":null}\n'], self.queues)
                    bee.add_messages(['{"id":null,"method":"mining.set_difficulty","params":[1000000]}\n'], self.queues)
                elif parsed['method'] == 'mining.set_extranonce':
                    bee.add_messages(['{"id":' + str(parsed['id']) + ',"result":true,"error":null}\n'], self.queues)
                elif parsed['method'] == 'mining.submit':
                    bee.add_messages(['{"id":' + str(parsed['id']) + ',"result":true,"error":null}\n'], self.queues)

    @staticmethod
    def is_close(a, b, rel_tol=1e-09, abs_tol=0.0):
        return abs(a - b) <= max(rel_tol * max(abs(a), abs(b)), abs_tol)


class SocketPigEmul(StratumSocket):
    def log_sent_message(self, msg):
       pass
       # log('PigEmul : <- ' + str(msg).replace('\n', ''))

    def log_received_message(self, _):
        pass


class SocketPigsThread(SelectThreadBase):
    def __init__(self, proxy_address):
        SelectThreadBase.__init__(self)
        self.proxy_address = proxy_address
        self.queues = [[], [], []]
        self.tasks = {}  # pig, start time
        self.pigs_to_connect = 1
        self.last_failed_pig_time = 0

    def create_pig(self):
        pig = SocketPigEmul(socket(AF_INET, SOCK_STREAM, IPPROTO_TCP), self.proxy_address)
        pig.socket.connect(pig.address)
        pig.set_blocking(False)
        self.queues = pig.add_messages([Emulation.build_subscribe()], self.queues)
        return pig

    def need_to_create_pig(self, passed_time):
        if self.pigs_to_connect > 0:
            if passed_time >= 0.5:
                return True
            if passed_time < self.last_failed_pig_time + 10:  # recreate after 10 seconds
                return False
            return True
        return False

    @staticmethod
    def is_close(a, b, rel_tol=1e-03, abs_tol=0.0):
        return abs(a - b) <= max(rel_tol * max(abs(a), abs(b)), abs_tol)

    def kill_pig(self, pig, passed_time):
        for q in self.queues:
            if pig in q:
                q.remove(pig)
                self.last_failed_pig_time = passed_time
                self.pigs_to_connect += 1

    def run(self):
        passed_time = 0
        dt = 0.1

        while not self.stopped.wait(dt):
            if self.need_to_create_pig(passed_time):
                pig = None
                try:
                    pig = self.create_pig()
                except:
                    log('Emul: cannot create pig, creating later')
                    self.last_failed_pig_time = passed_time
                if pig:
                    if pig not in self.queues[0]:
                        self.queues[0].append(pig)
                    self.pigs_to_connect -= 1

            to_remove = []  # if must calculate - remove from tasks, add to to_write
            for pig in self.tasks:
                task_time = self.tasks[pig]
                if passed_time > 4 + task_time:
                    to_remove.append(pig)
                    if pig not in self.queues[1]:
                        # TODO: hardcoded 5
                        self.queues = pig.add_messages([Emulation.build_submit(5, int(passed_time), int(passed_time))],
                                         self.queues)
                        #self.to_write.append(pig)
            for pig in to_remove:
                del self.tasks[pig]

            if self.queues[0]:  # for Windows there must be at least one socket in select()
                r, w, _ = select.select(self.queues[0], self.queues[1], [], 0)
                for pig in w:
                    ok, self.queues = pig.send_messages(self.queues)
                    if not ok:
                        self.kill_pig(pig, passed_time)
                        continue

                for pig in r:
                    ok, messages = pig.receive()
                    if not ok:
                        self.kill_pig(pig, passed_time)
                        continue

                    for msg in messages:
                        parsed = json.loads(msg)
                        if 'method' in parsed:
                            if parsed['method'] == 'mining.notify':
                                self.tasks[pig] = passed_time

                        if 'id' in parsed:
                            if parsed['id'] == 1:  # subscribe, send 'auth'
                                if 'method' in parsed and parsed['method'] == 'mining.set_extranonce':
                                    self.queues = pig.add_messages(['{"id":' + str(parsed['id']) + ',"result":true,"error":null}\n'],
                                                     self.queues)
                                else:
                                    self.queues = pig.add_messages([Emulation.build_auth()], self.queues)

            passed_time += dt
