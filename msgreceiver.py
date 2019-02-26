class MsgReceiver:
    def __init__(self):
        self.oldMsg = ''

    def receive(self, s):
        try:
            msg = s.recv(9000)
        except:
            return False, []
        if not msg:
            return False, []
        self.oldMsg += msg
        if self.oldMsg != '' and self.oldMsg[-1] == '\n':
            ret = self.oldMsg
            self.oldMsg = ''
            return True, MsgReceiver.unpack(ret)
        else:
            return True, []


    # sometimes incoming messages can hold multiple requests
    # it is guaranteed that request have new line at the end
    # but some pools can send data with new lines in body
    @staticmethod
    def unpack(msg):
        tokens = msg.split('\n')
        ret = []
        saved = ''
        for token in tokens:
            if token == '':
                continue
            if saved.count('{') + token.count('{') == saved.count('}') + token.count('}'):
                ret.append(saved + token + '\n')
                saved = ''
            else:
                saved += token + '\n'  # preserve line breaks
        if len(saved) > 0:
            ret.append(saved + '\n')

        #if len(ret) > 1:  # TODO remove
        #    print '------------ unpack results:'
        #    for a in ret:
        #        print a.replace('\n', '')
        #    print '------------'

        return ret
