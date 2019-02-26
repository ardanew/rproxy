import json
from log import log


class ProxySettings(object):
    def __init__(self):
        self.proxy_address = '0.0.0.0'
        self.proxy_port = 3333
        self.update_timeout_seconds = 30
        self.log_file_name = 'proxy.log'

    def read_settings(self):
        with open('proxy_settings.json') as settings_file:
            settings = json.load(settings_file)['proxy']
        self.proxy_address = settings['proxy_address']
        self.proxy_port = settings['proxy_port']
        self.log_file_name = settings['log_file_name']
        self.update_timeout_seconds = settings['update_timeout_seconds']


class DefaultSettings(object):
    def __init__(self):
        self.pool_address = ''  # type: str
        self.pool_port = 0  # type: int
        self.pool_login = ''
        self.pool_password = ''
        self.write_log = False  # default value for new pigs
        self.mute_notifies = False  # default value for new pigs

    def __str__(self):
        return str(self.pool_address) + ' ' + str(self.pool_port) + ' ' + str(self.write_log) + ' ' + str(self.mute_notifies)

    def read_settings(self, file_name):
        with open(file_name) as default_settings:
            settings = json.load(default_settings)['default_pool']
        if settings['address'] != self.pool_address or settings['port'] != self.pool_port or \
                settings['login'] != self.pool_login or settings['password'] != self.pool_password:

            log('old = ' + str(self))

            if self.pool_address == '':
                self.write_log = settings['write_log']
                self.mute_notifies = settings['mute_notifies']

            self.pool_address = settings['address']
            self.pool_port = settings['port']
            self.pool_login = settings['login']
            self.pool_password = settings['password']
            
            log('Configuration : new settings received from ' + file_name)

            log('new = ' + str(self))
            return True
        return False
