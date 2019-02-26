import sqlite3
from log import log
import os
from default_settings import DefaultSettings
from typing import Tuple


class Db(object):
    def __init__(self, db_name):
        self.db_name = db_name
        self.db = None
        self.cursor = None
        self.pig_id = 1

    def connect(self):
        if not os.path.isfile(self.db_name):
            self.create_db()
        else:
            log('Database : ' + self.db_name)
            self.db = sqlite3.connect(self.db_name)
            self.cursor = self.db.cursor()
            self.clear_db()

    def do_sql(self, query):
        log('SQL: ' + query)
        return self.cursor.execute(query)

    def clear_db(self):
        try:
            self.do_sql('DROP TABLE pigs')
        except sqlite3.OperationalError:
            pass
        self.do_sql('CREATE TABLE pigs (id integer, pool_addr text, pool_port integer, pig_addr text, pig_port integer,'
                    ' write_log boolean, mute_notifies boolean, alive boolean, kill boolean,'
                    ' total_notifies integer, total_submits integer, failed_submits integer)')

    def create_db(self):
        log('Creating database ' + self.db_name + '...')
        self.db = sqlite3.connect(self.db_name)
        self.cursor = self.db.cursor()
        self.clear_db()

    def print_all_pigs(self):
        for row in self.do_sql('SELECT * FROM pigs'):
            log(row)

    def get_all_pigs(self):
        return self.cursor.execute('SELECT * FROM pigs')

    def on_pig_accepted(self, pig_address, defaults):  # type: ((str, int), DefaultSettings)->None
        values = (self.pig_id, '', 0, pig_address[0], pig_address[1], defaults.write_log, defaults.mute_notifies,
                  True, False, 0, 0, 0)
        self.cursor.execute('DELETE FROM pigs WHERE pig_addr=? AND pig_port=?', [pig_address[0], pig_address[1]])
        self.cursor.executemany('INSERT INTO pigs VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)', [values])
        self.db.commit()
        self.pig_id += 1
        rows = self.cursor.execute('SELECT id FROM pigs WHERE pig_addr=? AND pig_port=?', [pig_address[0], pig_address[1]])
        for row in rows:
            return row[0]
        return 0

    def on_pig_disconnected(self, pig_address):  # type: (Tuple[str, int])->None
        self.cursor.execute('DELETE FROM pigs WHERE pig_addr=? AND pig_port=?', [pig_address[0], pig_address[1]])
        self.db.commit()

    def on_repeater_connected(self, pig_address, pool_address): # type: (Tuple[str, int], Tuple[str, int])->None
        sql = 'UPDATE pigs SET pool_addr=?, pool_port=?, alive=? WHERE pig_addr=? AND pig_port=?'
        self.cursor.execute(sql, [pool_address[0], pool_address[1], True, pig_address[0], pig_address[1]])
        self.db.commit()

    def update_statistics(self, pig_address, total_notifies, total_submits, failed_submits):
        sql = 'UPDATE pigs SET total_notifies=?, total_submits=?, failed_submits=? WHERE pig_addr=? AND pig_port=?'
        self.cursor.execute(sql, [total_notifies, total_submits, failed_submits, pig_address[0], pig_address[1]])

    def do_commit_after_update_statistics(self):
        self.db.commit()
