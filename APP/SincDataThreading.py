from .MysqlConnection import MysqlConnection
from .SincDataProcess import SincDataProcess
from APP.SincDataLogManipulation import SincDataLogManipulation
from APP.SincDataTimeCheck import SincDataChecks

import threading
import json
import time


class SincDataThreading(object):
    __imports = None
    __time_start = None
    __time_end = None
    __log_data = None

    def __init__(self, imports):
        self.__imports = imports

    def thread(self):
        json_loaded = json.loads(json.dumps(self.__imports))
        try:
            index = 1
            for t in json_loaded['tables']:
                connection_prod = MysqlConnection(host=json_loaded['connection_prod']['host'],
                                                  user=json_loaded['connection_prod']['user'],
                                                  password=json_loaded['connection_prod']['password'],
                                                  database=json_loaded['connection_prod']['database'])
                connection_bh = MysqlConnection(host=json_loaded['connection_bh']['host'],
                                                user=json_loaded['connection_bh']['user'],
                                                password=json_loaded['connection_bh']['password'],
                                                database=json_loaded['connection_bh']['database'])
                sinc_data = SincDataProcess(table_name=t['table_name'],
                                            order_by_date=t['order_by_date'],
                                            pk_table=t['pk_table'],
                                            days_in_prod=json_loaded['days_in_prod'],
                                            connection_prod=connection_prod,
                                            connection_bh=connection_bh,
                                            time_start=json_loaded['time_start'],
                                            time_end=json_loaded['time_end'],
                                            log_data=self.__log_data)
                t[index] = threading.Thread(target=sinc_data.process_data, args=()).start()
                index += 1
        except ValueError:
            print("Error: unable to start thread")

    def execute_import(self):
        json_loaded = json.loads(json.dumps(self.__imports))
        self.__time_start = json_loaded['time_start']
        self.__time_end = json_loaded['time_end']
        log_data = SincDataLogManipulation(None, json_loaded['days_keep_log'])
        log_data.create_log()
        log_data.write_log("*****SINCDATAMYSQL*****")
        self.__log_data = log_data
        try:
            while True:
                log_data.delete_old_files()
                if SincDataChecks(self.__time_start, self.__time_end).process_time() is True:
                    self.thread()
                else:
                    log_data.write_log("Não esta na hora de executar, inicio:" + str(self.__time_start) + "- fim:" +
                                       str(self.__time_end))
                time.sleep(60)
        except ValueError:
            print("Error: unable to start thread")
        while 1:
            pass
