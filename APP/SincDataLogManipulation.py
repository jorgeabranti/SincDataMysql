import time
import datetime
import os


class SincDataLogManipulation(object):

    def __init__(self, file_name, days_keep_log):
        self.file_name = file_name
        self.days_keep_log = days_keep_log

    def create_log(self):
        file = open("APP\LOG\sinc_data_log_" + datetime.datetime.fromtimestamp(time.time()).strftime('%Y%m%d_%H%M%S') +
                    "_.txt", "w+")
        file.close()
        self.file_name = file.name
        return self.file_name

    def write_log(self, text):
        file_name = self.file_name
        file = open(file_name, "a")
        file.write(datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d_%H:%M:%S') + "__" + text + "\n")
        file.close()

    def delete_old_files(self):
        dir_name = os.path.dirname(os.path.realpath(__file__)) + "\LOG"
        for f in os.listdir(dir_name):
            if os.stat(os.path.join(dir_name, f)).st_mtime < time.time() - self.days_keep_log * 86400:
                os.remove(os.path.join(dir_name, f))
