import datetime
import time


class SincDataChecks(object):

    def __init__(self, time_start, time_end):
        self.__time_start = time_start
        self.__time_end = time_end

    def process_time(self):
        start = self.__time_start
        end = self.__time_end
        intime = datetime.datetime.fromtimestamp(time.time()).strftime('%H:%M:%S')
        if start <= intime <= end:
            return True
        elif start > end:
            end_day = datetime.time(hour=23, minute=59, second=59, microsecond=999999).strftime('%H:%M:%S')
            if start <= intime <= end_day:
                return True
            elif intime <= end:
                return True
        return False
