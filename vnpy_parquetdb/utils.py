

from datetime import datetime, timedelta
from vnpy.trader.database import DB_TZ

from vnpy.trader.utility import get_folder_path
def get_storage_path():
    return get_folder_path('storage')

import re
def future_symbol_split(s):
    pattern = re.compile(r'^([A-Za-z]*)([0-9]*)')
    r = re.match(pattern, s).groups()
    return (r[0], int(r[1]))
def future_code_from_symbol(symbol):
    return str(future_symbol_split(symbol)[0]).upper()

class FloatDatetime():
    DATE_MULTIPLIER = 1000000
    DATE_FORMAT = '%Y%m%d%H%M%S.%f'
    @staticmethod
    def to_datetime(fdt):
        return datetime.strptime(str(float(fdt)),'%Y%m%d%H%M%S.%f' ).replace(tzinfo= DB_TZ)
    @staticmethod
    # float %f only keep to ms,  not able to keep us info!
    def from_datetime(dt, format ='%Y%m%d%H%M%S.%f'):
        return float(dt.strftime(format))
    @staticmethod
    def time(fdt):
        return fdt%1000000
    @staticmethod
    def date(fdt):
        return int(fdt/1000000)
    @staticmethod
    def hour(fdt):
        return int(fdt/10000)%100
    @staticmethod
    def minute(fdt):
        return int(fdt/100)%100
    @staticmethod
    def second(fdt):
        return int(fdt%100)
    @staticmethod
    def plus(fdt, delta: timedelta):
        if fdt > 600000:
            dt = datetime.strptime(str(float(fdt)),'%Y%m%d%H%M%S.%f' ) + delta
            return float(dt.strftime('%Y%m%d%H%M%S.%f'))
        else:
            # bug: 10000 strftime -> 10:00:00
            dt = datetime(year=1900, month=1, day=1, hour= int(fdt/10000), minute= int((fdt/100)%100), second=int(fdt%100), microsecond=round((fdt%1)*1000)) + delta
            return float(dt.strftime('%H%M%S.%f'))
    @staticmethod
    def diff(fdt1, fdt2):  # return seconds
        return (FloatDatetime.to_datetime(fdt1)- FloatDatetime.to_datetime(fdt2)).total_seconds()
    

from typing import Dict
from vnpy.trader.constant import Interval

INTERVAL_DELTA_MAP: Dict[Interval, timedelta] = {
    Interval.TICK: timedelta(milliseconds=1),
    Interval.MINUTE: timedelta(minutes=1),
    Interval.HOUR: timedelta(hours=1),
    Interval.DAILY: timedelta(days=1),
}

def INTERVAL_DELTASECONDS(interval):
    return INTERVAL_DELTA_MAP[interval].total_seconds()


