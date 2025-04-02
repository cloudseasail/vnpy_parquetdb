


import polars as pl
from datetime import datetime, timedelta
from .utils import future_code_from_symbol, FloatDatetime, INTERVAL_DELTASECONDS

FUTURES_TRADING_TIME_TEMPLATE = {
    '1500': [('09:00:00', '10:15:00'), ('10:30:00', '11:30:00'), ('13:30:00', '15:00:00')],
    '2300': [('09:00:00', '10:15:00'), ('10:30:00', '11:30:00'), ('13:30:00', '15:00:00'), ('21:00:00', '23:00:00')],
    '0100': [('09:00:00', '10:15:00'), ('10:30:00', '11:30:00'), ('13:30:00', '15:00:00'), ('21:00:00', '01:00:00')],
    '0230': [('09:00:00', '10:15:00'), ('10:30:00', '11:30:00'), ('13:30:00', '15:00:00'), ('21:00:00', '02:30:00')],
}

FUTURES_TRADING_TIME = {
    # SHFE
    'al': FUTURES_TRADING_TIME_TEMPLATE['0100'],
    'ao': FUTURES_TRADING_TIME_TEMPLATE['0100'],
    'cu': FUTURES_TRADING_TIME_TEMPLATE['0100'],
    'ni': FUTURES_TRADING_TIME_TEMPLATE['0100'],
    'pb': FUTURES_TRADING_TIME_TEMPLATE['0100'],
    'sn': FUTURES_TRADING_TIME_TEMPLATE['0100'],
    'ss': FUTURES_TRADING_TIME_TEMPLATE['0100'],
    'zn': FUTURES_TRADING_TIME_TEMPLATE['0100'],
    'ag': FUTURES_TRADING_TIME_TEMPLATE['0230'],
    'au': FUTURES_TRADING_TIME_TEMPLATE['0230'],
    'wr': FUTURES_TRADING_TIME_TEMPLATE['1500'],
    #INE
    'bc': FUTURES_TRADING_TIME_TEMPLATE['0100'],
    'sc': FUTURES_TRADING_TIME_TEMPLATE['0230'],
    'ec': FUTURES_TRADING_TIME_TEMPLATE['1500'],
    #GFEX
    'lc': FUTURES_TRADING_TIME_TEMPLATE['1500'],
    'si': FUTURES_TRADING_TIME_TEMPLATE['1500'],
    #DCE
    'bb': FUTURES_TRADING_TIME_TEMPLATE['1500'],
    'fb': FUTURES_TRADING_TIME_TEMPLATE['1500'],
    'jd': FUTURES_TRADING_TIME_TEMPLATE['1500'],
    'lh': FUTURES_TRADING_TIME_TEMPLATE['1500'],
    'lg': FUTURES_TRADING_TIME_TEMPLATE['1500'],
    #CZCE
    'AP': FUTURES_TRADING_TIME_TEMPLATE['1500'],
    'CJ': FUTURES_TRADING_TIME_TEMPLATE['1500'],
    'JR': FUTURES_TRADING_TIME_TEMPLATE['1500'],
    'PK': FUTURES_TRADING_TIME_TEMPLATE['1500'],
    'PM': FUTURES_TRADING_TIME_TEMPLATE['1500'],
    'RI': FUTURES_TRADING_TIME_TEMPLATE['1500'],
    'RS': FUTURES_TRADING_TIME_TEMPLATE['1500'],
    'SF': FUTURES_TRADING_TIME_TEMPLATE['1500'],
    'SM': FUTURES_TRADING_TIME_TEMPLATE['1500'],
    'UR': FUTURES_TRADING_TIME_TEMPLATE['1500'],
    'WH': FUTURES_TRADING_TIME_TEMPLATE['1500'],

    'DEFAULT': FUTURES_TRADING_TIME_TEMPLATE['2300'],
}

import copy
def get_future_trading_time(code):
    code = code.upper()
    if code in FUTURES_TRADING_TIME:
        time_split = FUTURES_TRADING_TIME[code]
    else:
        time_split = FUTURES_TRADING_TIME['DEFAULT']

    time_split = copy.deepcopy(time_split)
    # Do not corrupt the global 
    time_trading =[]
    for (t1, t2) in time_split:
        time_trading.append((int(t1.replace(':','')), int(t2.replace(':',''))))
    return time_trading

def is_in_trading_time(datetimef, trading_time_arr):
    time_valid = False
    timefloat = FloatDatetime.time(datetimef)
    for (t1,t2) in trading_time_arr:
        if t2 < 90000 : #split
            if timefloat>=t1 and timefloat<=240000:
                time_valid = True
                break
            if timefloat>=0 and timefloat<=t2:
                time_valid = True
                break
        elif timefloat>=t1 and timefloat<=t2:
            time_valid = True
            break
    return time_valid

##############################

def bar_generation_from_ticks(ticks, symbol, interval):
    seconds = INTERVAL_DELTASECONDS(interval)
    return bar_generation_from_ticks_wSeconds(ticks, symbol, seconds)

# ticks[0, 'daettimef'] must be round time
def bar_generation_from_ticks_wSeconds(ticks, symbol, seconds):
    code=future_code_from_symbol(symbol)
    time_valid = get_future_trading_time(code)
    time_valid_start = []
    time_valid_end = []
    for (t1,t2) in time_valid:
        time_valid_start.append(t1)
        if t2 < 90000:
            time_valid_end.append(0)  # cross 0 clock, make 0 clock as end as well
        time_valid_end.append(t2)
    
    ticks = ticks.with_columns((pl.col('volume')-pl.col('volume').shift(1)).alias('volume_change'))
    ticks[0, 'volume_change'] = ticks[0, 'volume']
    ticks = ticks.with_columns(pl.when(pl.col('volume_change')<0).then(pl.col('volume')).otherwise(pl.col('volume_change')))

    bars = pl.DataFrame()
    bar_end = ticks[0, 'datetimef'] -1
    while True:
        bar_start = None
        bar_start_query = ticks.filter(pl.col('datetimef')>bar_end)
        if(len(bar_start_query)==0):
            break
        bar_start_query = bar_start_query[0, 'datetimef']
        if not is_in_trading_time(bar_start_query, time_valid):
            bar_end = bar_start_query
            continue

        bar_start = bar_start if bar_start is not None else bar_start_query
        seconds_round = FloatDatetime.second(bar_start)
        seconds_round = int(seconds_round/seconds) * seconds
        bar_start = int(bar_start/100)*100 + seconds_round
        bar_end = FloatDatetime.plus(bar_start, timedelta(seconds=seconds))
        
        if int(FloatDatetime.time(bar_end)) in time_valid_end: 
            ticks_inbar = ticks.filter((pl.col('datetimef')>=bar_start_query) & (pl.col('datetimef')<=bar_end))
        else:
            ticks_inbar = ticks.filter((pl.col('datetimef')>=bar_start_query) & (pl.col('datetimef')< bar_end))
            bar_end = bar_end - 0.001 # to use in next loop which is next bar
        if len(ticks_inbar) ==0 :
            print(f"Error: ticks_inbar is empty, {bar_start_query}-{bar_end}")
            continue
        s = {
            'datetimef': bar_end,
            'high': ticks_inbar['current'].max(),
            'open': ticks_inbar[0, 'current'],
            'low': ticks_inbar['current'].min(),
            'close': ticks_inbar[-1, 'current'],
            'volume': ticks_inbar['volume_change'].sum(),
            'turnover': float(0.0),
            'position': ticks_inbar[-1, 'position'],
        }
        bars = bars.vstack(pl.DataFrame(s))
    return bars