from typing import List
from vnpy.trader.object import BarData, TickData
from vnpy.trader.constant import Exchange, Interval, Direction
import polars as pl
from .utils import FloatDatetime


class PolarsConverter():
    def TickData_to_Dataframe(self, ticks: pl.DataFrame):
        if type(ticks) is not pl.DataFrame:
            print("Tick data format is not polars dataframe,  make sure to set the correct format")
        return ticks
    def TickData_from_DataFrame(self, df: pl.DataFrame, symbol, exchange):
        return df
    def BarData_to_Dataframe(self, bars: pl.DataFrame):
        if type(bars) is not pl.DataFrame:
            print("Bar data format is not polars dataframe,  make sure to set the correct format")
        return bars
    def BarData_from_DataFrame(self, df: pl.DataFrame, symbol, exchange, interval):
        return df

class VnpyConverter():
    # ["datetimef", "current", "high", "low", "volume", "position", "a1_p", "b1_p","a1_v", "b1_v"]

    def TickData_to_Dataframe(self, ticks: List[TickData]):
        df = pl.DataFrame()
        for tick in ticks:
            s = {
                'datetimef': FloatDatetime.from_datetime(tick.datetime),
                'current': float(tick.last_price),
                'high': float(tick.high_price),
                'low': float(tick.low_price),
                'volume': float(tick.volume),
                'position': float(tick.open_interest),
                'a1_p': float(tick.ask_price_1),
                'b1_p': float(tick.bid_price_1),
                'a1_v': float(tick.ask_volume_1),
                'b1_v': float(tick.bid_volume_1),
            }
            df = df.vstack(pl.DataFrame(s))
        return df

    def TickData_from_DataFrame(self, df: pl.DataFrame, symbol, exchange):
        ticks = []
        for i in range(len(df)):
            ticks.append(TickData(
                        symbol=symbol,
                        exchange=exchange,
                        datetime= FloatDatetime.to_datetime(df[i, 'datetimef']),
                        open_price=df[i, 'current'],
                        high_price=df[i, 'high'],
                        low_price=df[i, 'low'],
                        # pre_close=,
                        last_price=df[i, 'current'],
                        volume=df[i, 'volume'],
                        # turnover=,
                        open_interest=df[i, 'position'],
                        limit_up= round(df[i, 'current'] * 1.09),
                        limit_down= round(df[i, 'current'] * 0.91),
                        ask_price_1=df[i, 'a1_p'],
                        bid_price_1=df[i, 'b1_p'],
                        ask_volume_1=df[i, 'a1_v'],
                        bid_volume_1=df[i, 'b1_v'],
                        gateway_name = 'DBFILE'
                    )
            )
        return ticks

    def BarData_to_Dataframe(self, bars: List[BarData]):
        df = pl.DataFrame()
        for bar in bars:
            s = {
                'datetimef': FloatDatetime.from_datetime(bar.datetime),
                'high': float(bar.high_price),
                'open': float(bar.open_price),
                'low': float(bar.low_price),
                'close': float(bar.close_price),
                'volume': float(bar.volume),
                'turnover': float(bar.turnover),
                'position': float(bar.open_interest),
                
            }
            df = df.vstack(pl.DataFrame(s))
        return df

    def BarData_from_DataFrame(self, df: pl.DataFrame, symbol, exchange, interval):
        bars = []
        for i in range(len(df)):
            bars.append(BarData(
                        symbol=symbol,
                        exchange=exchange,
                        interval= interval,
                        datetime= FloatDatetime.to_datetime(df[i, 'datetimef']),
                        open_price=df[i, 'open'],
                        close_price=df[i, 'close'],
                        high_price=df[i, 'high'],
                        low_price=df[i, 'low'],
                        volume=df[i, 'volume'],
                        turnover=df[i, 'turnover'],
                        open_interest=df[i, 'position'],
                        gateway_name = 'DBFILE'
                    )
            )
        return bars