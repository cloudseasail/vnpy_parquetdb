from typing import List
from vnpy.trader.constant import Exchange, Interval
from vnpy.trader.database import (
    BaseDatabase,
    BarOverview,
    DB_TZ,
    TickOverview
)
from datetime import datetime, timedelta
import polars as pl
# pl.Config.set_fmt_float('full')

from .utils import FloatDatetime, INTERVAL_DELTASECONDS, get_storage_path
from .converter import VnpyConverter, PolarsConverter
from .overview import ParquetdbOverview

ConverterClass = {
    'vnpy': VnpyConverter,
    'polars': PolarsConverter,
    # 'pandas': PandasConverter,
}

class ParquetDb(BaseDatabase):
    def __init__(self,  format ='vnpy'):
        if not format:
            format= 'polars'
        
        self.storage_path = get_storage_path()
        self.set_format(format)

    def set_format(self, format=None):
        self.default_format = format
    
    def get_converter(self, format=None):
        if format is None:
            format = self.default_format
        if format not in ConverterClass:
            format = 'polars'
        return ConverterClass[format]()
     
    def _save_data(self, df, filepath):
        if not filepath.parent.exists():
            filepath.parent.mkdir(parents=True)

        if filepath.exists():  # combine old and new
            df_new = df
            df_old = pl.read_parquet(str(filepath))
            df = df_old.filter(pl.col('datetimef')<df_new[0,'datetimef'])
            df = df.vstack(df_new)
            df_after = df_old.filter(pl.col('datetimef')>df_new[-1,'datetimef'])
            if len(df_after) >0:
                df = df.vstack(df_after)
       
        df.write_parquet(str(filepath))

    def _get_file_path(self, symbol, exchange, interval=Interval.TICK, tick_date=None):
        if tick_date:
            path = self.storage_path.joinpath(f'{interval.value}/{exchange.value}/{str(tick_date)}')
        else:
            path = self.storage_path.joinpath(f'{interval.value}/{exchange.value}')

        return path.joinpath(f'{symbol}.parquet')


    def save_tick_data(self, ticks, stream = False, symbol=None, exchange=None):
        if (type(ticks) is not pl.DataFrame):
            symbol = ticks[0].symbol
            exchange = ticks[0].exchange
            ticks = self.get_converter().TickData_to_Dataframe(ticks)
        ticks = ticks.with_columns((pl.col('datetimef')/FloatDatetime.DATE_MULTIPLIER).cast(pl.Int32).alias('datef'))
        dates = set(ticks['datef'])
        for date in dates:
            filepath =self._get_file_path(symbol, exchange, tick_date=date)
            self._save_data(ticks.filter(pl.col('datef')==date).drop('datef'), filepath)

    
    def load_tick_data(self, symbol, exchange, start, end, format=None):
        format = format if format is not None else self.default_format
        ticks = pl.DataFrame()
        step = timedelta(days=1)
        date = start.replace(hour=0, minute=0, second=0, microsecond=0)
        while date <= end:
            filepath = self._get_file_path(symbol, exchange, tick_date=FloatDatetime.date(FloatDatetime.from_datetime(date)))
            if filepath.exists():
                ticks = ticks.vstack(pl.read_parquet(str(filepath)))
            date = date + step
        if len(ticks)>0:
            ticks = ticks.with_columns(pl.col('datetimef').set_sorted(descending=False))
            ticks = ticks.filter((pl.col('datetimef')>=FloatDatetime.from_datetime(start)) & (pl.col('datetimef')<=FloatDatetime.from_datetime(end)))

        ticks = self.get_converter(format).TickData_from_DataFrame(ticks, symbol, exchange)
        return ticks
    
    
    def save_bar_data(self, bars, stream = False, symbol=None, exchange=None, interval=None):
        if (type(bars) is not pl.DataFrame):
            symbol = bars[0].symbol
            exchange = bars[0].exchange
            bars = self.get_converter().BarData_to_Dataframe(bars)
        filepath = self._get_file_path(symbol, exchange, interval=interval)
        self._save_data(bars, filepath)

    def load_bar_data(self, symbol, exchange, interval, start, end, format=None):
        format = format if format is not None else self.default_format
        filepath = self._get_file_path(symbol, exchange, interval=interval)
        if filepath.exists():
            bars_existing = pl.read_parquet(str(filepath))
        else:
            bars_existing = pl.DataFrame()
        bars = bars_existing
        if len(bars) > 0:
            bars = bars.with_columns(pl.col('datetimef').set_sorted(descending=False))
            bars = bars.filter((pl.col('datetimef')>=FloatDatetime.from_datetime(start)) & (pl.col('datetimef')<=FloatDatetime.from_datetime(end)))

        bars = self.get_converter(format).BarData_from_DataFrame(bars, symbol, exchange, interval)
        return bars
    

    def get_bar_overview(self) -> List[BarOverview]:
        df = ParquetdbOverview().get_bar_overview()
        overviews: List[BarOverview] = []
        for i in range(len(df)):
            overviews.append(BarOverview(
                symbol = str(df[i, 'symbol']),
                count = int(df[i, 'count']),
                exchange = Exchange(df[i, 'exchange']),
                interval = Interval(df[i, 'unit']),
                start = FloatDatetime.to_datetime(df[i, 'start_datetimef']),
                end = FloatDatetime.to_datetime(df[i, 'end_datetimef']),
            ))
        return overviews

    def get_tick_overview(self) -> List[TickOverview]:
        df = ParquetdbOverview().get_tick_overview()
        overviews: List[TickOverview] = []
        for i in range(len(df)):
            overviews.append(TickOverview(
                symbol = str(df[i, 'symbol']),
                count = int(df[i, 'count']),
                exchange = Exchange(df[i, 'exchange']),
                start = FloatDatetime.to_datetime(df[i, 'start_datetimef']),
                end = FloatDatetime.to_datetime(df[i, 'end_datetimef']),
            ))
        return overviews

    def delete_bar_data(self, symbol, exchange, interval):
        filepath = self._get_file_path(symbol, exchange, interval=interval)
        try:
            if filepath.exists():
                filepath.unlink()
        except PermissionError:
            print(f"Error: Permission denied to remove file {str(filepath)}")
        except Exception as e:
            print(f"Error: Unable to remove file {str(filepath)}, {e}")


    def delete_tick_data(self, symbol, exchange):
        tickPath = self.storage_path.joinpath(f"tick/{exchange.value}")
        for dfile in tickPath.glob('*'):
            if dfile.is_dir():
                datef = str(dfile.name)
                filepath = tickPath.joinpath(f"{datef}/{symbol}.parquet")
                try:
                    if filepath.exists():
                        filepath.unlink()
                except PermissionError:
                    print(f"Error: Permission denied to remove file {str(filepath)}")
                except Exception as e:
                    print(f"Error: Unable to remove file {str(filepath)}, {e}")

