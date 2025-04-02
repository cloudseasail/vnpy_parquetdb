from .utils import future_code_from_symbol, get_storage_path
import polars as pl

class ParquetdbOverview():
    def __init__(self):
        self.storage_path = get_storage_path()
        self.overview_path = self.storage_path.joinpath('overview.csv')
        self.load()

    def get_bar_overview(self, refresh=True):
        if refresh:
            self.overview_all()
        return self.overview.filter(pl.col('unit')!='tick')
    def get_tick_overview(self, refresh=True):
        if refresh:
            self.overview_all()
        return self.overview.filter(pl.col('unit')=='tick')


    def load(self, refresh=False):
        if self.overview_path.exists():
            self.overview = pl.read_csv(str(self.overview_path))
        else:
            self.overview = pl.DataFrame()
        if refresh:
            self.overview_all()
        return self.overview
    def save(self):
        if len(self.overview) > 0:
            self.overview.write_csv(str(self.overview_path))
    
    def overview_all(self, ignore_unit=['raw', 'temp', 'test']):
        for unFile in self.storage_path.glob('*'):
            if unFile.is_dir():
                unit = str(unFile.name)
                if unit in ignore_unit:
                    continue
                self.overview_unit(unit)

    def overview_unit(self, unit='tick'):
        if unit == 'tick':
            self.overview_tick()
        else:
            self.overview_bar(unit)
        self.save()

    def overview_tick(self):
        tickoveview = {}
        tickPath = self.storage_path.joinpath('tick')
        for exfile in tickPath.glob('*'):
            if exfile.is_dir():
                exchange = str(exfile.name)
                for dfile in exfile.glob('*'):
                    if dfile.is_dir():
                        datef = str(dfile.name)
                        for file in dfile.glob('*.parquet'):
                            symbol = str(file.name).split('.')[0]
                            if symbol in tickoveview:
                                tickoveview[symbol]['count'] += 1
                                if datef< tickoveview[symbol]['start_datef']:
                                    tickoveview[symbol]['start_datef'] = datef
                                if datef > tickoveview[symbol]['end_datef']:
                                    tickoveview[symbol]['end_datef'] = datef
                            else:
                                tickoveview[symbol] = {
                                    'symbol': symbol,
                                    'exchange': exchange,
                                    'count': 1,
                                    'start_datef': datef ,
                                    'end_datef':datef,
                                }
        for symbol in tickoveview:
            path = tickPath.joinpath(f"{str(tickoveview[symbol]['exchange'])}/{str(tickoveview[symbol]['start_datef'])}/{symbol}.parquet")
            start_datetimef = pl.read_parquet(str(path))[0, 'datetimef']
            path = tickPath.joinpath(f"{str(tickoveview[symbol]['exchange'])}/{str(tickoveview[symbol]['end_datef'])}/{symbol}.parquet")
            end_datetimef = pl.read_parquet(str(path))[-1, 'datetimef']
            self.update(symbol, tickoveview[symbol]['exchange'], 'tick', tickoveview[symbol]['count'], start_datetimef, end_datetimef)

    def overview_bar(self, unit):
        barPath = self.storage_path.joinpath(unit)
        for exfile in barPath.glob('*'):
            if exfile.is_dir():
                exchange = str(exfile.name)
                for file in exfile.glob('*.parquet'):
                    symbol = str(file.name).split('.')[0]
                    # path = barPath.joinpath(f"{exchange}/{symbol}.parquet")
                    bars =  pl.read_parquet(str(file))
                    self.update(symbol, exchange, unit,len(bars), bars[0, 'datetimef'], bars[-1, 'datetimef'])
    

    def update(self, symbol, exchange, unit, count, start_datetimef, end_datetimef):
        if (len(self.overview) >0) :
            data = self.overview.filter((pl.col('symbol')==symbol) & (pl.col('exchange')==exchange) & (pl.col('unit')==unit))
        else:
            data = pl.DataFrame()
  
        if len(data) == 0:
            self.overview = self.overview.vstack(pl.DataFrame({
                'code': future_code_from_symbol(symbol),
                'symbol': symbol,
                'exchange': exchange,
                'unit': unit,
                'count': count,
                'start_datetimef':start_datetimef,
                'end_datetimef': end_datetimef,
            }))
        else:
            if start_datetimef < data[-1, 'start_datetimef']:
                self.overview = self.overview.with_columns(pl.when((pl.col('symbol')==symbol) & (pl.col('exchange')==exchange) & (pl.col('unit')==unit)) \
                                                           .then(start_datetimef).otherwise(pl.col('start_datetimef')).alias('start_datetimef'))
            if end_datetimef > data[-1, 'end_datetimef']:
                self.overview = self.overview.with_columns(pl.when((pl.col('symbol')==symbol) & (pl.col('exchange')==exchange) & (pl.col('unit')==unit)) \
                                                           .then(end_datetimef).otherwise(pl.col('end_datetimef')).alias('end_datetimef'))