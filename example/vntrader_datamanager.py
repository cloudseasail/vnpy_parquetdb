import sys
sys.path.append("..")

from vnpy.event import EventEngine
from vnpy.trader.engine import MainEngine
from vnpy.trader.ui import MainWindow, create_qapp
from vnpy_datamanager import DataManagerApp



def main():
    """"""
    qapp = create_qapp()

    event_engine = EventEngine()

    main_engine = MainEngine(event_engine)

    main_engine.add_app(DataManagerApp)
    
    main_window = MainWindow(main_engine, event_engine)
    main_window.showMaximized()

    qapp.exec()

import traceback
if __name__ == "__main__":
    try:
        main()
    except Exception:
       print( f"vntrader_app 触发异常已停止\n{traceback.format_exc()}")
    finally:
        pass