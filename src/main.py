from threading import Thread
from mictlanx.logger.log import Log
import humanfriendly as HF
import time as T


class TezcanalyticX(Thread):
    def __init__(self,heartbeat:str="5sec",name: str="tezcanalyticx", daemon:bool = True) -> None:
        Thread.__init__(self,name=name,daemon=daemon)
        self.is_running = True
        self.heartbeat = HF.parse_timespan(heartbeat)
        self.__log = log            = Log(
            name = "tezcanalyticx",
            console_handler_filter=lambda x: True,
            interval=24,
            when="h"
        )
    def run(self) -> None:
        while self.is_running:
            self.__log.debug({
                "event":"TICK"
            })
            T.sleep(self.heartbeat)


if __name__ == "__main__":
    tz = TezcanalyticX()
    tz.start()
    T.sleep(1000)
    
