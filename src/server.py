import os
from threading import Thread
import time as T
# 
import humanfriendly as HF
from fastapi import FastAPI,Response,Request
from mictlanx.logger.log import Log
import uvicorn
import json as J

app = FastAPI()
class Event(object):
    """
        This class should contain the event structure. 
    """
    def __init__(self):
        pass

"""
This route is call by the clients that emits a batch of N events. You should process them and store the batch for further analysis...
"""
@app.post("/api/v4/events")
async def add_event(request:Request):
    events = J.loads(await request.json())
    # print(type(events))
    for event in events:
        print(event)
        
        print("_"*20)
    # print("Event",events)
    return Response(content=None, status_code=204)


class TezcanalyticX(Thread):
    """
        This class represents the daemon thread that executes every <hearbeat> seconds.
    """
    def __init__(self,heartbeat:str="5sec",name: str="tezcanalyticx", daemon:bool = True) -> None:
        Thread.__init__(self,name=name,daemon=daemon)
        self.is_running = True
        self.heartbeat = HF.parse_timespan(heartbeat)
        self.__log = Log(
            name = "tezcanalyticx",
            console_handler_filter=lambda x: True,
            interval=24,
            when="h"
        )
    def run(self) -> None:
        while self.is_running:
            self.__log.debug({
                "event":"ANALYZE BATCH....."
            })
            T.sleep(self.heartbeat)

#Start the thread
tz = TezcanalyticX()
tz.start()

if __name__ == "__main__":
    uvicorn.run(
        app= app,
        host=os.environ.get("IP_ADDR","0.0.0.0"), 
        port=int(os.environ.get("PORT","45000")),
        reload=bool(int(os.environ.get("REALOAD","1"))),
    )
    
