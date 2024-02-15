import os
import time as T
import pandas as pd
import unittest
from mictlanx.v4.client import Client
from mictlanx.utils.index import Utils
from mictlanx.logger.log import JsonFormatter
import logging
# from logging import handlers as LH
from typing import Dict,List,Tuple
import requests as R
import json as J
from threading import Thread
from queue import Queue
import humanfriendly as HF

TEZCANALYTICX_URL = os.environ.get("TEZCANALYTICX_URL","localhost:45000")

class TezcanaliticXHttpHandlerDaemon(Thread):
    def __init__(self,
                 q:Queue,
                 url:str,
                 buffer_size:int = 10,
                 flush_timeout:str="30seg",
                 name: str="tezcanalyticx",
                 daemon:bool = True
    ) -> None:
        Thread.__init__(self,name=name,daemon=daemon)
        self.url = url
        self.last_flush_at = T.time()
        self.is_running = True
        self.q=q
        self.buffer:List[Dict[str,str]] = []
        self.max_buffer = buffer_size
        self.flush_timeout = HF.parse_timespan(flush_timeout)

    def flush(self):
        if len(self.buffer)>0:
            json_data = J.dumps(self.buffer)
            try:
                # print(json_data)
                response = R.post(self.url, json=json_data, headers={"Content-Type":"application/json"})
                response.raise_for_status()
            except Exception as e:
                pass
            finally:
                self.buffer=[]
    def can_flush(self):
        return (T.time() - self.last_flush_at) >= self.flush_timeout
    def run(self) -> None:
        while self.is_running:
            try:
                event = self.q.get(block=True,timeout=self.flush_timeout)
                if event == -1:
                    self.flush()
                else:
                    self.buffer.append(event)
            except Exception as e:
                self.flush()
                # self.q.put()

class TezcanalyticXHttpHandler(logging.Handler):
    def __init__(self,
        flush_timeout:str="10s",
        buffer_size:int = 10,
        path:str="/api/v4/events",
        port:int = 45000,
        hostname:str ="localhost",
        protocol:str ="http", level: int = 0
    ):
        super().__init__(level)
        self.protocol = protocol
        self.hostname = hostname
        self.port = port
        self.path = path
        self.q = Queue(maxsize=buffer_size)
        self.url= "{}://{}{}".format(self.protocol,self.hostname,self.path) if port<=0 else "{}://{}:{}{}".format(self.protocol,self.hostname,self.port,self.path)
        self.buffer_size = buffer_size
        self.emit_counter = 0
        self.daemon = TezcanaliticXHttpHandlerDaemon(

            url=self.url,
            q = self.q,
            buffer_size=self.buffer_size,
            flush_timeout=flush_timeout,
            name="tezcanalyticx-daemon",
            daemon=True
        )
        self.daemon.start()
        self.setFormatter(JsonFormatter())
        # self.failed_requests_buffer:List[logging.LogRecord ] = []
    def format(self,record:logging.LogRecord):
        msg = record.getMessage()
        if type(msg) == dict:
            return {
                "message":record.getMessage(),
                "level":record.levelname,
                "name":record.name,
                "time":self.formatter.formatTime(record=record, datefmt="%Y-%m-%d %H:%M:%S"),
                "timestamp":int(T.time())
            }
        else:
            return {
                "message":record.getMessage(),
                "level":record.levelname,
                "name":record.name,
                "time":self.formatter.formatTime(record=record, datefmt="%Y-%m-%d %H:%M:%S"),
                "timestamp":int(T.time())
            }

        
    def emit(self, record: logging.LogRecord):
        self.emit_counter+=1
        _r = self.format(record=record)
        self.q.put(_r)
        if self.emit_counter % self.buffer_size == 0:
            self.q.put(-1)
# _____________________________________________________________
logger = logging.getLogger("tezcanalyticx")
logger.setLevel(logging.DEBUG)
logger.addHandler(TezcanalyticXHttpHandler())



# peers =  list(Utils.peers_from_str(peers_str=os.environ.get("MICTLANX_PEERS","mictlanx-peer-0:localhost:7000 mictlanx-peer-1:localhost:7001")) )
# # print(peers)
# bucket_id = "public-bucket-0"
    
# client = Client(
#     client_id    = "client-example-0",
#     # 
#     peers        = peers,
#     # 
#     debug        = False,
#     # 
#     daemon       = True, 
#     show_metrics = False,
#     # 
#     max_workers  = 2,
#     # 
#     lb_algorithm ="2CHOICES_UF",
#     bucket_id= bucket_id ,

# )
    

class BasicWorkloadTestSuite(unittest.TestCase):

    def test_10operation(self):
        workload = pd.read_csv("./workload1.csv")
        for (i,row) in workload.iterrows():
            operation_type = row["OPERATION_TYPE"]
            if operation_type =="PUT":
                logger.info(
                    {
                        "operation_type":"PUT",
                        "arrival_time":str(T.time())
                    }
                )
                # T.sleep(20)
            else:
                logger.info(
                    {
                        "operation_type":"GET",
                        "arrival_time":str(T.time())
                    }
                )
            T.sleep(.5)
            # print(row)
        print("DONE")
        T.sleep(100)

if __name__ =="__main__":
    unittest.main()