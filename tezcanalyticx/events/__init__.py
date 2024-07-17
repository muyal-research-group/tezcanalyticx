import aiorwlock
import asyncio
from typing import List, Any,Dict,Tuple
import humanfriendly as HF
import numpy as np
import numpy.typing as npt
import pandas as pd
import seaborn as sns
import scipy.stats as S
import hashlib as H
import json as J
from nanoid import generate as nanoid
# from pyndantic
import time as T

class EventX(object):
    def __init__(self,
        timestamp:float =0,
        time:str="",
        task_id:str="",
        event_type:str="UKNOWN",
        bucket_id:str="",
        key:str="",
        size:int=0.0,
        response_time:float=0.0,
        peer_id:str = "",
        # replicas:List[str] =[],
        interarrival_time:float = 0.0,
        waiting_time:float = 0.0,
        metadata:Dict[str,Any] = {},
    ):
        self.arrival_time  = T.time()
        self.timestamp     = timestamp
        self.task_id       = task_id
        self.event_id      = nanoid()
        self.event_type    = event_type
        self.bucket_id     = bucket_id
        self.key           = key
        self.size          = size
        # self.replicas      = replicas
        self.response_time = response_time
        self.waiting_time      =  waiting_time
        self.interarrival_time  = interarrival_time
        self.metadata      = metadata
        self.peer_id = peer_id
        self.time = time
    def get_combined_key(self)->str:
        return "{}@{}".format(self.bucket_id, self.key)
    def get_hashed_combined_key(self)->str:
        combined_key = self.get_combined_key()
        h = H.sha256()
        h.update(combined_key.encode())
        return h.hexdigest()
    @staticmethod
    def from_json(x:Dict[str, Any])->'EventX':
        try:
            return EventX(
                time=x.get("time",""),
                timestamp= x.get("x_timestamp",T.time_ns()),
                task_id= x.get("task_id",""),
                event_type=x.get("event","UKNOWN"),
                bucket_id= x.get("bucket_id",""),
                key= x.get("key",""),
                metadata=x.get("metadata",{}),
                response_time= x.get("response_time",0.0),
                size= x.get("size",0),
                peer_id= x.get("peer_id","")
            )
        except Exception as e:
            return EventX.empty()

    def to_dict(self):
        return self.__dict__
    def to_json(self,**kwargs):
        return J.dumps(self.to_dict(),**kwargs)
    @staticmethod
    def empty():
        return EventX()
    
class Metrics(object)  :
    @staticmethod
    def mean(xs:npt.NDArray):
        return np.mean(xs) if len(xs)>0 else 0
    @staticmethod
    def median(xs:npt.NDArray):
        return np.median(xs) if len(xs)>0 else 0

    @staticmethod
    def std(xs:npt.NDArray):
        return np.std(xs) if len(xs)>0 else 0
    @staticmethod
    def min(xs:npt.NDArray):
        return np.min(xs) if len(xs)>0 else 0
    @staticmethod
    def max(xs:npt.NDArray):
        return np.max(xs) if len(xs)>0 else 0
    
    @staticmethod
    def nanoseconds_to_seconds(nanoseconds: int) -> float:
        seconds = nanoseconds / 1e9
        return seconds
    @staticmethod
    def calculate_interarrival_times(arrival_times: List[float]) -> List[float]:
        if not arrival_times:
            return []

        interarrival_times = []
        for i in range(1, len(arrival_times)):
            interarrival_time = arrival_times[i] - arrival_times[i - 1]
            interarrival_times.append(interarrival_time)
        
        return interarrival_times
    @staticmethod
    def count_events_by_type(events: List[EventX]) -> Dict[str, int]:
        event_counter = {}
        for event in events:
            if event.event_type in event_counter:
                event_counter[event.event_type] += 1
            else:
                event_counter[event.event_type] = 1
        return event_counter

            # replicas= x.get("replicas",[]) + x.get("current_replicas",[]) + x.get("new_replicas",[]),


class StatValue:
    def __init__(self,name:str = "", value:float =0.0, format_value:str=""):
        self.name = name
        self.value = value
        self.format_value = format_value
    def to_dict(self):
        return self.__dict__
    
class MetricStats:
    def __init__(self, metric_name:str, mean:float, median:float, stddev:float, unit:str = "seconds",_min:float = 0, _max:float=0):
        formatter = HF.format_timespan if (unit == "time" or unit == "seconds") else HF.format_size
        mean_value = float(mean)
        self.mean = StatValue(
            name="mean",
            value= mean_value,
            format_value= formatter(mean_value)
        )
        median_value = float(median)
        self.median = StatValue(
            name="median",
            value= median_value,
            format_value= formatter(median_value)
        )
        stddev_value = float(stddev)
        self.stddev = StatValue(
            name="stddev", 
            value=stddev_value,
            format_value=formatter(stddev_value)
        )
        min_value = float(_min)
        self.min    =StatValue(
            name= "min",
            value=min_value,
            format_value= formatter(min_value)
        )
        max_value = float(_max)
        self.max    =StatValue(
            name= "max",
            value=max_value,
            format_value= formatter(max_value)
        )

        self.metric_name= metric_name
        self.unit = unit
    def to_dict(self):
        return {
            "metric_name": self.metric_name,
            "unit": self.unit,
            "mean": self.mean.to_dict(),
            "median": self.median.to_dict(),
            "stddev": self.stddev.to_dict(),
            "min": self.min.to_dict(),
            "max": self.max.to_dict()
        }
    def to_json(self,**kwargs):
        return J.dumps(self.to_dict(), **kwargs)
    @staticmethod
    def empty(name:str):
        return MetricStats(
            mean=0,
            median=0,
            metric_name=name,
            stddev=0
        )


# class ExponentialGrowth:
#     def __init__(self):
#         self.

class PeriodStats(object):
    BY_EVENT_TYPE = "by_event_type"
    BY_PERRS = "by_peers"
    TOTAL="TOTAL"
    def __init__(self,period_id:str,period_index:int, start_at:float,end_at:float):
        self.period_id    = period_id
        self.period_index = period_index
        self.start_at     = start_at
        self.end_at       = end_at
        self.events:List[EventX]       = []
        self.metrics:List[MetricStats] =[]
        self.counters:Dict[str, Dict[str,Any]] = {}
    def add_event(self, event:EventX):
        self.events.append(event)
    def calculate(self):
        rts = []
        iats = []
        sizes = []
        self.counters[PeriodStats.BY_EVENT_TYPE] = {}
        self.counters[PeriodStats.BY_PERRS] ={}
        self.counters[PeriodStats.TOTAL] = {}
        for e in self.events:
            ckey = e.get_combined_key()
            # By event
            current_counter = self.counters[PeriodStats.BY_EVENT_TYPE].setdefault(e.event_type,0)
            self.counters[PeriodStats.BY_EVENT_TYPE][e.event_type] = current_counter + 1
            # By  peer
            self.counters[PeriodStats.BY_PERRS].setdefault(ckey, {})
            by_peer_current_value = self.counters[PeriodStats.BY_PERRS][ckey].setdefault(e.peer_id, 0)
            self.counters[PeriodStats.BY_PERRS][ckey][e.peer_id] = by_peer_current_value + 1 
            # Total
            total_current_value = self.counters[PeriodStats.TOTAL].setdefault(ckey,0)
            self.counters[PeriodStats.TOTAL][ckey]  = total_current_value + 1 
            # _____________________________________________________________________________________
            rts.append(e.response_time)
            iats.append(Metrics.nanoseconds_to_seconds(e.interarrival_time))
            sizes.append(e.size)
        rts = np.array(rts)
        iats = np.array(iats)
        # MetricStats.empty()
        rt_is_empty = len(rts)>0
        rt_metric = MetricStats(
            metric_name = "RESPONSE.TIME",
            mean        = Metrics.mean(rts),
            median      = Metrics.median(rts),
            stddev      = Metrics.std(rts),
            _min        = Metrics.min(rts) ,
            _max        = Metrics.max(rts),
            unit        = "seconds"
        )
        iat_metric = MetricStats(
            metric_name = "INTERARRIVAL.TIME",
            mean        = Metrics.mean(iats),
            median      = Metrics.median(iats),
            stddev      = Metrics.std(iats),
            _min        = Metrics.min(iats),
            _max        = Metrics.max(iats),
            unit="seconds"
        )
        size_metric = MetricStats(
            metric_name = "SIZE",
            mean        = Metrics.mean(sizes),
            median      = Metrics.median(sizes),
            stddev      = Metrics.std(sizes),
            _min        = Metrics.min(sizes),
            _max        = Metrics.max(sizes),
            unit="bytes"
        )
        self.metrics.append(rt_metric)
        self.metrics.append(iat_metric)
        self.metrics.append(size_metric)
    def to_dict(self):
        return {
            "period_id":self.period_id,
            "period_index":self.period_index,
            "start_at":self.start_at,
            "end_at":self.end_at,
            "metrics":list(map(lambda m: m.to_dict(),self.metrics)),
            "counters":self.counters
        }
        # return
        # rt_metric = MetricStats(
        #     metric_name="RESPONSE.TIME",
        #     mean=np.mean(rts),
        #     median=np.median(rts),
        #     stddev=np.std(rts),
        # )
            
class Period(object):
    PUT_EVENT_TYPES = ["PUT.DATA","PUT.METADATA","PUT"]
    GET_EVENT_TYPES = ["GET.METADATA","GET.DATA","GET"]
    def __init__(self,period_index:int , reverse:bool= False):
        self.period_id = nanoid()
        self.period_index = period_index
        self.start_at = T.time()
        self.end_at   = - 1 
        self.events:List[EventX] = []
        self.last_event_arrival_time = -1 
        self.counter = 0
        self.lock = aiorwlock.RWLock(fast=True)
        self.exponential_growth = None
        self.reverse = reverse 
        # self

    async def find_puts(self)->List[EventX]:
        async with self.lock.reader_lock:
            xs = filter(lambda e: e.event_type in Period.PUT_EVENT_TYPES,self.events)
            return xs
            # for x in xs:
                # combined_key = x.get_combined_key()
                # if not combined_key in res:
                    # res[combined_key] = []

    async def find_gets(self)->List[EventX]:
        async with self.lock.reader_lock:
            xs = filter(lambda e: e.event_type in Period.GET_EVENT_TYPES,self.events)
            return xs
    async def get_stats(self)->Dict[str, Any]:
        events  = await self.get_events()
        gets    = PeriodStats(period_id= self.period_id, period_index= self.period_index, start_at=self.start_at,end_at=self.end_at)
        puts    = PeriodStats(period_id= self.period_id, period_index= self.period_index,start_at= self.start_at, end_at= self.end_at)
        _global = PeriodStats(period_id= self.period_id, period_index= self.period_index, start_at= self.start_at, end_at= self.end_at)
        uknown:List[EventX]  = []
        for e in events:
            if e.event_type in Period.GET_EVENT_TYPES:
                gets.add_event(event=e)
            elif e.event_type in Period.PUT_EVENT_TYPES:
                puts.add_event(event=e)
            else:
                uknown.append(e)
                continue
            _global.add_event(event=e)
        gets.calculate()
        puts.calculate()
        _global.calculate()
        return {
            "get": gets.to_dict(),
            "put": puts.to_dict(),
            "global": _global.to_dict(),
            "uknown":list(map(lambda e:e.to_dict() ,uknown))
        }

    async def avg_response_times(self)->MetricStats:
        async with self.lock.reader_lock:
            rts = list(map(lambda e: e.response_time, self.events))
            metric_name= 'RESPONSE.TIME'
            if len(rts) ==0:
                return MetricStats.empty(name=metric_name)
            ms = MetricStats(
                metric_name=metric_name,
                mean= np.mean(rts),
                median= np.median(rts),
                stddev= np.std(rts)
            )
            return ms
    async def get_total_counter(self)->Dict[str,int]:
        xs = await self.get_counter_by_peers()
        res:List[Tuple[str, int]] = []
        
        for k,v in xs.items():
            total  = sum(v.values())
            res.append((k, total ))
        return dict(res)
    async def get_counter_by_peers(self)->Dict[str, Dict[str, int]]:
        gets  = filter(lambda x: x.event_type == "GET.DATA", self.events)
        xs = {}
        for g in gets:
            combined_key = "{}@{}".format(g.bucket_id, g.key)
            if not combined_key in xs:
                xs[combined_key] = {}
            current_gets = xs[combined_key].setdefault(g.peer_id, 0)
            xs[combined_key][g.peer_id] = current_gets +1
        return xs

    async def add_events(self, events:List[EventX]=[]):
        async with self.lock.writer_lock:
            events.sort(key= lambda e: e.timestamp, reverse= self.reverse)
            # if len(self.events) >0:
            timestamps         = list(map(lambda e: e.timestamp , events))
            interarrival_times =  Metrics.calculate_interarrival_times(arrival_times= timestamps)
            # print(interarrival_times)
            # events_is_empty = len(self.events) ==0
            # last_arrival_time = self.events[-1].arrival_time if not events_is_empty  else 0
            for e,iat in zip(events,interarrival_times):
                e.interarrival_time = iat
                # e.arrival_time -  last_arrival_time  if not events_is_empty else 0
                # print(e.event_id,"INTERARRIVAL", e.interarrival_time)
                # last_arrival_time = e.arrival_time
            self.events.extend(events)
    
    async def add_event(self,event:EventX):
        async with self.lock.writer_lock:
            if self.last_event_arrival_time == -1:
                self.last_event_arrival_time = event.timestamp
                event.interarrival_time = 0.0
                event.waiting_time = 0.0
            else:
                event.interarrival_time = event.timestamp - self.last_event_arrival_time 
            self.events.append(event)
    async def get_events(self)->List[EventX]:
        async with self.lock.reader_lock:
            return self.events
    async def set_events(self,events:List[EventX]):
        async with self.lock.writer_lock:
            self.events = events
    async def delete_event(self,event_id:str):
        await self.set_events(list(filter(lambda x: x.event_id != event_id, self.events)))
    @staticmethod
    def empty(period_index:int = 0)->"Period":
        return Period(period_index= period_index)
    def to_dict(self):
        x = self.__dict__.copy()
        del x["lock"]
        return x
    def to_json(self,**kwargs):
        return J.dumps(self.__dict__, **kwargs)
    
    
class EventManager(object):
    def __init__(self):
        self.periods:List[Period] = []
        self.period_index = 0
        self.lock = aiorwlock.RWLock(fast=True)

    async def stats(self):
        async with self.lock.reader_lock:
            xs = []
            for p in self.periods:
                s = await p.get_stats()
                xs.append(s)
            return xs

    async def get_periods(self):
        async with self.lock.reader_lock:
            return self.periods

    async def __calculate(self, p:Period):
        # peers_access = filter(lambda e: e.event_type.sta ,p.events)
        event_types = Metrics.count_events_by_type(events=await p.get_events())
        puts        = p.find_puts()
        getts        = p.find_puts()
        return {
            "metrics":[
                (await p.avg_response_times()).to_dict()
            ],
            "counters":{
                "objects":0,
                "event_types":event_types,
            },
            "accesses":{
                "by_peers":await p.get_counter_by_peers(),
                "total":await p.get_total_counter()
            },
            **p.to_dict()

        }
    async def get_periods_dict(self,calculate:bool = True):
        async with self.lock.reader_lock:
            if calculate:
                xs = await asyncio.gather(*[ self.__calculate(p=p)  for p in self.periods])
                # xs = list((self.__calculate, self.periods))
                    # **x.to_dict(),"metrics": [(await x.avg_response_times()).to_dict()]
                    # } ,self.periods)
                # )
                return xs
            else:
                xs = list(map(lambda x : x.to_dict() ,self.periods))
                return xs
        
    async def get_periods_json(self):
        async with self.lock.reader_lock:
            return list(map(lambda x : x.to_json() ,self.periods))

    async def get_current_period(self):
        async with self.lock.reader_lock:
            try:
                return self.periods[self.period_index]
            except Exception as e:
                self.period_index +=1
                period = Period.empty(period_index= self.period_index)
                self.periods.append(period)
                return period
    async def add_events(self, events:List[EventX]):
        async with self.lock.writer_lock:
            period = await self.get_current_period()
            period.add_events(events =  events)
            
    async def add_event(self,event:EventX):
        async with self.lock.writer_lock:
            period = await self.get_current_period()
            await period.add_event(event=event)
