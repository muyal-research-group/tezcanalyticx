import os
import time as T
import asyncio
import humanfriendly as HF
from enum import Enum
from pydantic import BaseModel
from fastapi import FastAPI,Response,Request,HTTPException
from fastapi.openapi.utils import get_openapi
from fastapi.responses import JSONResponse
from fastapi.encoders import jsonable_encoder
from mictlanx.logger.log import Log
from asyncio.locks import Lock
from typing import Dict,List,Any
import uvicorn
import json as J
import numpy as np
import numpy.typing as npt
from tezcanalyticx.interfaces.index import Counter,ObjectTimeStore
import seaborn as sns
import matplotlib.pyplot as plt
import pandas as pd 
import pytz as PTZ
from contextlib import asynccontextmanager
from io import BytesIO
# ________________________________

# Environment variables
LOG_INTERVAL                = int(os.environ.get("LOG_INTERVAL","24"))
LOG_PATH                    = os.environ.get("LOG_PATH", "/log")
LOG_WHEN                    = os.environ.get("LOG_WHEN","h")
PERIOD_WINDOW_TIME_SECS_STR = os.environ.get("PERIOD_WINDOW_TIME_SEC","60s")
IP_ADDR                     = os.environ.get("TEZCANALYTICX_IP_ADDR","0.0.0.0"),
PORT                        = int(os.environ.get("TEZCANALYTICX_PORT","45000")),
RELOAD                      = bool(int(os.environ.get("REALOAD","1"))),
PERIOD_WINDOW_TIME_SECS_STR             = os.environ.get("PERIOD_WINDOW_TIME_SEC","60s")
# ________________________________

GLOBAL_START_TIME                       = -1
EVENTS:List[Dict[str,Any]]              = []
# MAX_EVENTS                              = 1000
INTERARRIVAL_TIMES:npt.NDArray          = np.array([])
PERIOD_WINDOW_TIME_SECS                 = HF.parse_timespan(PERIOD_WINDOW_TIME_SECS_STR)
CURRENT_INTERVAL_TIME_WINDOW_START_TIME = -1
PERIOD_COUNTER                          = 0
PERIOD_OPERATION_COUNTER                = 0
ACCESS_COUNTER_BY_OBJECT                = Counter()
OBJECT_TIME_STORE                       = ObjectTimeStore()
# MAX_PERIODS                             = 60
RESPONSES_TIMES:npt.NDArray             = np.array([])
EVENTS_BY_PERIOD                        = []
OBJECT_ACCESS_BY_PEER                   = {}
GLOBAL_DF                               = pd.DataFrame({
    "TIMESTAMP":[],
    "OBJECT_ID":[],
    "RESPONSE_TIME":[],
    "INTERARRIVAL_TIME":[]
})
log = Log(
    name = "tezcanalyticx",
    console_handler_filter=lambda x: True,
    interval=LOG_INTERVAL,
    when=LOG_WHEN,
    path=LOG_PATH
)

log.debug({
    "event":"TEZCANALYTICX.STARTED",

})

LOCK = Lock()




async def run_async_analyzer(heartbeat:str="30sec"):
    _heartbeat = HF.parse_timespan(heartbeat)
    global EVENTS
    global EVENTS_BY_PERIOD
    while True :
        try:

            EVENTS_BY_PERIOD.append(EVENTS.copy())
            EVENTS =[]
            log.debug({
                "event":"ANALYZE.BATCH",
                "event_len":len(EVENTS),
                "periods_counter":len(EVENTS_BY_PERIOD)
            })
        except Exception as e:
            log.error({
                "msg":str(e)
            })
        finally:
            await asyncio.sleep(_heartbeat)






@asynccontextmanager
async def lifespan(app:FastAPI):
    task = asyncio.create_task(run_async_analyzer(heartbeat=PERIOD_WINDOW_TIME_SECS_STR ))
    yield
    task.cancel()


# @app.on_event("startup")
# async def startup_event():


app = FastAPI(
    lifespan=lifespan,
    # title="",
    # description="",
    
    
)
def custom_openapi():
    if app.openapi_schema:
        return app.openapi_schema
    openapi_schema = get_openapi(
        version="0.0.1",
        title="TezcanalyticX",
        description="TezcanalyticX is a software tool for object storage system improvement. By analyzing access frequency, replica demand, and other key metrics, it provides actionable insights to enhance data availability and performance. With automated replication strategies and customizable reporting.",
        routes=app.routes,
    )
    openapi_schema["info"]["x-logo"] = {
        "url": "https://alpha.tamps.cinvestav.mx/v0/mictlanx/router/api/v4/buckets/public/0cc3ffc850215e8fc7dd4189a68a4885bb50d134bbac2d4aff2b1904d6ad8b8d"
    }
    app.openapi_schema = openapi_schema
    return app.openapi_schema


app.openapi = custom_openapi




async def get_object_info():
    global ACCESS_COUNTER_BY_OBJECT
    global OBJECT_TIME_STORE
    global OBJECT_ACCESS_BY_PEER
    async with LOCK:
        xs = {}
        for key,value in ACCESS_COUNTER_BY_OBJECT.get_counter().items():
            freq = ACCESS_COUNTER_BY_OBJECT.get_frequency_by_key(key=key)
            ot = OBJECT_TIME_STORE.get_object_time_by_key(key=key)
            ct = T.time()
            xs.setdefault(key,{})
            xs[key]= {
                # "object_id":key,
                "accesses":value,
                "access_frequency":freq,
                "rrd":value/(ct - ot.created_at),
                "sve": (ct - ot.last_access_at) * (1/freq), 
                "access_by_peer": OBJECT_ACCESS_BY_PEER[key]
            }
    return xs

        # xs = sorted(xs, key= lambda x: x["sve"])


async def get_access_by_period(key:str)->List[int]:
    global EVENTS_BY_PERIOD
    xs:List[int] =[]
    events = []
    async with LOCK:
        for period in  EVENTS_BY_PERIOD:
            filtered = list(filter(lambda x : x["key"] == key and x["event_type"] == "GET", period ))
            events.append(filtered)
            access_by_period = len(filtered)
            xs.append(access_by_period)
    return xs,events




@app.post("/flush")
async def flush():
    global GLOBAL_START_TIME
    global EVENTS
    global INTERARRIVAL_TIMES
    global PERIOD_WINDOW_TIME_SECS_STR
    global PERIOD_WINDOW_TIME_SECS
    global CURRENT_INTERVAL_TIME_WINDOW_START_TIME
    global PERIOD_COUNTER
    global PERIOD_OPERATION_COUNTER
    global ACCESS_COUNTER_BY_OBJECT
    global OBJECT_TIME_STORE
    global RESPONSES_TIMES
    global EVENTS_BY_PERIOD
    global OBJECT_ACCESS_BY_PEER
    global GLOBAL_DF
    try:
        start_time = T.time()
        GLOBAL_START_TIME                       = -1
        EVENTS                                  = []
        # MAX_EVENTS                              = 1000
        INTERARRIVAL_TIMES         = np.array([])
        PERIOD_WINDOW_TIME_SECS                 = HF.parse_timespan(PERIOD_WINDOW_TIME_SECS_STR)
        CURRENT_INTERVAL_TIME_WINDOW_START_TIME = -1
        PERIOD_COUNTER                          = 0
        PERIOD_OPERATION_COUNTER                = 0
        ACCESS_COUNTER_BY_OBJECT                = Counter()
        OBJECT_TIME_STORE                       = ObjectTimeStore()
        # MAX_PERIODS                             = 60
        RESPONSES_TIMES             = np.array([])
        EVENTS_BY_PERIOD                        = []
        OBJECT_ACCESS_BY_PEER                   = {}
        GLOBAL_DF                               = pd.DataFrame({
            "TIMESTAMP":[],
            "OBJECT_ID":[],
            "RESPONSE_TIME":[],
            "INTERARRIVAL_TIME":[]
        })
        res = {
            "response_time":T.time() - start_time
        }
        return JSONResponse(content=jsonable_encoder(res))
    except Exception as e:
        return 
@app.get("/api/v4/periods/accesses/{object_id}", response_model=List[int])
async def accesses_per_period_by_object_id(object_id:str):
        xs,_ = await get_access_by_period(key=object_id)
        return JSONResponse(
            content= jsonable_encoder(xs)
        )

class EventsPerPeriodByObjectId(BaseModel):
    arrival_time: float
    time:str
    timestamp:int
    event_type: str
    bucket_id: str
    key: str
    peer_id: str
    size:str 
    period_arrival_time: float
    mean_interarrival_time: float
    median_interarrival_time: float
    std_interarrival_time: float
    interarrival_time: float
    period_counter: int
    period_operation_counter: int
    global_access_counter: int
    frequency: float
    response_time: float
    rrd: float
    diff_ct_lt: float
    sve: float

@app.get("/api/v4/periods/events/{object_id}", response_model= List[List[EventsPerPeriodByObjectId]])
async def events_per_period_by_object(object_id:str):
        _,xs = await get_access_by_period(key=object_id)
        return JSONResponse(
            content= jsonable_encoder(xs)
        )





async def freq_rrd_sve_summary(object_id:str)->List[Dict[str,Any]]:
    xs,es = await get_access_by_period(key=object_id)
    data = []
    for i,period in enumerate(es):
        can_calculate_exponential_growth = len(period) > 0 and i >=2
            # print("CALCULATE EXPONENTIAL GROWTH", i)
        freqs = np.array(list(map(lambda x : x["frequency"], period)))
        rrds = np.array(list(map(lambda x : x["rrd"], period)))
        sves = np.array(list(map(lambda x : x["sve"], period)))
        current_data = {
            "avg_freq":np.mean(freqs) if len(freqs) >= 2 else 0,
            "avg_rrd":np.mean(rrds) if len(rrds) >= 2 else 0,
            "avg_sve":np.mean(sves) if len(sves) >= 2 else 0,
        }
        if can_calculate_exponential_growth:
            last_data = data[i-1]
            last_freq = last_data["avg_freq"]
            last_rrd  = last_data["avg_rrd"]
            last_sve  = last_data["avg_sve"] 
            freq_ri   = 0
            rrd_ri    = 0
            sve_ri    = 0
            if last_freq > 0:
                freq_ri = (current_data["avg_freq"] / last_freq) - 1
            if last_rrd >0:
                rrd_ri = (current_data["avg_rrd"] / last_rrd) - 1
            
            if last_sve >0:
                sve_ri = (current_data["avg_sve"] / last_sve) - 1


            current_data["freq_rate_of_growth"] = freq_ri
            current_data["rrd_rate_of_growth"] = rrd_ri
            current_data["sve_rate_of_growth"] = sve_ri

        data.append(current_data)
    return data




class ExponentialGrowthByObjectById(BaseModel):
    avg_freq:float
    avg_rrd:float
    avg_sve: float
    freq_rate_of_growth: float
    rrd_rate_of_growth: float
    sve_rate_of_growth: float

@app.get("/api/v4/periods/expg/{object_id}", response_model=ExponentialGrowthByObjectById)
async def exponential_growth_by_object_id(object_id:str):
        data =await freq_rrd_sve_summary(object_id=object_id)
        return JSONResponse(
            content= jsonable_encoder(data)
        )

# class EventByPeriod(BaseModel):

@app.get("/api/v4/periods", response_model=List[List[EventsPerPeriodByObjectId]])
async def events_by_period():
    global EVENTS_BY_PERIOD
    async with LOCK:

        return JSONResponse(
            content= jsonable_encoder(EVENTS_BY_PERIOD)
        )

class SummaryItem(BaseModel):
    avg_freq:float
    avg_rrd: float
    avg_sve:float
class ObjectStats(BaseModel):
    accesses: int
    access_frequency: float
    rrd: float
    sve: float
    access_by_peer: Dict[str,int]
    summary:List[SummaryItem]
    avg_freq_rog: float
    avg_rrd_rog: float
    avg_sve_rog: float

@app.get("/api/v4/objects",response_model=ObjectStats)
async def object_stats():
    xs = await get_object_info()
    ys = {}
    for object_id,value in  xs.items():
        summary = await freq_rrd_sve_summary(object_id=object_id)
        value["summary"] = summary
        
        avg_freq_rogs = []
        avg_rrd_rogs = []
        avg_sve_rogs = []
        
        for s in summary:
            avg_freq_rogs.append( s.get("freq_rate_of_growth",0) )
            avg_rrd_rogs.append(s.get("rrd_rate_of_growth",0) )
            avg_sve_rogs.append(s.get("sve_rate_of_growth",0))
        value["avg_freq_rog"] = np.array(avg_freq_rogs).mean()
        value["avg_rrd_rog"] = np.array(avg_rrd_rogs).mean()
        value["avg_sve_rog"] = np.array(avg_sve_rogs).mean()

        ys[object_id] = value
    xs = dict(sorted(ys.items(), key= lambda x: x[1]["sve"]))
    return JSONResponse(
        content= jsonable_encoder(xs)
    )


# class GetSortedAccessCounter(BaseModel):
@app.get("/api/v4/objects/counter",response_model=Dict[str,int])
async def get_object_access_counter():
    global ACCESS_COUNTER_BY_OBJECT
    async with LOCK:
        return JSONResponse(
            content= jsonable_encoder(ACCESS_COUNTER_BY_OBJECT.get_sorted_counter() )
        )

@app.get("/api/v4/objects/frequency", response_model=Dict[str,float])
async def get_object_frequencies():
    global ACCESS_COUNTER_BY_OBJECT
    async with LOCK:
        return JSONResponse(
            content= jsonable_encoder(ACCESS_COUNTER_BY_OBJECT.get_counter_frequency() )
        )


@app.get("/api/v4/objects/sve",response_model=Dict[str,float])
async def get_object_sve():
    global ACCESS_COUNTER_BY_OBJECT
    global OBJECT_TIME_STORE
    async with LOCK:
        freqs = ACCESS_COUNTER_BY_OBJECT.get_counter_frequency()
        xs = {}
        for key,ot in OBJECT_TIME_STORE.get_store().items():
            freq = freqs.get(key)
            sve = (T.time() - ot.last_access_at)* 1/freq
            xs[key] = sve

        sorted_dict = dict(sorted(xs.items(), key=lambda item: item[1], reverse=False))
        return JSONResponse(
            content= jsonable_encoder( sorted_dict)
        )


class GetObjectTimesResponseModel(BaseModel):
    created_at: float
    first_access_at: float
    last_access_at: float

@app.get("/api/v4/objects/times", response_model=Dict[str, GetObjectTimesResponseModel])
async def get_object_time():
    global OBJECT_TIME_STORE
    async with LOCK:
        return JSONResponse(
            content= jsonable_encoder(OBJECT_TIME_STORE.get_store() )
        )


@app.get("/plot/rts")
async def plot_response_time():
    global GLOBAL_START_TIME
    global GLOBAL_DF
    async with LOCK:
        # Generate seaborn plot
        df = GLOBAL_DF.copy(deep=True)
        df["TIMESTAMP"] = pd.to_datetime(df["TIMESTAMP"],unit="s").dt.tz_localize("UTC")
        local_tz = PTZ.timezone("America/Mexico_City")
        df["TIMESTAMP"] = df["TIMESTAMP"].dt.tz_convert(local_tz)
        df.sort_values(by=["TIMESTAMP"],inplace=True,ignore_index=True)
        plt.figure(figsize=(10, 6))
        sns.lineplot(x="TIMESTAMP", y="RESPONSE_TIME", data=df, sort=False, estimator=None, color='black', alpha=.9, hue = "EVENT_TYPE")
        plt.fill_between(df["TIMESTAMP"], df["RESPONSE_TIME"], color="black", alpha=0.2)

        # Add labels and title
        plt.xlabel("Elapsed Time")
        plt.ylabel("Response Time (sec)")
        # plt.title("Response Time Over Time (Area Plot)")
        
        # Save plot to BytesIO object
        buffer = BytesIO()
        plt.savefig(buffer, format='png')
        buffer.seek(0)

        # Close plot
        plt.close()

        # Return plot as bytes in response
        return Response(content=buffer.getvalue(), media_type="image/png")



PLOT_TYPE_CONFIG = {
    "rrd":{
        "key":"avg_rrd",
        "x_label":"Periods",
        "y_label":"Replication Request Demand (RRD)",
    },
    "sve":{
        "key":"avg_sve",
        "x_label":"Periods",
        "y_label":"Storage Value Element (SVE)",
    },
    "freq":{
        "key":"avg_freq",
        "x_label":"Periods",
        "y_label":"Access Frequency (AF)",
    },
    "rogrrd":{
        "key":"rrd_rate_of_growth",
        "x_label":"Periods",
        "y_label":"Rate of Growth - RRD",
    },
    "rogsve":{
        "key":"sve_rate_of_growth",
        "x_label":"Periods",
        "y_label":"Rate of Growth - SVE",
    },
    "rogfreq":{
        "key":"freq_rate_of_growth",
        "x_label":"Periods",
        "y_label":" Rate of Growth - AF",
    },

}


class PlotType(str, Enum):
    freq ="freq"
    rrd = "rrd"
    sve = "sve"
    rogfreq ="rogfreq"
    rogrrd = "rogrrd"
    rogsve = "rogsve"
    iat    = "interarrival_time"

@app.get("/plot/periods/{object_id}/{plot_type}")
async def plot_stats_by_periods(object_id:str,plot_type:PlotType,max_periods:int=10):
    global GLOBAL_START_TIME
    global GLOBAL_DF
    global EVENTS_BY_PERIOD
    global PLOT_TYPE_CONFIG

    xs = await get_object_info()
    # plot_type = plot_type.
    # for object_id,value in  xs.items():
    config = PLOT_TYPE_CONFIG.get(plot_type,PLOT_TYPE_CONFIG["rrd"])

    value = xs.get(object_id,None)
    if value ==  None:
        raise HTTPException(status_code=404, detail="{} not found".format(object_id))
        # return Response(content="{} not found".format(object_id), )
    # object_id = 
    summary = await freq_rrd_sve_summary(object_id=object_id)
    value["summary"] = summary
    print(value)
    
    avg_freq_rogs = []
    avg_rrd_rogs = []
    avg_sve_rogs = []
    
    for s in summary:
        avg_freq_rogs.append( s.get("freq_rate_of_growth",0) )
        avg_rrd_rogs.append(s.get("rrd_rate_of_growth",0) )
        avg_sve_rogs.append(s.get("sve_rate_of_growth",0))
    value["avg_freq_rog"] = np.array(avg_freq_rogs).mean()
    value["avg_rrd_rog"] = np.array(avg_rrd_rogs).mean()
    value["avg_sve_rog"] = np.array(avg_sve_rogs).mean()

    
    async with LOCK:
        periods = len(EVENTS_BY_PERIOD)
        periods_range =  list(range(1, periods+1))
        print("PERIODS",periods_range)
        response_times = list(map(lambda x: x.get(config["key"],0), summary))
        print("RTYS", response_times)
        # print("SUMMARY",summary)
        # Generate seaborn plot
        # df = GLOBAL_DF.copy(deep=True)
        # df["TIMESTAMP"] = pd.to_datetime(df["TIMESTAMP"],unit="s").dt.tz_localize("UTC")
        # local_tz = PTZ.timezone("America/Mexico_City")
        # df["TIMESTAMP"] = df["TIMESTAMP"].dt.tz_convert(local_tz)
        # df.sort_values(by=["TIMESTAMP"],inplace=True,ignore_index=True)
        df = pd.DataFrame({
            "TIMESTAMP": periods_range,
            "RESPONSE_TIME":response_times
            # summary
        })
        plt.figure(figsize=(10, 6))
        sns.lineplot(x="TIMESTAMP", y="RESPONSE_TIME", data=df,  color='black', alpha=.9, markers=["o"])
        # max_ticks = 
        num_ticks = min(len(periods_range), max_periods)

        # Calculate the positions and labels for the ticks
        # plt.xticks(periods_range, map(int, periods_range))
        plt.fill_between(df["TIMESTAMP"], df["RESPONSE_TIME"], color="black", alpha=0.2)

        # Add labels and title
        plt.xlabel(config["x_label"])
        plt.ylabel(config["y_label"])
        if num_ticks -1 == 0:
            raise HTTPException(status_code=400, detail="This plot couldn't be generated")
        tick_positions =[int(i * (len(periods_range) - 1) / (num_ticks - 1)) for i in range(num_ticks)]
        tick_labels = [str(periods_range[i]) for i in tick_positions]
        # if tick_positions[0] == 0:
            # tick_positions.insert(1,1)
            # tick_labels.insert(0, '2')
        # print("POSITION",tick_positions)
        # print("LAVBELS",tick_labels)

        # Format X-axis ticks
        plt.xticks(tick_positions, tick_labels)
        # plt.title("Response Time Over Time (Area Plot)")
        
        # Save plot to BytesIO object
        buffer = BytesIO()
        plt.savefig(buffer, format='png')
        buffer.seek(0)

        # Close plot
        plt.close()

        # Return plot as bytes in response
        return Response(content=buffer.getvalue(), media_type="image/png")



@app.get("/plot/{column}/dist")
async def plot_dist(column:str,max_periods=10):
    global GLOBAL_DF

    sns.histplot(data=GLOBAL_DF, x =column ,hue="event_type",kde=True,color="black")
    buffer = BytesIO()
    plt.savefig(buffer, format='png')
    buffer.seek(0)

    # Close plot
    plt.close()
    return Response(content=buffer.getvalue(), media_type="image/png")



"""
This route is call by the clients that emits a batch of N events. You should process them and store the batch for further analysis...
"""
@app.post("/api/v4/events")
async def process_events(request:Request):
    events            = J.loads(await request.json())
    last_arrival_time =  None
    global INTERARRIVAL_TIMES
    global EVENTS
    global EVENTS_BY_PERIOD
    global PERIOD_WINDOW_TIME_SECS
    global CURRENT_INTERVAL_TIME_WINDOW_START_TIME
    global PERIOD_COUNTER
    global PERIOD_OPERATION_COUNTER
    global OBJECT_TIME_STORE
    global GLOBAL_START_TIME
    global RESPONSES_TIMES
    global GLOBAL_DF
    global OBJECT_ACCESS_BY_PEER

    async with LOCK:
        if GLOBAL_START_TIME == -1:
            GLOBAL_START_TIME = T.time()
        

        for event in events:
            current_time = T.time()
            diff         = 0 if CURRENT_INTERVAL_TIME_WINDOW_START_TIME == -1 else current_time - CURRENT_INTERVAL_TIME_WINDOW_START_TIME
            # 
            if CURRENT_INTERVAL_TIME_WINDOW_START_TIME == -1:
                CURRENT_INTERVAL_TIME_WINDOW_START_TIME = T.time()

            elif diff  > PERIOD_WINDOW_TIME_SECS:
                PERIOD_OPERATION_COUNTER                = 0
                PERIOD_COUNTER                          += 1
                CURRENT_INTERVAL_TIME_WINDOW_START_TIME = T.time()
                
            PERIOD_OPERATION_COUNTER +=1

            event_type:str = event.get("event","UKNOWN")
            logger_level = event.get("level","DEBUG")
            # if event_type == "UKNOWN" or event_type =="GET_BUCKET_METADATA" or event_type == "GET_ALL_BUCKET_METADATA":
            if not event_type.startswith("PUT") or event_type.startswith("GET") or logger_level =="DEBUG":
                continue
            timestamp         = event.get("timestamp",T.time())
            key               = event.get("key","")
            size              = int(event.get("size",0))
            response_time     = float(event.get("response_time",0))
            peer_id           = event.get("peer_id","")
            interarrival_time = 0
            # _event_type = if event_type.sart

            if last_arrival_time == None :
                last_arrival_time = timestamp
            elif last_arrival_time < timestamp:
                interarrival_time = timestamp - last_arrival_time 
                last_arrival_time = timestamp
                
            INTERARRIVAL_TIMES = np.insert(INTERARRIVAL_TIMES,INTERARRIVAL_TIMES.shape[0],interarrival_time)
            RESPONSES_TIMES = np.insert(RESPONSES_TIMES, RESPONSES_TIMES.shape[0], response_time)
            arrival_time = T.time()
            freq = ACCESS_COUNTER_BY_OBJECT.get_frequency_by_key(key=key)
            e = {
                "arrival_time":arrival_time,
                "time":event.get("time",""),
                "timestamp":timestamp,
                "event_type":event_type,
                "bucket_id":event.get("bucket_id",""),
                "key":key,
                "peer_id":peer_id,
                "size":HF.format_size(size),
                "period_arrival_time":diff,
                "mean_interarrival_time": INTERARRIVAL_TIMES.mean(),
                "median_interarrival_time": np.median(INTERARRIVAL_TIMES),
                "std_interarrival_time": INTERARRIVAL_TIMES.std(),
                "interarrival_time": interarrival_time,
                "period_counter":PERIOD_COUNTER,
                "period_operation_counter":PERIOD_OPERATION_COUNTER,
                "global_access_counter":ACCESS_COUNTER_BY_OBJECT.total(),
                "frequency":freq,
                "response_time":response_time
            }
            
            if event_type.startswith("GET"):
                OBJECT_ACCESS_BY_PEER.setdefault(key,{})
                current_access_at_peer = OBJECT_ACCESS_BY_PEER[key].setdefault(peer_id,0)
                OBJECT_ACCESS_BY_PEER[key][peer_id]+=1
                ACCESS_COUNTER_BY_OBJECT.increment_by_key(key=key)
                OBJECT_TIME_STORE.first_access(key=key,timestamp=timestamp)
                rrd_denominator = OBJECT_TIME_STORE.get_diff_current_creation(key=key)

                e["rrd"] = 0 if rrd_denominator ==0 else ACCESS_COUNTER_BY_OBJECT.get_counter_by_key(key=key) / rrd_denominator
                e["diff_ct_lt"] = OBJECT_TIME_STORE.get_diff_current_last_access(key=key)
                e["sve"] = -1 if freq ==0 else e["diff_ct_lt"] * (1/ freq)
            elif event_type.startswith("PUT"):
                OBJECT_TIME_STORE.add_object(key=key, timestamp= arrival_time)
            elif event_type.startswith("DELETE"):
                ACCESS_COUNTER_BY_OBJECT.del_counter_by_key(key=key)
                OBJECT_TIME_STORE.delete_by_key(key=key)
            
            EVENTS.append(e)
            row = {
                "TIMESTAMP":timestamp,
                "EVENT_TYPE":event_type,
                "OBJECT_ID":key,
                "RESPONSE_TIME":response_time,
                "INTERARRIVAL_TIME":interarrival_time,
                **e
            }
            GLOBAL_DF = pd.concat([GLOBAL_DF, pd.DataFrame([row])], ignore_index=True)
            log.info({
                "event":event_type,
                **e
            })
    # print("Event",events)
    return Response(content=None, status_code=204)


# if __name__ == "__main__":
#     uvicorn.run(
#         app= "server:app",
#         host=IP_ADDR, 
#         port=PORT,
#         reload=RELOAD,
#     )
    