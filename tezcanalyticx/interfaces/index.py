from typing import Dict
import time as T
from dataclasses import dataclass
import numpy as np
# from pyndan

class Counter(object):
    def __init__(self):
        self.__counter:Dict[str,int] = {}
    def increment_by_key(self,key:str):
        self.__counter.setdefault(key,0)
        self.__counter[key] = self.__counter[key]+1
    def get_counter_by_key(self,key:str):
        return self.__counter.get(key,0)
    def get_frequency_by_key(self,key:str):
        count = self.get_counter_by_key(key=key)
        total = self.total()
        if total ==0:
            return 0.0
        return count/total
        
    def del_counter_by_key(self,key:str):
        if key in self.__counter:
            return self.__counter.pop(key)
        return 0 
    
    def get_counter(self):
        return self.__counter
    def get_sorted_counter(self):
        sorted_dict = dict(sorted(self.__counter.items(), key=lambda item: item[1], reverse=True))
        return sorted_dict
    
    def get_counter_frequency(self):
        xs = {}
        t = self.total()
        for key,value in self.__counter.items():
            if t == 0:
                xs[key] =0
            else:
                xs[key] = (value / t )
        # sorted()
        sorted_dict = dict(sorted(xs.items(), key=lambda item: item[1], reverse=True))
        return sorted_dict
        # return self.__counter
    def total(self):
        return sum(value for value in self.__counter.values())


@dataclass
class ObjectTime:
    created_at: float = -1
    first_access_at: float= -1
    last_access_at: float = -1


class ObjectTimeStore(object):
    def __init__(self):
        self.__store:Dict[str,ObjectTime] = {}
    def add_object(self,key:str, timestamp:float):
        self.__store[key] = ObjectTime(created_at=timestamp, first_access_at=-1,last_access_at=-1)
    
    def get_object_time_by_key(self,key:str):
        return self.__store.get(key,ObjectTime())
    def get_diff_current_creation(self,key:str)->float:
        ot = self.__store.get(key, ObjectTime())
        if ot.created_at == -1 or ot.last_access_at==-1:
            return 0
        return T.time() - ot.created_at

    def get_diff_current_last_access(self, key:str):
        ot = self.__store.get(key, ObjectTime())
        if ot.created_at == -1 or ot.last_access_at==-1:
            return 0
        return np.abs(T.time() - ot.last_access_at)
    def delete_by_key(self,key:str):
        return self.__store.pop(key)

    def first_access(self,key:str, timestamp:float):
        if  key in self.__store:
            obj = self.__store.get(key)
            if obj.first_access_at == -1:
                obj.first_access_at = timestamp
                obj.last_access_at = timestamp
            else:
                obj.last_access_at = timestamp
            
            self.__store[key] = obj
        else:
            self.add_object(key=key, timestamp=timestamp)
            # self.__store.update({[key]: obj})
    def get_store(self):
        return self.__store