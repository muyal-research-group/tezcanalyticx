import os
import time as T
import pandas as pd
import unittest
import logging
import scipy.stats as S
from mictlanx.logger.tezcanalyticx.tezcanalyticx import TezcanalyticXHttpHandler
TEZCANALYTICX_URL = os.environ.get("TEZCANALYTICX_URL","localhost:45000")
logger = logging.getLogger("tezcanalyticx")
logger.setLevel(logging.DEBUG)
logger.addHandler(
    TezcanalyticXHttpHandler(
        flush_timeout="10s",
        buffer_size=100,
        path="/api/v4/events",
        port=45000,
        hostname="localhost",
        protocol="http"
    )
)

class BasicWorkloadTestSuite(unittest.TestCase):

    def test_10operation(self):
        
        workload = pd.read_csv("./workload1.csv")
        for (i,row) in workload.iterrows():
            operation_type = row["OPERATION_TYPE"]
            service_time   = S.expon.rvs()
            key            = row["KEY"]
            if operation_type =="PUT":
                logger.info(
                    {
                        "operation_type":"PUT",
                        "key":key,
                        "arrival_time":str(T.time()),
                        "service_time":service_time,
                    })
                # T.sleep(20)
            else:
                logger.info(
                    {
                        "operation_type":"GET",
                        "arrival_time":str(T.time()),
                        "service_time":service_time
                    }
                )
            T.sleep(.5)
            # print(row)
        print("DONE")
        T.sleep(100)

if __name__ =="__main__":
    unittest.main()