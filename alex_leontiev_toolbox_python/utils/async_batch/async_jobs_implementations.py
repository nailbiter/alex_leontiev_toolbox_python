"""===============================================================================

        FILE: /Users/nailbiter/Documents/forgithub/alex_leontiev_toolbox_python/alex_leontiev_toolbox_python/utils/async_batch/async_jobs_implementations.py

       USAGE: (not intended to be directly executed)

 DESCRIPTION: 

     OPTIONS: ---
REQUIREMENTS: ---
        BUGS: ---
       NOTES: ---
      AUTHOR: Alex Leontiev (alozz1991@gmail.com)
ORGANIZATION: 
     VERSION: ---
     CREATED: 2023-10-28T18:59:34.932629
    REVISION: ---

==============================================================================="""

from alex_leontiev_toolbox_python.utils.async_batch import AsyncJob
from datetime import datetime, timedelta


class SleepJob(AsyncJob):
    def __init__(self, sleep_seconds: int):
        self._sleep_seconds = sleep_seconds
        self._started = None

    def start(self):
        self._started = datetime.now()

    def is_running(self):
        return ((datetime.now() - self._started).total_seconds()) <= self._sleep_seconds

    def get_result(self):
        return self._sleep_seconds
