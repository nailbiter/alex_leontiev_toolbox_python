"""===============================================================================

        FILE: /Users/nailbiter/Documents/forgithub/alex_leontiev_toolbox_python/alex_leontiev_toolbox_python/utils/async_batch.py

       USAGE: (not intended to be directly executed)

 DESCRIPTION: 

     OPTIONS: ---
REQUIREMENTS: ---
        BUGS: ---
       NOTES: ---
      AUTHOR: Alex Leontiev (alozz1991@gmail.com)
ORGANIZATION: 
     VERSION: ---
     CREATED: 2023-10-28T18:27:32.673001
    REVISION: ---

==============================================================================="""
from abc import ABC, abstractmethod
import typing


class AsyncJob(ABC):
    @abstractmethod
    def start(self) -> None:
        ...

    @abstractmethod
    def is_running(self) -> bool:
        ...

    @abstractmethod
    def get_result(self):
        ...


class AsyncBatch:
    def __init__(
        self,
        async_jobs: list[AsyncJob],
        capacity: typing.Optional[int] = None,
        dependencies: set[typing.Tuple[int, int]] = set(),
    ):
        self._async_jobs: list[AsyncJob] = async_jobs
        self._capacity: typing.Optional[int] = capacity
        self._dependencies: set[typing.Tuple[int, int]] = dependencies
        # assert len(dependencies) == 0, "FIXME"
        self._running_jobs_idxs: set = set()
        self._job_results: dict = {}

    def is_all_prerequisites_done(self, job_i: int) -> bool:
        for i, j in self._dependencies:
            if (j == job_i) and (i not in self._job_results):
                return False
        return True

    def rotate(self) -> bool:
        """
        @return:bool -- is_running
        """
        for i in list(self._running_jobs_idxs):
            job = self._async_jobs[i]
            if not job.is_running():
                self._job_results[i] = job.get_result()
                self._running_jobs_idxs.discard(i)

        remaining_capacity = (
            None
            if self._capacity is None
            else max(0, self._capacity - len(self._running_jobs_idxs))
        )
        scheduled = 0
        for i, job in enumerate(self._async_jobs):
            if scheduled >= remaining_capacity:
                break
            if (
                (i not in self._running_jobs_idxs)
                and (i not in self._job_results)
                and self.is_all_prerequisites_done(i)
            ):
                scheduled += 1
                job.start()
                self._running_jobs_idxs.add(i)
