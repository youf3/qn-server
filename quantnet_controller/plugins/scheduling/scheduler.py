import aioschedule as schedule
import asyncio
import logging
from queue import Queue
from quantnet_mq import Code

logger = logging.getLogger("plugins.scheduler")


class JobDesciption:
    def __init__(self, job, repeat, interval, duration) -> None:
        self.job = job
        self.repeat = repeat
        self.interval = interval
        self.duration = duration


class Scheduler:
    def __init__(self):
        self._jobs = Queue()
        self._jobs.put(None)

    async def run(self, interval=1):
        while True:
            await schedule.run_pending()
            await asyncio.sleep(interval)
            # check if job repeat counter is less than 1, and remove the job
            while True:
                jobdisc = self._jobs.get()
                if jobdisc is None:
                    self._jobs.put(None)
                    break
                elif jobdisc.repeat[0] < 1:
                    schedule.cancel_job(jobdisc.job)
                else:
                    self._jobs.put(jobdisc)

    def start(self):
        logger.info("Scheduler is started")
        asyncio.create_task(self.run())

    async def schedule(self, func, args, repeat: int = 1, interval: int = 6, duration: int = 5, blocking: bool = True):
        logger.info(
            f"Scheduling {func.__name__} for {repeat} times {interval}s interval, assuming each takes {duration}s"
        )

        if repeat <= 0:
            raise Exception("repeat is only allowed >= 1")
        elif interval < 0 or interval > 2629746:
            raise Exception(
                "interval is only allowed between 1 and 2629746 sec (1 month)")
        elif interval < duration:
            raise Exception("interval should be larger than duration")
        else:
            # run once
            if blocking:
                await func(args)
            else:
                asyncio.create_task(func(args))
            if repeat == 1:
                return Code.OK

            # change repeat counter to a mutable object
            repeat = [repeat - 1]

            async def func_wrapper(func, args):
                await func(args)
                repeat[0] = repeat[0] - 1
                # not working in aioschedule
                # workaround: create a queue to maintain counter and decrement each time the function is called
                return schedule.cancel_job

            job = schedule.every(interval).seconds.do(func_wrapper, func, args)
            jobdesc = JobDesciption(job, repeat, interval, duration)
            self._jobs.put(jobdesc)
            return Code.OK

    def stop(self):
        logger.info("Stopping Scheduler")
        self.stop_run_continuously.set()
