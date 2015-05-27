import unittest
import asyncio
import sys
from time import sleep

from artiq import *
from artiq.master.scheduler import Scheduler


class EmptyExperiment(Experiment, AutoDB):
    def run(self):
        pass


class BackgroundExperiment(Experiment, AutoDB):
    def run(self):
        while True:
            self.scheduler.pause()
            sleep(0.2)


def _get_expid(name):
    return {
        "file": sys.modules[__name__].__file__,
        "experiment": name,
        "arguments": dict()
    }


def _get_basic_steps(rid, expid):
    return [
        {"action": "setitem", "key": rid, "value": 
            {"pipeline": "main", "status": "pending", "priority": 0,
            "expid": expid, "due_date": None}, "path": []},
        {"action": "setitem", "key": "status", "value": "preparing",
            "path": [rid]},
        {"action": "setitem", "key": "status", "value": "prepare_done",
            "path": [rid]},
        {"action": "setitem", "key": "status", "value": "running",
            "path": [rid]},
        {"action": "setitem", "key": "status", "value": "run_done",
            "path": [rid]},
        {"action": "setitem", "key": "status", "value": "analyzing",
            "path": [rid]},
        {"action": "setitem", "key": "status", "value": "analyze_done",
            "path": [rid]},
        {"action": "delitem", "key": rid, "path": []}
    ]


_handlers = {
    "init_rt_results": lambda description: None
}


class SchedulerCase(unittest.TestCase):
    def test_steps(self):
        scheduler = Scheduler(0, _handlers)
        expid = _get_expid("EmptyExperiment")

        expect = _get_basic_steps(0, expid)
        done = asyncio.Event()
        expect_idx = 0
        def notify(notifier, mod):
            nonlocal expect_idx
            self.assertEqual(mod, expect[expect_idx])
            expect_idx += 1
            if expect_idx >= len(expect):
                done.set()
        scheduler.notifier.publish = notify

        loop = asyncio.get_event_loop()
        scheduler.start()
        scheduler.submit("main", expid, 0, None)
        loop.run_until_complete(done.wait())
        loop.run_until_complete(scheduler.stop())

    def test_pause(self):
        scheduler = Scheduler(0, _handlers)
        expid_bg = _get_expid("BackgroundExperiment")
        expid = _get_expid("EmptyExperiment")

        expect = _get_basic_steps(1, expid)
        background_running = asyncio.Event()
        done = asyncio.Event()
        expect_idx = 0
        def notify(notifier, mod):
            nonlocal expect_idx
            if mod == {"path": [0],
                       "value": "running",
                       "key": "status",
                       "action": "setitem"}:
                background_running.set()
            if mod["path"] == [1] or (mod["path"] == [] and mod["key"] == 1):
                self.assertEqual(mod, expect[expect_idx])
                expect_idx += 1
                if expect_idx >= len(expect):
                    done.set()
        scheduler.notifier.publish = notify

        loop = asyncio.get_event_loop()
        scheduler.start()
        scheduler.submit("main", expid_bg, -99, None)
        loop.run_until_complete(background_running.wait())
        scheduler.submit("main", expid, 0, None)
        loop.run_until_complete(done.wait())
        loop.run_until_complete(scheduler.stop())
