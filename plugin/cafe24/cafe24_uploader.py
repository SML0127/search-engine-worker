# -*- coding: utf-8 -*-
# pse_worker.py
# made by jinho ko, @ 20190819
# modified by Seung-Min Lee

""" original import """
from plugin.cafe24.single_uploader import Cafe24SingleUploader
from yaml import load, Loader
import errno
import logging
import os
import random
import signal
import socket
import sys
import time
import traceback
import warnings
from datetime import timedelta
from uuid import uuid4

try:
    from signal import SIGKILL
except ImportError:
    from signal import SIGTERM as SIGKILL

from redis import WatchError

from rq import worker_registration
from rq.compat import PY2, as_text, string_types, text_type
from rq.connections import get_current_connection, push_connection, pop_connection

from rq.defaults import (DEFAULT_RESULT_TTL,
                         DEFAULT_WORKER_TTL, DEFAULT_JOB_MONITORING_INTERVAL,
                         DEFAULT_LOGGING_FORMAT, DEFAULT_LOGGING_DATE_FORMAT)
from rq.exceptions import DequeueTimeout, ShutDownImminentException
from rq.job import Job, JobStatus
from rq.logutils import setup_loghandlers
from rq.queue import Queue
from rq.registry import (FailedJobRegistry, FinishedJobRegistry,
                         StartedJobRegistry, clean_registries)
from rq.suspension import is_suspended
from rq.timeouts import JobTimeoutException, HorseMonitorTimeoutException, UnixSignalDeathPenalty
from rq.utils import (backend_class, ensure_list, enum,
                      make_colorizer, utcformat, utcnow, utcparse)
from rq.version import VERSION
from rq.worker_registration import clean_worker_registry, get_keys

try:
    from setproctitle import setproctitle as setprocname
except ImportError:
    def setprocname(*args, **kwargs):  # noqa
        pass

from rq import Worker
from functools import partial
print_flushed = partial(print, flush=True)

""" smlee, jhko import """


green = make_colorizer('darkgreen')
yellow = make_colorizer('darkyellow')
blue = make_colorizer('darkblue')

logger = logging.getLogger(__name__)


def compact(l):
    return [x for x in l if x is not None]


_signames = dict((getattr(signal, signame), signame)
                 for signame in dir(signal)
                 if signame.startswith('SIG') and '_' not in signame)


def signal_name(signum):
    try:
        if sys.version_info[:2] >= (3, 5):
            return signal.Signals(signum).name
        else:
            return _signames[signum]

    except KeyError:
        return 'SIG_UNKNOWN'
    except ValueError:
        return 'SIG_UNKNOWN'


WorkerStatus = enum(
    'WorkerStatus',
    STARTED='started',
    SUSPENDED='suspended',
    BUSY='busy',
    IDLE='idle'
)


class Cafe24Uploader(Worker):
    def __init__(self, *args, **kwargs):
        # TODO define here
        #smlee-error, InitWorkerError("Cannot initialize worker")
        try:
            #self.selenium_manager = SeleniumManager(self.settings)
            # self.selenium_manager.init()
            super(Cafe24Uploader, self).__init__(*args, **kwargs)
            self.uploader = Cafe24SingleUploader()
        except Exception as e:
            print_flushed("-------Raised Exception in WORKER-------")
            print_flushed(e)
            print_flushed("----------------------------------------")
            print_flushed("--------------STACK TRACE---------------")
            print_flushed(str(traceback.format_exc()))
            print_flushed("----------------------------------------")

    # added by smlee
    def handle_warm_shutdown_request(self):
        self.log.info('Warm shut down requested')
        try:
            # self.selenium_manager.close()
            self.log.info('Chromedriver process shutdown.')
            self.uploader.close()
        except Exception as e:
            print_flushed("-------Raised Exception in WORKER-------")
            print_flushed(e)
            print_flushed("----------------------------------------")
            print_flushed("--------------STACK TRACE---------------")
            print_flushed(traceback.format_exc())
            print_flushed("----------------------------------------")
            self.log.info(
                'Chromedriver not found OR process failed to shutdown.')
        finally:
            print_flushed('GOODBYE.')

    def main_work_horse(self, *args, **kwargs):
        raise NotImplementedError("Test worker does not implement this method")

    def execute_job(self, job, queue):
        """Execute job in same thread/process, do not fork()"""
        try:
            self.set_state(WorkerStatus.BUSY)
            timeout = (job.timeout or DEFAULT_WORKER_TTL) + 5
            self.perform_job(job, queue, heartbeat_ttl=timeout)
            self.set_state(WorkerStatus.IDLE)
        except Exception as e:
            pass

    def perform_task(self, task):
        try:
            args = task['args']
            #node_ids = task['mpids']
            #selfuploader = Cafe24SingleUploader()
            rv = self.uploader.upload_products_of_mpid(args, task['mpids'])
            #rv = uploader.upload_products_of_nodes(args, node_ids)
            #rv = uploader.upload_products(args)
            #rv = {}
            #self.lm.logging_task(task, UNKNOWN)
            #op, gvar = self.prepare_task(task)
            # op.run(gvar)
            #self.complete_task(op, gvar)
            # print_flushed(gvar.profiling_info)
        except Exception as e:
            print_flushed("-------Raised Exception in WORKER-------")
            print_flushed("----------------------------------------")
            print_flushed("--------------STACK TRACE---------------")
            print_flushed(str(traceback.format_exc()))
            print_flushed("----------------------------------------")
            raise
        # self.logging_manager.logging_successful_task_detail(task))
        return rv

    # def perform_pse_job(self, job, queue, heartbeat_ttl = None):
    def perform_job(self, job, queue, heartbeat_ttl=None):
        """Performs the actual work of a job.  Will/should only be called
        inside the work horse's process.
        """
        # JHKO. Todo... Redefine the errors and make it hierachically
        #smlee-error, RunStageError("Fait to perform stage")
        # added by smlee
        rv = None
        self.prepare_job_execution(job, heartbeat_ttl)
        push_connection(self.connection)

        started_job_registry = StartedJobRegistry(
            job.origin, self.connection, job_class=self.job_class)

        try:
            """Copied, re-writing code from https://github.com/rq/rq/blob/master/rq/worker.py, commit# : d0884be
            Changes marked at the spot.
            """
            job.started_at = utcnow()
            timeout = job.timeout or self.queue_class.DEFAULT_TIMEOUT
            with self.death_penalty_class(timeout, JobTimeoutException, job_id=job.id):
                # print_flushed(job.args)
                rv = self.perform_task(job.args[0])

            job.ended_at = utcnow()

            # Pickle the result in the same try-except block since we need
            # to use the same exc handling when pickling fails

            job._result = rv
            self.handle_job_success(job=job,
                                    queue=queue,
                                    started_job_registry=started_job_registry)
        except Exception as e:
            job.ended_at = utcnow()
            # added by smlee
            exc_info = sys.exc_info()
            exc_string = ''.join(traceback.format_exception(*exc_info))
            # exc_string = self._get_safe_exception_string(
            #    traceback.format_exception(*exc_info)
            # )
            try:
                self.handle_job_failure(job=job, exc_string=exc_string, queue=queue,
                                        started_job_registry=started_job_registry)
                self.handle_exception(job, *exc_info)
            except:
                raise
            raise
        finally:
            pop_connection()

        self.log.info('%s: %s (%s)', green(job.origin), blue('Job OK'), job.id)
        if rv is not None:
            log_result = "{0!r}".format(as_text(text_type(rv)))
            self.log.debug('Result: %s', yellow(log_result))

        if self.log_result_lifespan:
            result_ttl = job.get_result_ttl(self.default_result_ttl)
            if result_ttl == 0:
                self.log.info('Result discarded immediately')
            elif result_ttl > 0:
                self.log.info('Result is kept for %s seconds', result_ttl)
            else:
                self.log.info(
                    'Result will never expire, clean up result key manually')
