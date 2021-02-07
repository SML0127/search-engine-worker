from redis import Redis
from rq import Queue
from rq.job import Job

from util.pse_errors import *

def dummy_func():
    pass

class dummyJob(Job):

    def __init__(self, *args, **kwargs):
        super(dummyJob, self).__init__(*args, **kwargs)

    def perform(self):
        raise DeprecationWarning
        pass


class RedisManager():
       
    def __init__(self):
        self.redis_conn = None
        self.q = None

    def get_connection(self):
        return self.redis_conn

    def connect(self, settings):
        try:
            port = int(settings.get('redis_port', 6379))
            host = settings.get('redis_host', '127.0.0.1')
            self.redis_conn = Redis(host=host,port=port)
        except Exception as e:
            raise RedisError(e)
    
    def create_rq(self, redis_conn, queue_name):
        try:
            self.q = Queue(connection=redis_conn, name=queue_name, job_class="managers.redis_manager.dummyJob", default_timeout=2592000)
        except Exception as e:
            raise RedisError(e)
        return self.q 
    
    def enqueue(self, task):
        try:
            stage = self.q.enqueue(dummy_func, task)
        except Exception as e:
            raise RedisError(e)
        return stage 
        
    def get_result(self, job):
        try:
            return job.result
        except Exception as e:
            raise RedisError(e)
    
    def get_status(self, job):
        try:
            return job.get_status()
        except Exception as e:
            raise RedisError(e)
            
    def num_results(self,job):
        try:
            return len(job.result)
        except Exception as e:
            raise RedisError(e)

    def get_type_of_result(self, job):
        try:
            return type(job.result)
        except Exception as e:
            raise RedisError(e)

    def set_result(self, job, result):
        try:
            job._result = result
        except Exception as e:
            raise RedisError(e)


