from redis import Redis
from rq.job import Job

class pseJob(Job):

    def __init__(self, *args, **kwargs):
        super(pseJob, self).__init__(*args, **kwargs)

