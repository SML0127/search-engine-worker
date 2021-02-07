from redis import Redis
from rq.job import Job

class uploadJob(Job):

    def __init__(self, *args, **kwargs):
        super(uploadJob, self).__init__(*args, **kwargs)

