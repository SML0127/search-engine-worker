import traceback
import time
from managers.log_manager import *
from driver_components.operators import *

class TaskScheduler():


    def run(self, rm, stage, results, previous_tasks):
        mstage = materialize(stage)
        mstage.run(rm, results, previous_tasks)
