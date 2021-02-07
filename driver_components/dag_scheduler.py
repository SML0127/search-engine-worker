import json

from driver_components.task_scheduler import *
import traceback

class DagScheduler():

    def schedule(self, program):
        return program['ops']

    def run(self, program):
        stages = self.schedule(program)
        results = {}
        task_scheduler = TaskScheduler()
        for level, stage in enumerate(stages, 1):
            stage_id = program['lm'].start_stage(program['execution_id'], level)
            try:
                stage['stage_id'] = stage_id
                stage['db_conn'] = program['data_db_conn']
                stage['log_conn'] = program['log_db_conn']
                stage['execution_id'] = program['execution_id']
                stage['zipcode_url'] = program['ops'][0].get('zipcode_url', None)
                task_scheduler.run(program['rm'], stage, results, None)
            except Exception as e:
                program['lm'].end_stage(stage_id, {"status": -1, "error": str(traceback.format_exc())})
                raise e
            program['lm'].end_stage(stage_id, {"status": 1})

    def rerun(self, program, previous_eid):
        stages = self.schedule(program)
        results = {}
        task_scheduler = TaskScheduler()
        for level, stage in enumerate(stages, 1):
            stage_id = program['lm'].start_stage(program['execution_id'], level)
            previous_tasks = program['lm'].get_failed_tasks_of_level(previous_eid, level)
            print("rerun {} tasks at level {}", len(previous_tasks), level)
            try:
                stage['stage_id'] = stage_id
                stage['db_conn'] = program['data_db_conn']
                stage['log_conn'] = program['log_db_conn']
                stage['execution_id'] = program['execution_id']
                stage['zipcode_url'] = program['ops'][0]['zipcode_url']
                task_scheduler.run(program['rm'], stage, results, previous_tasks)
            except Exception as e:
                program['lm'].end_stage(stage_id, {"status": -1, "error": str(traceback.format_exc())})
                raise e
            program['lm'].end_stage(stage_id, {"status": 1})

if __name__ == "__main__":
    pass
