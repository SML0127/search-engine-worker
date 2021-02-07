import time
import copy
from yaml import load, Loader
import os
import time
import psycopg2
import sys
import traceback
import argparse

from managers.redis_manager import *
from managers.settings_manager import *
from managers.log_manager import LogManager

from driver_components.dag_scheduler import *
from driver_components.task_scheduler import *



class PseDriver():

    def __init__(self):
        pass

    def init(self):
        self.setting_manager = SettingsManager()
        self.setting_manager.setting("settings-driver.yaml")
        settings = self.setting_manager.get_settings()
        self.log_manager = LogManager()
        self.log_manager.init(settings)

    def close(self):
        self.log_manager.close()


    def init_program(self, program):
        try:
            program['usr_msg'] = []
            program['err_msg'] = []
            program['lm'] = self.log_manager
            program['rm'] = RedisManager()
            program['rm'].connect(self.setting_manager.get_settings())
            program['rm'].create_rq(program['rm'].get_connection(), program['queue'])
        except:
            raise

    def load_category_from_file(self, fname):
        return json.load(open(fname))

    def load_program_from_file(self, fname):
        return json.load(open(fname))

    def load_program_from_db(self, program_id):
        return self.log_manager.load_program(program_id)

    def load_category_from_db(self, category_id):
        return self.log_manager.load_category(category_id)

    def save_program_from_file_to_db(self, args):
        program = self.load_program_from_file(args.wf)
        if args.url != None:
            program['ops'][0]['url'] = args.url
        if args.max_page != None:
            program['ops'][1]['max_num_tasks'] = args.max_page
        program_id = self.log_manager.save_program(str(args.wfn), args.wf)
        print("Program {} is saved: id - {}".format(str(args.wfn), program_id))
        return program, program_id

    def save_category_from_file_to_db(self, args):
        category = self.load_category_from_file(args.ct)
        category_id = self.log_manager.save_category(str(args.ctn), args.ct)
        print("Category {} is saved: id - {}".format(str(args.ctn), category_id))
        return category, category_id

    def run(self, program, eid):
        try:
            self.init_program(program)
            program['lm'] = self.log_manager
            program['execution_id'] = eid
            dag_scheduler = DagScheduler()
            dag_scheduler.run(program)
        except Exception as e:
            print("-------Raised Exception in DRIVER-------")
            print(e)
            print("---------------------------------------")
            print("--------------STACK TRACE--------------")
            print(str(traceback.format_exc()))
            print("---------------------------------------")
            raise e

    def rerun(self, program, previous_eid, eid):
        try:
            self.init_program(program)
            program['lm'] = self.log_manager
            program['execution_id'] = eid
            dag_scheduler = DagScheduler()
            dag_scheduler.rerun(program, previous_eid)
        except Exception as e:
            print("-------Raised Exception in DRIVER-------")
            print(e)
            print("---------------------------------------")
            print("--------------STACK TRACE--------------")
            print(str(traceback.format_exc()))
            print("---------------------------------------")
            raise e

    def register_execution(self, prog_name, program, category):
        program_id = self.log_manager.save_program(prog_name, program)

    def run_from_file(self, args):
        category, cid = self.save_category_from_file_to_db(args)
        program, pid = self.save_program_from_file_to_db(args)
        eid = self.log_manager.start_execution(pid,0,cid,args.cno)
        try:
            self.run(program, eid)
        except Exception as e:
            self.log_manager.end_execution(eid, {"status": -1, "error": str(traceback.format_exc())})
            raise e
        self.log_manager.end_execution(eid, {"status": 1})
        return eid

    def run_from_db(self, args):
        program = self.load_program_from_db(args.wf)
        print(program)
        eid = self.log_manager.start_execution(args.wf,0,args.ct,args.cno)
        try:
            self.run(program, eid)
        except Exception as e:
            self.log_manager.end_execution(eid, {"status": -1, "error": str(traceback.format_exc())})
            raise e
        self.log_manager.end_execution(eid, {"status": 1})
        return eid

    def run_execution(self, args):
        program = self.log_manager.load_program_of_execution(args.eid)
        try:
            self.run(program, args.eid)
        except Exception as e:
            self.log_manager.end_execution(args.eid, {"status": -1, "error": str(traceback.format_exc())})
            raise e
        self.log_manager.end_execution(args.eid, {"status": 1})
        return args.eid

    def rerun_execution_from_db(self, args):
        program = self.load_program_from_db(args.wf)
        print(program)
        eid = self.log_manager.start_execution(args.wf, args.eid, args.ct, args.cno)
        try:
            self.rerun(program, args.eid, eid)
        except Exception as e:
            self.log_manager.end_execution(eid, {"status": -1, "error": str(traceback.format_exc())})
            raise e
        self.log_manager.end_execution(eid, {"status": 1})
        return eid

    def rerun_execution_from_file(self, args):
        category, cid = self.save_category_from_file_to_db(args)
        program, pid = self.save_program_from_file_to_db(args)
        eid = self.log_manager.start_execution(pid, args.eid, cid, args.cno)
        try:
            self.rerun(program, args.eid, eid)
        except Exception as e:
            self.log_manager.end_execution(eid, {"status": -1, "error": str(traceback.format_exc())})
            raise e
        self.log_manager.end_execution(eid, {"status": 1})
        return eid

    def execute(self):
        try:
            parser = argparse.ArgumentParser()
            parser.add_argument('--c', required=False, help='')
            parser.add_argument('--eid', required=False)
            parser.add_argument('--wf', required=False)
            parser.add_argument('--ct', required=False)
            parser.add_argument('--cno', required=False)
            parser.add_argument('--url', required=False)
            parser.add_argument('--wfn', required=False)
            parser.add_argument('--ctn', required=False)
            parser.add_argument('--max_page', required=False)
            args, unknown = parser.parse_known_args()
            print(args.ctn)
            if args.c == 'run_execution':
                return self.run_execution(args)
            elif args.c == 'rerun_execution_from_db':
                return self.rerun_execution_from_db(args)
            elif args.c == 'rerun_execution_from_file':
                return self.rerun_execution_from_file(args)
            elif args.c == 'run_from_file':
                return self.run_from_file(args)
            elif args.c == 'run_from_db':
                return self.run_from_db(args)
            elif args.c == 'save_workflow':
                return self.save_program_from_file_to_db(args)
            elif args.c == 'save_category':
                return self.save_category_from_file_to_db(args)
        except Exception as e:
            print(str(traceback.format_exc()))
            raise e


if __name__ == "__main__":
    driver = PseDriver()
    driver.init()
    try:
        driver.execute()
    except Exception as e:
        pass
    driver.close()