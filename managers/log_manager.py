from datetime import datetime, timedelta

import psycopg2
from yaml import load, Loader
import os
import json
import traceback


class LogMgrErr(Exception):

    def __init__(self, error):
        self.error = error

    def __str__(self):
        return str("WebMgrErr: \n") + str(self.error)


class LogManager():

    def init(self, settings):
        try:
            self.conn_info = settings['log_db_conn_info']
            self.conn = psycopg2.connect(self.conn_info)
            self.cur = self.conn.cursor()
        except Exception as e:
            raise LogMgrErr(e)

    def close(self):
        try:
            self.conn.close()
        except Exception as e:
            raise LogMgrErr(e)

    def save_program(self, name, program):
        try:
            query = "insert into program "
            query += "(name, program) "
            query += "values(%s, %s) returning id;"
            self.cur.execute(query, (name, json.dumps(program)))
            result = self.cur.fetchone()[0]
            self.conn.commit()
            return result
        except Exception as e:
            self.conn.rollback()
            raise LogMgrErr(e)


    def load_program(self, program_id):
        try:
            query = "select program from user_program where id = {};".format(program_id)
            self.cur.execute(query)
            result = self.cur.fetchone()[0]
            result['data_db_conn'] = result.pop('dataDb')
            result['log_db_conn'] = result.pop('logDb')
            return result
        except Exception as e:
            self.conn.rollback()
            raise LogMgrErr(e)



    def load_programOLD(self, program_id):
        try:
            query = "select program from program where id = {};".format(program_id)
            self.cur.execute(query)
            result = self.cur.fetchone()[0]
            self.conn.commit()
            return json.loads(result)
        except Exception as e:
            self.conn.rollback()
            raise LogMgrErr(e)

    def get_lastest_execution_id_using_job_id(self, job_id):
        try:
            query = "select id from execution where job_id = {} order by id desc limit 1;".format(job_id)
            self.cur.execute(query)
            result = self.cur.fetchone()[0]
            self.conn.commit()
            return result
        except Exception as e:
            self.conn.rollback()
            raise LogMgrErr(e)

    def check_existing_source_view_using_job_id(self, job_id):
        try:
            query = "select count(*) from job_source_view where job_id = {}".format(job_id)
            #query = "select count(*) from execution where job_id = {}".format(job_id)
            self.cur.execute(query)
            result = self.cur.fetchone()[0]
            self.conn.commit()
            if result >= 1:
               return True
            else:         
               return False
        except Exception as e:
            self.conn.rollback()
            raise LogMgrErr(e)



    def load_program_of_execution(self, execution_id):
        try:
            query = "select p.program from program p, execution e "
            query += "where e.program_id = p.id and e.id = %s;"
            self.cur.execute(query, (str(execution_id)))
            result = self.cur.fetchone()[0]
            self.conn.commit()
            return result
        except Exception as e:
            self.conn.rollback()
            raise LogMgrErr(e)

    def save_category(self, name, category):
        try:
            query = "insert into category "
            query += "(name, category) "
            query += "values(%s, %s) returning id;"
            self.cur.execute(query, (name, json.dumps(category)))
            result = self.cur.fetchone()[0]
            self.conn.commit()
            return result
        except Exception as e:
            self.conn.rollback()
            raise LogMgrErr(e)

    def load_category(self, category_id):
        try:
            query = "select category from category where id = {};".format(category_id)
            self.cur.execute(query)
            result = self.cur.fetchone()[0]
            self.conn.commit()
            return json.loads(result)
        except Exception as e:
            self.conn.rollback()
            raise LogMgrErr(e)


    def start_execution(self, program_id, previous_id, job_id):
        try:

            time_gap = timedelta(hours=9)
            start_time = datetime.utcnow() + time_gap 
            query = "insert into execution "
            query += "(program_id, previous_id, start_time, job_id) "
            query += "values(%s, %s, %s, %s) returning id;"
            self.cur.execute(query, (str(program_id), str(previous_id),
                                      str(start_time), str(job_id)))
            result = self.cur.fetchone()[0]
            print("start execution:  ", result)
            print("  job_id: ", job_id)
            print("  program_id: ", program_id)
            print("  previous_id: ", previous_id)
            print("  start_time: ", start_time)
            self.conn.commit()
            return result
        except Exception as e:
            self.conn.rollback()
            raise LogMgrErr(e)

    def start_executionOLD(self, program_id, previous_id, category_id, category_no, job_id):
        try:
            time_gap = timedelta(hours=9)
            start_time = datetime.utcnow() + time_gap
            query = "insert into execution "
            query += "(program_id, previous_id, category_id, category_no, start_time, job_id) "
            query += "values(%s, %s, %s, %s, %s, %s) returning id;"
            self.cur.execute(query, (str(program_id), str(previous_id),
                                     str(category_id), str(category_no), str(start_time), str(job_id)))
            result = self.cur.fetchone()[0]
            print("start execution:  ", result)
            print("  job_id: ", job_id)
            print("  program_id: ", program_id)
            print("  previous_id: ", previous_id)
            print("  category_id: ", category_id)
            print("  category_no: ", category_no)
            print("  start_time: ", start_time)
            self.conn.commit()
            return result
        except Exception as e:
            self.conn.rollback()
            raise LogMgrErr(e)

    def end_execution(self, execution_id, output):
        try:
            time_gap = timedelta(hours=9)
            end_time = datetime.utcnow() + time_gap
            print("end execution:  ", execution_id)
            print("  end_time: ", end_time)
            query = "update execution "
            query += "set end_time = %s "
            query += "where id = %s;"
            self.cur.execute(query, (str(end_time), str(execution_id)))
            query =  "insert into execution_output "
            query += "(execution_id, output) "
            query += "values(%s, %s);"
            self.cur.execute(query, (str(execution_id), json.dumps(output)))
            self.conn.commit()
        except Exception as e:
            self.conn.rollback()
            raise LogMgrErr(e)

    def start_stage(self, execution_id, level):
        try:
            time_gap = timedelta(hours=9)
            start_time = datetime.utcnow() + time_gap
            query = "insert into stage "
            query += "(execution_id, level, start_time) "
            query += "values(%s, %s, %s) returning id;"
            self.cur.execute(query, (str(execution_id), str(level), str(start_time)))
            result = self.cur.fetchone()[0]
            print("start stage:  ", result)
            print("  level: ", level)
            print("  start_time: ", start_time)
            self.conn.commit()
            return result
        except Exception as e:
            self.conn.rollback()
            raise LogMgrErr(e)

    def end_stage(self, stage_id, output):
        try:
            time_gap = timedelta(hours=9)
            end_time = datetime.utcnow() + time_gap
            print("end stage:  ", stage_id)
            print("  end_time: ", end_time)
            query = "update stage "
            query += "set end_time = %s "
            query += "where id = %s;"
            self.cur.execute(query, (str(end_time), str(stage_id)))
            query = "insert into stage_output "
            query += "(stage_id, output) "
            query += "values(%s, %s);"
            self.cur.execute(query, (str(stage_id), json.dumps(output)))
            self.conn.commit()
        except Exception as e:
            self.conn.rollback()
            raise LogMgrErr(e)

    def get_failed_tasks_of_level(self, execution_id, level):
        try:
            query = "select t.id, ti.input "
            query += "from stage s, task t, task_input ti "
            query += "where s.level = %s and s.execution_id = %s and t.status < 1 "
            query += "and s.id = t.stage_id and ti.task_id = t.id;"
            self.cur.execute(query, (str(level), str(execution_id)))
            result = self.cur.fetchall()
            self.conn.commit()
            return result
        except Exception as e:
            self.conn.rollback()
            raise LogMgrErr(e)

    def start_task(self, stage_id, parent_id, previous_id, url):
        try:
            time_gap = timedelta(hours=9)
            start_time = datetime.utcnow() + time_gap
            status = 0
            query = "insert into task "
            query += "(stage_id, parent_id, previous_id, status, start_time) "
            query += "values(%s, %s, %s, %s, %s) returning id;"
            self.cur.execute(query, (str(stage_id), str(parent_id), str(previous_id), str(status), str(start_time)))
            result = self.cur.fetchone()[0]
            query = "insert into task_input "
            query += "(task_id, input) "
            query += "values(%s, %s);"
            self.cur.execute(query, (str(result), json.dumps(url)))
            self.conn.commit()
            return result
        except Exception as e:
            self.conn.rollback()
            raise LogMgrErr(e)

    def end_task(self, task_id, status, output):
        try:
            time_gap = timedelta(hours=9)
            end_time = datetime.utcnow() + time_gap
            query = "update task "
            query += "set end_time=%s, status =%s "
            query += "where id = %s;"
            self.cur.execute(query, (str(end_time), str(status), str(task_id)))
            query = "insert into task_output "
            query += "(task_id,output) "
            query += "values(%s,%s);"
            self.cur.execute(query, (str(task_id), json.dumps(output)))
            self.conn.commit()
        except Exception as e:
            self.conn.rollback()
            raise LogMgrErr(e)


if __name__ == '__main__':
    log_manager = LogManager()
    log_manager.init({"log_db_conn_info": "host=127.0.0.1 port=5432 user=pse password=pse dbname=pse"})

    execution_id = log_manager.start_execution(0, 0, 0)
    stage_id = log_manager.start_stage(execution_id, 0)
    task_id = log_manager.start_task(stage_id, 0, 0, "http://test")
    log_manager.end_task(task_id, 1, "task test")
    log_manager.end_stage(stage_id, "stage test")
    log_manager.end_execution(execution_id, "stage test")
