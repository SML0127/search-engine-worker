import sys
import os
from flask import Flask
from flask_restful import Resource, Api
from flask_restful import reqparse
import json
import psycopg2
from flask_cors import CORS
#from demo_test_execution import *
#from demo_rerun_module import *
import subprocess
import traceback
import demjson
#import multiprocessing
import json
import decimal
import zlib
from datetime import date
from datetime import datetime

#from pse_driver import *


class CategoryManager(Resource):
    def get_category(self):
        try:
            cur = conn.cursor()
            cur.execute("select category from category;")
            result = cur.fetchall()
            conn.commit()
            return { "success": True, "output" : result }
        except:
            conn.rollback()
            return { "success": False }

    def save_category(self, category):
        try:
            print(category)
            cur = conn.cursor()
            sql = "update category set category = '"
            sql += category
            sql += "' where id = 1"
            print(sql)
            cur.execute(sql)
            conn.commit()

            return { "success": True }
        except:
            conn.rollback()
            return { "success": False, "traceback": str(traceback.format_exc()) }       

    def post(self):
        parser = reqparse.RequestParser()
        parser.add_argument('req_type')
        parser.add_argument('category')
        args = parser.parse_args()
        if args['req_type'] == 'save_category':
            return self.save_category(args['category']);
        if args['req_type'] == 'get_category':
            return self.get_category();
        return { "success": False }

class TransformManager(Resource):
    def get_transforms(self):
        try:
            cur = conn.cursor()
            cur.execute("select id, transform from transform order by id asc;")
            result = cur.fetchall()
            conn.commit()
            return { "success": True, "output" : result }
        except:
            conn.rollback()
            return { "success": False }

    def get_columns_and_tree(self, transform_id):
        try:
            cur = conn.cursor()
            sql = "select columns_and_tree from transform "
            sql += "where id = "
            sql += transform_id + ";"
            cur.execute(sql)
            print(sql)
            result = cur.fetchall()
            conn.commit()
            return { "success": True, "output" : result }
        except:
            conn.rollback()
            return { "success": False, "traceback": str(traceback.format_exc()) }       
  
    def delete_transform(self, transform_id):
        try:
            cur = conn.cursor()
            sql = 'delete from transform where id = '
            sql += transform_id + ';'
            cur.execute(sql)
            conn.commit()
            return { "success": True}
        except:
            conn.rollback()
            return { "success": False, "traceback": str(traceback.format_exc()) }


    def add_transform(self):
        try:
            cur = conn.cursor()
            values=[""]
            sql = make_query_insert_and_returning_id("transform", ["transform"], values, "id")
            cur.execute(sql, values)
            result = cur.fetchone()[0]
            conn.commit()
            return { "success": True, "output":result }
        except:
            conn.rollback()
            return { "success": False, "traceback": str(traceback.format_exc()) }

    def update_transform(self, transform, columns_and_tree, transform_id):
        try:
            cur = conn.cursor()
            columns_and_tree = columns_and_tree.replace("'", '"')
            sql = "update transform set transform = '"
            sql += transform
            sql += "', columns_and_tree = '"
            sql += columns_and_tree
            sql += "' where id = "
            sql += transform_id + ";"
            print(sql)
            cur.execute(sql)
            conn.commit()
            return { "success": True }
        except:
            conn.rollback()
            return { "success": False, "traceback": str(traceback.format_exc()) }

    def post(self):
        parser = reqparse.RequestParser()
        parser.add_argument('req_type')
        parser.add_argument('transform')
        parser.add_argument('transform_id')
        parser.add_argument('columns_and_tree')
        args = parser.parse_args()
        if args['req_type'] == 'add_transform':
            return self.add_transform();
        if args['req_type'] == 'delete_transform':
            return self.delete_transform(args['transform_id']);
        if args['req_type'] == 'update_transform':
            return self.update_transform(args['transform'],args['columns_and_tree'],args['transform_id']);
        if args['req_type'] == 'get_transforms':
            return self.get_transforms();
        if args['req_type'] == 'get_columns_and_tree':
            return self.get_columns_and_tree(args['transform_id']);
        return { "success": False }



class ObjectManager(Resource):
    def get_object_tree(self):
        try:
            cur = conn.cursor()
            cur.execute("select object_tree from object_tree;")
            result = cur.fetchall()
            conn.commit()
            return { "success": True, "output" : result }
        except:
            conn.rollback()
            return { "success": False }

    def save_object_tree(self, object_tree):
        try:
            #print(category)
            cur = conn.cursor()
            sql = "update object_tree set object_tree = '"
            sql += object_tree
            sql += "' where id = 1"
            #print(sql)
            cur.execute(sql)
            conn.commit()

            return { "success": True }
        except:
            conn.rollback()
            return { "success": False, "traceback": str(traceback.format_exc()) }       

    def post(self):
        parser = reqparse.RequestParser()
        parser.add_argument('req_type')
        parser.add_argument('object_tree')
        args = parser.parse_args()
        if args['req_type'] == 'save_object_tree':
            return self.save_object_tree(args['object_tree']);
        if args['req_type'] == 'get_object_tree':
            return self.get_object_tree();
        return { "success": False }

class UserProgramManager(Resource):
    def get_user_program(self):
        try:
            cur = conn.cursor()
            cur.execute("select id, site, category, program from user_program;")
            result = cur.fetchall()
            conn.commit()
            return { "success": True, "output" : result }
        except:
            conn.rollback()
            return { "success": False }

    def get_last_user_program(self):
        try:
            query = "select id, user_program from user_program order by id desc limit 1;"
            cur = conn.cursor()
            cur.execute(query)
            result = cur.execute(query)
            result = cur.fetchone()
            conn.commit()
            return { "success": True, "program_id": result[0], "program": result[1] }
        except:
            conn.rollback()
            return { "success": False, "traceback": str(traceback.format_exc()) }

    def save_user_program(self, site, category, user_program):
        try:
            print(site)
            values = [site, category, user_program]
            query = make_query_insert("user_program", ["site","category","program"], values)
            cur = conn.cursor()
            cur.execute(query, values)
            conn.commit()

            return { "success": True }
        except:
            conn.rollback()
            return { "success": False, "traceback": str(traceback.format_exc()) }       

    def post(self):
        parser = reqparse.RequestParser()
        parser.add_argument('req_type')
        parser.add_argument('site')
        parser.add_argument('category')
        parser.add_argument('program')
        args = parser.parse_args()
        if args['req_type'] == 'save_user_program':
            return self.save_user_program(args['site'],args['category'],args['program']);
        if args['req_type'] == 'get_user_program':
            return self.get_user_program();
        if args['req_type'] == 'get_last_user_program':
            return self.get_last_user_program();
        return { "success": False }

class UserProgramTempManager(Resource):
    def get_user_program(self, project_id):
        try:
            cur = conn.cursor()
            query = "select id, site, category, program from user_program_temp where project_id = "
            query += "'" + project_id + "';"
            cur.execute(query)
            result = cur.fetchall()
            conn.commit()
            return { "success": True, "output" : result }
        except:
            conn.rollback()
            return { "success": False }

    def get_last_user_program(self):
        try:
            query = "select id, user_program from user_program order by id desc limit 1;"
            cur = conn.cursor()
            cur.execute(query)
            result = cur.execute(query)
            result = cur.fetchone()
            conn.commit()
            return { "success": True, "program_id": result[0], "program": result[1] }
        except:
            conn.rollback()
            return { "success": False, "traceback": str(traceback.format_exc()) }

    def save_user_program(self, site, category, user_program, project_id):
        try:
            print(site)
            values = [site, category, user_program, project_id]
            query = make_query_insert("user_program_temp", ["site","category","program","project_id"], values)
            cur = conn.cursor()
            cur.execute(query, values)
            conn.commit()
            return { "success": True }
        except:
            conn.rollback()
            return { "success": False, "traceback": str(traceback.format_exc()) }       

    def post(self):
        parser = reqparse.RequestParser()
        parser.add_argument('req_type')
        parser.add_argument('site')
        parser.add_argument('category')
        parser.add_argument('program')
        parser.add_argument('project_id')
        args = parser.parse_args()
        if args['req_type'] == 'save_user_program':
            return self.save_user_program(args['site'],args['category'],args['program'],args['project_id'])
        if args['req_type'] == 'get_user_program':
            return self.get_user_program(args['project_id'])
        if args['req_type'] == 'get_last_user_program':
            return self.get_last_user_program()
        return { "success": False }







class JsonExtendEncoder(json.JSONEncoder):
    """
        This class provide an extension to json serialization for datetime/date.
    """
    def default(self, o):
        """
            provide a interface for datetime/date
        """
        if isinstance(o, datetime):
            return o.strftime('%Y-%m-%d %H:%M:%S')
        elif isinstance(o, date):
            return o.strftime('%Y-%m-%d')
        elif isinstance(o, decimal.Decimal):
            return (str(o) for o in [o])
        else:
            return json.JSONEncoder.default(self, o)
            #except:
            #    return ""


def register_New_Date():
    NewDate = psycopg2.extensions.new_type((1082,), 'DATE', psycopg2.STRING)
    psycopg2.extensions.register_type(NewDate)

register_New_Date()

def make_query_insert(table_name, colnames, values):
    sql = "INSERT INTO {} (".format(table_name)
    sql += ", ".join([colname for colname in colnames])
    sql += ") VALUES ( "
    sql += ",".join(["%s" for _ in range(len(values))])
    sql += ")"
    return sql

def make_query_insert_and_returning_id(table_name, colnames, values, id_name):
    sql = make_query_insert(table_name, colnames, values)
    sql += " RETURNING {}".format(id_name)
    return sql

conn = None
try:
    conn = psycopg2.connect("dbname='pse' user='smlee' host='127.0.0.1' port='5434' password='smlee'")
except:
    print("fail connect to the database")

db_conn = None
try:
    db_conn = psycopg2.connect("dbname='pse' user='smlee' host='' port='' password='smlee'")
except:
    print("fail connect to the database")


class TaskManager(Resource):


    def get_input_of_tasks(self, task_ids):
        try:
            cur = conn.cursor()
            query = "select input from task where "
            task_id_list = task_ids.split(',')
            for _ in range(len(task_id_list)):
                query += " task.id = %s or"
            cur.execute(query % str(task_id_list))
            input_urls = cur.fetchall()[0]
            conn.commit()
            return {
                "success" : True,
                "input_urls" : input_urls,
                 }
        except:
            conn.rollback()
            return { "success": False, "traceback": str(traceback.format_exc()) } 

    def get_failed_task(self, task_id):
        try:
            cur = conn.cursor()
            query = "select input, err_msg from task, failed_task_detail where task.id = failed_task_detail.task_id and task.id = %s;"
            cur.execute(query % str(task_id))
            input_url, err_msg = cur.fetchall()[0]
            conn.commit()
            return {
                "success" : True,
                "input_url" : input_url,
                 "err_msg" : err_msg
                 }
        except:
            conn.rollback()
            return { "success": False, "traceback": str(traceback.format_exc()) }       
    
    def get_succeed_task(self, task_id, tables):
        try:

            cur = conn.cursor()
           
            query = "select task.input, stage.level from task join stage on stage.id = task.stage_id where task.id = %s;"
            cur.execute(query % str(task_id))
            (input_url,level) = cur.fetchone()
          
            query = "select output from succeed_task_detail where task_id = %s;"
            cur.execute(query % str(task_id))
            output_url_list = cur.fetchall()[0]

            conn.commit()
            cur = db_conn.cursor()
            output_db=[]

            tables = json.loads(tables)
            for table in tables:
                query = "select * from %s where pse_task_id = %s"
                cur.execute(query % (str(table), str(task_id)))
                cols = [str(desc[0]) for desc in cur.description]
                rows = []
                for data in cur.fetchall():
                    row = [str(col) for col in data]
                    rows.append(row)
                db = {"tableName" : str(table), "cols": cols, "rows": rows}
                output_db.append(db)

            db_conn.commit()
            ouput_db = json.dumps(output_db)
            return {
                "success": True,
                "input_url" : input_url,
                "level" : level,
                "output_url_list" : output_url_list,
                "output_db" : output_db
                 }
        except:
            conn.rollback()
            db_conn.rollback()
            print (str(traceback.format_exc()))
            return { "success": False, "traceback": str(traceback.format_exc()) }       

    def get_stage(self, stage_id):
        try:
            cur = conn.cursor()

            query = "select "
            query += "count(*) FILTER (WHERE task.status = 1) as succeed, "
            query += "count(*) FILTER (WHERE task.status < 0) as failed, "
            query += "count(*) FILTER (WHERE task.status = 0) as unknown, "
            query += "count(*) FILTER (WHERE task.status = -1) as unknown_err, "
            query += "count(*) FILTER (WHERE task.status = -2) as redis_err, "
            query += "count(*) FILTER (WHERE task.status = -3) as grinplum_err, "
            query += "count(*) FILTER (WHERE task.status = -4) as psql_err, "
            query += "count(*) FILTER (WHERE task.status = -5) as selenium_err "
            query += "from task "
            query += "where task.stage_id = %s ;"
            cur.execute(query % str(stage_id))
            dataForTasks = cur.fetchall()[0]

            query = "select id, input from task where status = 1 and stage_id = %s order by id asc; "
            cur.execute(query % str(stage_id))
            dataForSucceedTasks = cur.fetchall()

            query = "select id, input, status from task where status < 0 and stage_id = %s order by id asc; "
            cur.execute(query % str(stage_id))
            dataForFailedTasks = cur.fetchall()
            conn.commit()
            return {
                "success": True,
                "dataForTasks" : dataForTasks,
                "dataForSucceedTasks" : dataForSucceedTasks,
                "dataForFailedTasks" :dataForFailedTasks}
        except:
            conn.rollback()
            print (str(traceback.format_exc()))
            return { "success": False, "traceback": str(traceback.format_exc()) }



    def get_all_tasks_of_stage(self, stage_id):
        try:
            cur = conn.cursor()
            query = "select id, input from task where stage_id = %s order by id asc; "
            cur.execute(query % str(stage_id))
            dataForAllTasks = cur.fetchall()
            conn.commit()
            return {
                "success": True,
                "allTasks" : dataForAllTasks
            }
        except:
            conn.rollback()
            print (str(traceback.format_exc()))
            return { "success": False, "traceback": str(traceback.format_exc()) }       

    def get_stages(self, execution_id, num_of_stages):
        try:
            cur = conn.cursor()

            query = "select stage.id, stage.level, TO_CHAR(stage.start_time, 'YYYY:HH24:MI:SS'), TO_CHAR(stage.end_time, 'YYYY:HH24:MI:SS'), "
            query += "count(*) FILTER (WHERE task.status = 1) as succeed, "
            query += "count(*) FILTER (WHERE task.status < 0) as failed, "
            query += "count(*) FILTER (WHERE task.status = 0) as unknown "
            query += "from stage left join task on stage.id = task.stage_id "
            query += "where stage.execution_id = %s  "
            query += "group by stage.id, stage.level, stage.start_time, stage.end_time "
            query += "order by stage.level;"
            query = query % str(execution_id)
            cur.execute(query)
            stages = cur.fetchall()
            conn.commit()
            runningStageId = 1
            for stage in stages:
                if stage[3] == None: 
                    break
                runningStageId = runningStageId + 1

            for i in range (len(stages), int(num_of_stages)):
                stages.append([i+1, "", "", 0, 0, 0])
            return {
                "success": True,
                "RunningStageId" : runningStageId,
                "stageStatus" : stages
            }
        except:
            conn.rollback()
            print (str(traceback.format_exc()))
            return { "success": False, "traceback": str(traceback.format_exc()) }       

    def post(self):
        parser = reqparse.RequestParser()
        parser.add_argument('execution_id')
        parser.add_argument('job_id')
        parser.add_argument('req_type')
        parser.add_argument('stage_id')
        parser.add_argument('level')
        parser.add_argument('num_of_stages')
        parser.add_argument('task_id')
        parser.add_argument('task_ids')
        parser.add_argument('tables')
        args = parser.parse_args()
        if args['req_type'] == 'stages':
            return self.get_stages(args['execution_id'], args['num_of_stages'])
        if args['req_type'] == 'stage':
            return self.get_stage(args['stage_id'])
        if args['req_type'] == 'tasks':
            return self.get_all_tasks_of_stage(args['stage_id'])
        if args['req_type'] == 'succeed_task':
            return self.get_succeed_task(args['task_id'], args['tables'])
        if args['req_type'] == 'failed_task':
            return self.get_failed_task(args['task_id'])
        if args['req_type'] == 'input_of_tasks':
            return self.get_input_of_tasks(args['task_ids'])
        return { "success": False }


class FailedJobsManager(Resource):

    def get_num_failed_jobs_per_level(self, execution_id):
        try:
            cur = conn.cursor()
            query = "select bfs_level, count(*) as num "
            query += "from log_failed_job "
            query += "where execution_id = %s "
            query += "group by bfs_level; "
            cur.execute(query % execution_id)
            result = cur.fetchall()
            conn.commit()
            return { "success": True, "num_failed_jobs_per_level" : result }
        except:
            conn.rollback()
            print("fail")
            return { "success": False }

    def get_failed_jobs(self, execution_id):
        try:
            cur = conn.cursor()
            query = "select idx, bfs_level, url "
            query += "from log_failed_job where execution_id = %s;"
            cur.execute(query, (execution_id))
            result = cur.fetchall()
            conn.commit()
            return { "success": True, "failed_jobs" : result }
        except:
            conn.rollback()
            print("fail")
            return { "success": False }

    def get_failed_jobs(self, execution_id, level):
        try:
            cur = conn.cursor()
            query = "select idx, url "
            query += "from log_failed_job "
            query += "where execution_id = %s and bfs_level = %s;"
            cur.execute(query, (execution_id, level))
            result = cur.fetchall()
            conn.commit()
            return { "success": True, "failed_jobs" : result }
        except:
            conn.rollback()
            return { "success": False }

    def get_failed_job(self, job_id):
        try:
            cur = conn.cursor()
            query = "select bfs_level, url, err_msg "
            query += "from log_failed_job "
            query += "where idx = %s;"
            cur.execute(query % (job_id))
            result = cur.fetchone()
            conn.commit()
            return { "success": True, "failed_job" : result }
        except:
            conn.rollback()
            return { "success": False }

    def post(self):
        print("fucking")
        parser = reqparse.RequestParser()
        parser.add_argument('execution_id')
        parser.add_argument('job_id')
        parser.add_argument('req_type')
        parser.add_argument('level')
        args = parser.parse_args()
        if args['req_type'] == 'failed_jobs_per_level':
            return self.get_num_failed_jobs_per_level(args['execution_id'])
        if args['req_type'] == 'failed_jobs_of_level':
            return self.get_failed_jobs(args['execution_id'], args['level'])
        if args['req_type'] == 'failed_job':
            return self.get_failed_job(args['job_id'])
        return {}

class TesterOnServer(Resource):
    def test_job(self, program, schema, level, url):
        url = json.loads(url)[0]
        results = [] #run_test(schema, program, url, int(level))
        return{"stdout":"\n".join(results[0]), "err_msg": results[1]}

    def simple_test_job(self, program, schema, level, url):
        #test = Tester()
        url = json.loads(url)[0]
        program = json.loads(program)
        print (url, level)
        (msg_list, err_msg) = ([],"")#test.test(url, program, int(level)-1)
        #test.close()

        return {"stdout": "\n".join(msg_list), "err_msg": err_msg}

#    def test_job_realistic(self, program, schema, level, url):
#        pool = multiprocessing.Pool(processes = 1)
#        results = pool.map(test_job, [(program, schema, level, url)])
#        print(result)
#        return {}

    def post(self):
        parser = reqparse.RequestParser()
        parser.add_argument('req_type')
        parser.add_argument('program')
        parser.add_argument('schema')
        parser.add_argument('level')
        parser.add_argument('url')
        args = parser.parse_args()
        if args['req_type'] == 'simple_test_job':
            return self.simple_test_job(args['program'], args['schema'], args['level'], args['url'])
        if args['req_type'] == 'test_job':
            return self.test_job(args['program'], args['schema'], args['level'], args['url'])
        return {}

class DBSchemasManager(Resource):
    def get_db_schemas(self):
        try:
            cur = conn.cursor()
            cur.execute("select id from db_schema;")
            result = cur.fetchall()
            conn.commit()
            return { "success": True, "db_schemas" : result }
        except:
            conn.rollback()
            return { "success": False }

    def get_db_schema(self, db_schema_id):
        try:
            query = "select schema from db_schema where id = %s"
            values = (db_schema_id)
            print (query % db_schema_id)
            cur = conn.cursor()
            cur.execute(query, values)
            result = cur.fetchone()[0]
            conn.commit()
            return { "success": True, "db_schema": result }
        except:
            conn.rollback()
            return { "success": False, "traceback": str(traceback.format_exc()) }       

    def insert_schema(self, db_schema):
        try:
            query = make_query_insert_and_returning_id("db_schema", ["schema"], [json.dumps(db_schema)], "id")
            cur = conn.cursor()
            cur.execute(query, values)
            result = cur.fetchone()[0]
            conn.commit()
            return { "success": True, "db_schema_id": result }
        except:
            conn.rollback()
            return { "success": False }

    def get(self):
        return self.get_schemas()

    def post(self):
        parser = reqparse.RequestParser()
        parser.add_argument('req_type')
        parser.add_argument('db_schema_id')
        parser.add_argument('user_id')
        args = parser.parse_args()
        if args['req_type'] == 'get_db_schema':
            return self.get_db_schema(args['db_schema_id']);
        return { "success": False }

class ProgramsManager(Resource):
    def get_programs(self):
        try:
            cur = conn.cursor()
            cur.execute("select id from program;")
            result = cur.fetchall()
            return { "success": True, "programs" : result }
        except:
            return { "success": False }


    def get_last_program(self):
        try:
            query = "select id, program from program order by id desc limit 1;"
            cur = conn.cursor()
            cur.execute(query)
            result = cur.execute(query)
            result = cur.fetchone()
            conn.commit()
            return { "success": True, "program_id": result[0], "program": result[1] }
        except:
            conn.rollback()
            return { "success": False, "traceback": str(traceback.format_exc()) }
   

    def get_program(self, program_id):
        try:
            query = "select program from program where id = %s"
            values = (program_id)
            print (query % program_id)
            cur = conn.cursor()
            cur.execute(query, values)
            result = cur.fetchone()[0]
            conn.commit()
            return { "success": True, "program" : result }
        except:
            conn.rollback()
            return { "success": False, "traceback": str(traceback.format_exc()) }       

    def save_program(self, program):
        try:
            values = [program]
            query = make_query_insert("program", ["program"], values)
            cur = conn.cursor()
            cur.execute(query, values)
            conn.commit()

            return { "success": True }
        except:
            conn.rollback()
            return { "success": False, "traceback": str(traceback.format_exc()) }       

    def insert_program(self, program):
        try:
            query = make_query_insert_and_returning_id("program", ["program"], [json.dumps(program)], "id")
            cur = conn.cursor()
            cur.execute(query, values)
            result = cur.fetchone()[0]
            conn.commit()
            return { "success": True, "program_id": result }
        except:
            conn.rollback()
            return { "success": False }

    def get(self):
        return self.get_programs()

    def post(self):
        parser = reqparse.RequestParser()
        parser.add_argument('req_type')
        parser.add_argument('program_id')
        parser.add_argument('user_id')
        parser.add_argument('program')
        args = parser.parse_args()
        if args['req_type'] == 'get_program':
            return self.get_program(args['program_id']);
        if args['req_type'] == 'get_last_program':
            return self.get_last_program();
        if args['req_type'] == 'save_program':
            return self.save_program(args['program']);
        return { "success": False }

class ExecutionsManager(Resource):
    #def get_executions(self):
    #    try:
    #        cur = conn.cursor()
    #        subquery =  "select exc.id as id, exc.program_id as program_id, exc.db_schema_id as db_schema_id, exc.start_time as start_time, exc.end_time as end_time, COALESCE(stage.level, 0) as current_stage "
    #        subquery += "from execution as exc left join stage on exc.id = stage.execution_id "
    #        query =  "select t.id, t.program_id, t.db_schema_id, TO_CHAR(t.start_time, 'YYYY:HH24:MI:SS'), TO_CHAR(t.end_time, 'YYYY:HH24:MI:SS'), MAX(current_stage) "
    #        query += "from (" + subquery + ") as t "
    #        query += "group by t.id, t.program_id, t.db_schema_id, t.start_time, t.end_time "
    #        query += "order by t.id desc; "
    #        cur.execute(query)
    #        result = cur.fetchall()
    #        conn.commit()
    #        return { "success": True, "executions" : result }
    #    except:
    #        conn.rollback()
    #        print(traceback.format_exc())
    #        return { "success": False, "traceback": str(traceback.format_exc()) }       
    def get_executions(self):
        try:
            cur = conn.cursor()
            subquery =  "select exc.id as id, exc.program_id as program_id, exc.category as category, exc.start_time as start_time, exc.end_time as end_time, COALESCE(stage.level, 0) as current_stage "
            subquery += "from execution as exc left join stage on exc.id = stage.execution_id "
            query =  "select t.id, t.program_id,t.category, TO_CHAR(t.start_time, 'YYYY:HH24:MI:SS'), TO_CHAR(t.end_time, 'YYYY:HH24:MI:SS'), MAX(current_stage) "
            query += "from (" + subquery + ") as t "
            query += "group by t.id, t.program_id, t.category,  t.start_time, t.end_time "
            query += "order by t.id desc; "
            cur.execute(query)
            result = cur.fetchall()
            conn.commit()
            return { "success": True, "executions" : result }
        except:
            conn.rollback()
            print(traceback.format_exc())
            return { "success": False, "traceback": str(traceback.format_exc()) }       

    def get_executions_category(self,category):
        try:
            cur = conn.cursor()
            subquery =  "select exc.id as id, exc.program_id as program_id, exc.category as category, exc.start_time as start_time, exc.end_time as end_time, COALESCE(stage.level, 0) as current_stage "
            subquery += "from execution as exc left join stage on exc.id = stage.execution_id "
            query =  "select t.id, t.program_id,t.category, TO_CHAR(t.start_time, 'YYYY:HH24:MI:SS'), TO_CHAR(t.end_time, 'YYYY:HH24:MI:SS'), MAX(current_stage) "
            query += "from (" + subquery + ") as t "
            query += "where category = '"+category+"' group by t.id, t.program_id, t.category,  t.start_time, t.end_time "
            query += "order by t.id desc; "
            cur.execute(query)
            result = cur.fetchall()
            conn.commit()
            return { "success": True, "executions" : result }
        except:
            conn.rollback()
            print(traceback.format_exc())
            return { "success": False, "traceback": str(traceback.format_exc()) }       



    def get_last_execution(self):
        try:
            cur = conn.cursor()
            query = "select ex.id, pr.program, sc.schema "
            query += "from execution ex, program pr, db_schema sc "
            query += "where ex.program_id = pr.id and ex.db_schema_id = sc.id "
            query += "order by ex.id desc limit 1;"
            cur.execute(query)
            result = cur.fetchone()
            conn.commit()
            return { "success": True, "execution" : result }
        except:
            conn.rollback()
            return { "success": False }

    def get_execution(self, execution_id):
        try:
            cur = conn.cursor()
            query = "select pr.program "
            query += "from execution ex, program pr "
            query += "where ex.id = %s and ex.program_id = pr.id;"
            cur.execute(query % execution_id)
            result = cur.fetchone()
            #result = json.dumps(parsed, indent=4, sort_keys=True)
            conn.commit()
            return { "success": True, "execution" : result }
        except:
            print ("fail")
            conn.rollback()
            print(traceback.format_exc())
            return { "success": False, "traceback": str(traceback.format_exc()) }       

    def insert_execution(self, program_id, db_schema_id):
        try:
            query = make_query_insert_and_returning_id("execution", ["program_id", "db_schema_id"], [program_id, db_schema_id], "id")
            cur = conn.cursor()
            cur.execute(query, values)
            result = cur.fetchone()[0]
            conn.commit()
            return { "success": True, "execution_id": result }
        except:
            conn.rollback()
            return { "success": False }

    def get_succeed_execution(self, execution_id, tables):
        try:
            print ("shit..")
            cur = db_conn.cursor()
            tables = json.loads(tables)
            output_db=[]
            for table in tables:
                query = "select * from %s where pse_execution_id = %s "
                cur.execute(query % (str(table), str(execution_id)))
                cols = [str(desc[0]) for desc in cur.description]
                rows = []
                for data in cur.fetchall():
                    row = [str(col) for col in data]
                    rows.append(row)
                db = {"tableName" : str(table), "cols": cols, "rows": rows}
                output_db.append(db)

            db_conn.commit()
            ouput_db = json.dumps(output_db)
            return {
                "success": True,
                "output_db" : output_db
                 }
        except:
            conn.rollback()
            db_conn.rollback()
            print (str(traceback.format_exc()))
            return { "success": False, "traceback": str(traceback.format_exc()) }       

    def get(self):
        return self.get_executions()

    def post(self):
        parser = reqparse.RequestParser()
        parser.add_argument('execution_id')
        parser.add_argument('program_id')
        parser.add_argument('category')
        parser.add_argument('db_schema_id')
        parser.add_argument('req_type')
        parser.add_argument('tables')
        args = parser.parse_args()
        if args['req_type'] == 'get_execution':
            return self.get_execution(args['execution_id'])
        if args['req_type'] == 'get_executions_category':
            return self.get_executions_category(args['category'])
        if args['req_type'] == 'get_last_execution':
            return self.get_last_execution()
        if args['req_type'] == 'get_scrapped_data_of_execution':
            return self.get_succeed_execution( args['execution_id'], args['tables'])
        return {}

class AccountManager(Resource):
    def get_auth(self, userId, password):
        try:
            cur = conn.cursor()
            query = "select exists (select 1 from account where user_id = "
            query += "'" + userId + "'"
            query += "and password = "
            query += "'" + password + "');"
            cur.execute(query)
            result = cur.fetchone()[0]
            conn.commit()
            if result == True:
                return {"success": True, "auth": True}
            else:
                return {"success": True, "auth": False}
        except:
            conn.rollback()
            return {"success": False}

    def sign_up(self, userId, password):
        try:
            cur = conn.cursor()
            query = "insert into account values ("
            query += "'" + userId + "', '" + password + "');"
            cur.execute(query)
            conn.commit()
            return {"success": True}
        except:
            conn.rollback()
            return {"success": False}

    def post(self):
        parser = reqparse.RequestParser()
        parser.add_argument('req_type')
        parser.add_argument('user_id')
        parser.add_argument('password')
        parser.add_argument('new_user_id')
        parser.add_argument('new_password')
        args = parser.parse_args()
        if args['req_type'] == 'get_auth':
            return self.get_auth(args['user_id'], args['password'])
        elif args['req_type'] == 'sign_up':
            return self.sign_up(args['new_user_id'], args['new_password'])
        return { "success": False }

class ProjectManager(Resource):
    def get_project_list(self, user_id):
        try:
            cur = conn.cursor()
            query = "select project_id, project_name from project where user_id = "
            query += "'" + user_id + "';"
            cur.execute(query)
            result = cur.fetchall()
            conn.commit()
            return result
        except:
            conn.rollback()
            return -1

    def make_new_project(self, user_id):
        try:
            cur = conn.cursor()
            query = "insert into project (user_id, project_name) values "
            query += "('" + user_id + "', 'New Project');"
            cur.execute(query)
            conn.commit()
            return True
        except:
            conn.rollback()
            return False

    def remove_project(self, project_id):
        try:
            cur = conn.cursor()
            query = "delete from project where project_id = '" + project_id + "';"
            cur.execute(query)
            conn.commit()
            return True
        except:
            conn.rollback()
            return False

    def post(self):
        parser = reqparse.RequestParser()
        parser.add_argument('req_type')
        parser.add_argument('user_id')
        parser.add_argument('password')
        parser.add_argument('project_id')
        args = parser.parse_args()
        if args['req_type'] == 'get_project_list':
            return self.get_project_list(args['user_id'])
        elif args['req_type'] == 'make_new_project':
            return self.make_new_project(args['user_id'])
        elif args['req_type'] == 'remove_project':
            return self.remove_project(args['project_id'])


app = Flask(__name__)
CORS(app)
api = Api(app)

api.add_resource(TaskManager, '/api/db/task')
api.add_resource(FailedJobsManager, '/api/db/failedjobs')
api.add_resource(DBSchemasManager, '/api/db/dbschemas')
api.add_resource(ProgramsManager, '/api/db/programs')
api.add_resource(ExecutionsManager, '/api/db/executions')
api.add_resource(UserProgramManager, '/api/db/userprogram')
api.add_resource(CategoryManager, '/api/db/category')
api.add_resource(ObjectManager, '/api/db/object')
api.add_resource(TransformManager, '/api/db/transform')
api.add_resource(AccountManager, '/api/db/account')
api.add_resource(ProjectManager, '/api/db/project')
api.add_resource(UserProgramTempManager, '/api/db/userprogramtemp')


if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)
