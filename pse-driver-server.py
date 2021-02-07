import sys
import os
from flask import Flask
from flask_restful import Resource, Api
from flask_restful import reqparse
from flask_cors import CORS

import json
import psycopg2
import subprocess
import traceback

import decimal
from datetime import date
from datetime import datetime


from pse_driver import PseDriver
from managers.log_manager import LogManager
from managers.settings_manager import SettingsManager

class JsonExtendEncoder(json.JSONEncoder):
    def default(self, o):
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

driver = PseDriver()
driver.init_managers()

class DriverManager(Resource):

    def register_execution(self, program_id, db_schema_id):
        try:
            log_manager.

            query = make_query_insert_and_returning_id("execution", ["program_id", "db_schema_id"], [program_id, db_schema_id], "id")
            cur = conn.cursor()
            cur.execute(query, [program_id, db_schema_id])
            result = cur.fetchone()[0]
            conn.commit()
            return result
        except:
            conn.rollback()
            raise
#        pool = multiprocessing.Pool(processes = 1)
#        pool = multiprocessing.Pool(processes = 1)
#        results = pool.map(test_job, [(program, schema, level, url)])
#        print(result)
#
#    def register_program_execution(self, program):
#        try:
#            cur = conn.cursor()
#            query = make_query_insert_and_returning_id("program", ["program"], [program], "id")
#            cur.execute(query, [program])
#            program_id = cur.fetchone()[0]
#            
#            query = make_query_insert_and_returning_id("execution", ["program_id"], [program_id], "id")
#            cur.execute(query, [program_id])
#            execution_id = cur.fetchone()[0]
#            conn.commit()
#            return {
#                "success": True,
#                "execution_id": execution_id,
#                "program_id": program_id,
#            }
#        except:
#            conn.rollback()
#            print (str(traceback.format_exc()))
#            return { "success": False, "traceback": str(traceback.format_exc()) }       
#

    def register_execution0(self, program, category):
        try:


    def register_program_execution(self, program, category):
        try:
            cur = conn.cursor()
            query = make_query_insert_and_returning_id("program", ["program"], [program], "id")
            cur.execute(query, [program])
            program_id = cur.fetchone()[0]
            
            query = make_query_insert_and_returning_id("execution", ["program_id","category"], [program_id, category], "id")
            cur.execute(query, [program_id, category])
            execution_id = cur.fetchone()[0]
            conn.commit()
            return {
                "success": True,
                "execution_id": execution_id,
                "program_id": program_id,
            }
        except:
            conn.rollback()
            print (str(traceback.format_exc()))
            return { "success": False, "traceback": str(traceback.format_exc()) }       



    def run_driver(self, execution_id):
        try:
            print("simple run")
            #driver.run_from_db(sysinfo, False)
            subprocess.Popen("python driver.py run_from_execution %s" % str(execution_id), shell=True)
            return {
                "success": True,
            }
        except:
            print (str(traceback.format_exc()))
            return { "success": False, "traceback": str(traceback.format_exc()) }       

    def post(self):
        parser = reqparse.RequestParser()
        parser.add_argument('execution_id')
        parser.add_argument('program')
        parser.add_argument('category')
        parser.add_argument('program_id')
        parser.add_argument('schema')
        parser.add_argument('schema_id')
        parser.add_argument('req_type')
        args = parser.parse_args()
        if args['req_type'] == "register_program_execution":
            return self.register_program_execution(args['program'], args['category'])
        elif args['req_type'] == "run_driver":
            return self.run_driver(args['execution_id'])
        else:
            print (str(traceback.format_exc()))
            return { "success": False, "traceback": str(traceback.format_exc()) }       

app = Flask(__name__)
CORS(app)
api = Api(app)

api.add_resource(DriverManager, '/api/driver/')

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5001)
