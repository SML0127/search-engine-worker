import sys
import os
import json
import base64
import requests
import psycopg2
import subprocess
import traceback
import pprint
import time
import re
import itertools

from flask import Flask
from flask_restful import Resource, Api
from flask_restful import reqparse
from flask_cors import CORS


from price_parser import Price
from forex_python.converter import CurrencyRates


from plugin.cafe24.APIManagers import Cafe24Manager
from managers.graph_manager import GraphManager
from managers.settings_manager import *
from engine.exporter import Exporter

from multiprocessing.dummy import Pool as ThreadPool


class Cafe24SingleUploader(Resource):

  def __init__(self):
    self.setting_manager = SettingsManager()
    self.setting_manager.setting("/home/pse/PSE-engine/settings-worker.yaml")
    settings = self.setting_manager.get_settings()
    self.graph_manager = GraphManager()
    self.graph_manager.init(settings)
    pass

  def upload_products(self, args):
    total_time = time.time()
    profiling_info = {}
    try:
      label = args['label']
      exec_id = args['execution_id']
      exporter = Exporter()
      exporter.init()
      #node_ids = exporter.graph_mgr.find_nodes_of_execution_with_label(exec_id, label)
      mpids = exporter.graph_mgr.get_mpid_from_mysite(exec_id)
      job_id = exporter.graph_mgr.get_job_id_from_eid(exec_id)
      exporter.close()
      print("num of products: ", len(mpids))
      #return {} 
      num_threads = args['num_threads']
      chunk_size = (len(mpids) // num_threads) + 1
      mpid_chunks = [mpids[i:i + chunk_size] for i in range(0, len(mpids), chunk_size)]
      pool = ThreadPool(num_threads)
      tasks = []
      for i in range(len(mpid_chunks)):
        nargs = args.copy()
        nargs.update(args['clients'][i])
        tasks.append((nargs, mpid_chunks[i]))

      tasks = list(map(lambda x: (args.copy(), x), mpid_chunks))
      results = pool.map(self.upload_products_of_task, tasks)
      #self.upload_products_of_nodes(args, node_ids)
      pool.close()
      pool.join()
    except:
      profiling_info['total_time'] = time.time() - total_time
      print(profiling_info)
      print(traceback.format_exc())
      return {}
    profiling_info['total_time'] = time.time() - total_time
    print(profiling_info)
    return {}



  def upload_products_of_mpid(self, args, mpids):
    total_time = time.time()
    profiling_info = {}
    try:
      exec_id = args['execution_id']
      args['job_id'] = self.graph_manager.get_job_id_from_eid(exec_id)

      num_threads = args.get('num_threads', 1)
      pool = ThreadPool(num_threads)
      print('num threads in args: ', num_threads)
      chunk_size = (len(mpids) // num_threads) + 1
      mpid_chunks = [mpids[i:i + chunk_size] for i in range(0, len(mpids), chunk_size)]
      tasks = []
      for i in range(len(mpid_chunks)):
        nargs = args.copy()
        nargs.update(args['clients'][i])
        tasks.append((nargs, mpid_chunks[i]))
      #results = pool.map(self.upload_products_of_task, tasks)
      #tasks = list(map(lambda x: (args.copy(), x), mpid_chunks))
      results = pool.map(self.upload_products_of_task_from_mpid, tasks)
      profiling_info['threads'] = results
    except:
      pool.close()
      pool.join()
      profiling_info['total_time'] = time.time() - total_time
      #print(profiling_info)
      #print(traceback.format_exc())
      return profiling_info
    profiling_info['total_time'] = time.time() - total_time
    #print(profiling_info)
    pool.close()
    pool.join()
    return profiling_info


  def upload_products_of_task_from_mpid(self, task):
    total_time = time.time()
    successful_node = 0
    failed_node = 0
    profiling_info = {}
    try:
      (args, mpids) = task
      cafe24manager = Cafe24Manager(args)
      cafe24manager.get_auth_code()
      cafe24manager.get_token()
      cafe24manager.list_brands()
      #targetsite_url = 'https://{}.cafe24.com/'.format(args['mall_id'])

      exporter = Exporter()
      exporter.init()
      exporter.import_rules_from_code(args['code'])

      #print(exec_id, label)
      #print(node_ids)
      job_id = args['job_id'] 
      tsid = args['tsid'] 
      targetsite_url, gateway = self.graph_manager.get_targetsite(tsid)
      print("tsid: ", tsid)
      print("targetsite: ", targetsite_url)
      if 'selected' in args:
        for mpid in mpids:
          node_time = time.time()
          try:
            product, original_product_information = exporter.export_from_selected_mpid(job_id, args['execution_id'], mpid)
            product['targetsite_url'] = targetsite_url
           
            status = self.graph_manager.check_status_of_product(job_id, mpid)
             
            #Status 0 = up to date, 1 = changed, 2 = New, 3 = Deleted 4 = Duplicated
            print('status : ', status)
            if gateway.upper() == 'CAFE24':
              tpid = self.graph_manager.get_tpid(job_id, targetsite_url, mpid)
              cafe24manager.update_exist_product(product, profiling_info, job_id, tpid)
              cafe24manager.refresh()
            #self.graph_manager.logging_all_uploaded_product(job_id, args['execution_id'], mpid, original_product_information, product, targetsite_url) 

            successful_node += 1
          except:
            #print(traceback.format_exc())
            failed_node += 1
      elif 'onetime' in args:
        for mpid in mpids:
          node_time = time.time()
          try:
           
            status = self.graph_manager.check_status_of_product(job_id, mpid)
             
            #Status 0 = up to date, 1 = changed, 2 = New, 3 = Deleted 4 = Duplicated
            print('status : ', status)
            if gateway.upper() == 'CAFE24':
              is_uploaded = self.graph_manager.check_is_item_uploaded(job_id, targetsite_url, mpid)
              print('is uploaded proudct? : ', is_uploaded)
              if is_uploaded == False: # upload as new item
                product, original_product_information = exporter.export_from_mpid_onetime(job_id, args['execution_id'], mpid, tsid)
                product['targetsite_url'] = targetsite_url
                cafe24manager.upload_new_product(product, profiling_info, job_id)
              else:
                if status == 1:
                  product, original_product_information = exporter.export_from_mpid_onetime(job_id, args['execution_id'], mpid, tsid)
                  product['targetsite_url'] = targetsite_url
                  tpid = self.graph_manager.get_tpid(job_id, targetsite_url, mpid)
                  print('tpid : ', tpid)
                  cafe24manager.update_exist_product(product, profiling_info, job_id, tpid)
                elif status == 2:
                  product, original_product_information = exporter.export_from_mpid_onetime(job_id, args['execution_id'], mpid, tsid)
                  product['targetsite_url'] = targetsite_url
                  cafe24manager.upload_new_product(product, profiling_info, job_id)
                elif status == 3:
                  tpid = self.graph_manager.get_tpid(job_id, targetsite_url, mpid)
                  print('tpid : ', tpid)
                  cafe24manager.hide_exist_product(profiling_info, job_id, tpid)

              cafe24manager.refresh()
            #self.graph_manager.logging_all_uploaded_product(job_id, args['execution_id'], mpid, original_product_information, product, targetsite_url) 

            successful_node += 1
          except:
            print(traceback.format_exc())
            failed_node += 1     
      else:
        for mpid in mpids:
          node_time = time.time()
          try:
            product, original_product_information = exporter.export_from_mpid(job_id, args['execution_id'], mpid)
            product['targetsite_url'] = targetsite_url
           
            status = self.graph_manager.check_status_of_product(job_id, mpid)
             
            #Status 0 = up to date, 1 = changed, 2 = New, 3 = Deleted 4 = Duplicated
            print('status : ', status)
            if gateway.upper() == 'CAFE24':
              is_uploaded = self.graph_manager.check_is_item_uploaded(job_id, targetsite_url, mpid)
              if is_uploaded == False: # upload as new item
                cafe24manager.upload_new_product(product, profiling_info, job_id)
              else:
                if status == 1:
                  tpid = self.graph_manager.get_tpid(job_id, targetsite_url, mpid)
                  cafe24manager.update_exist_product(product, profiling_info, job_id, tpid)
                elif status == 2:
                  cafe24manager.upload_new_product(product, profiling_info, job_id)
                elif status == 3:
                  tpid = self.graph_manager.get_tpid(job_id, targetsite_url, mpid)
                  cafe24manager.hide_exist_product(product, profiling_info, job_id, tpid)

              cafe24manager.refresh()
            #self.graph_manager.logging_all_uploaded_product(job_id, args['execution_id'], mpid, original_product_information, product, targetsite_url) 

            successful_node += 1
          except:
            #print(traceback.format_exc())
            failed_node += 1
    except:
      profiling_info['total_time'] = time.time() - total_time
      #print(profiling_info)
      #print(traceback.format_exc())
      return profiling_info
    print('s/f', successful_node, '/', failed_node)
    profiling_info['total_time'] = time.time() - total_time
    profiling_info['successful_node'] = successful_node
    profiling_info['failed_node'] = failed_node
    #print(profiling_info)
    return profiling_info




  def upload_products_of_nodes(self, args, node_ids):
    total_time = time.time()
    profiling_info = {}
    num_threads = args.get('num_threads', 1)
    pool = ThreadPool(num_threads)
    try:
      num_threads = args['num_threads']
      chunk_size = (len(node_ids) // num_threads) + 1
      node_id_chunks = [node_ids[i:i + chunk_size] for i in range(0, len(node_ids), chunk_size)]
      pool = ThreadPool(num_threads)
      tasks = []
      for i in range(len(node_id_chunks)):
        nargs = args.copy()
        nargs.update(args['clients'][i])
        tasks.append((nargs, node_id_chunks[i]))
      results = pool.map(self.upload_products_of_task, tasks)
      print(results)
      profiling_info['threads'] = results
    except:
      pool.close()
      pool.join()
      profiling_info['total_time'] = time.time() - total_time
      print(profiling_info)
      print(traceback.format_exc())
      return profiling_info
    profiling_info['total_time'] = time.time() - total_time
    print(profiling_info)
    pool.close()
    pool.join()
    return profiling_info



  def upload_products_of_task(self, task):
    total_time = time.time()
    successful_node = 0
    failed_node = 0
    profiling_info = {}
    try:
      (args, node_ids) = task
      cafe24manager = Cafe24Manager(args)
      cafe24manager.get_auth_code()
      cafe24manager.get_token()
      cafe24manager.list_brands()

      exporter = Exporter()
      exporter.init()
      exporter.import_rules_from_code(args['code'])

      #print(exec_id, label)
      #print(node_ids)
      for node_id in node_ids:
        node_time = time.time()
        try:
          product = exporter.export(node_id)
          cafe24manager.upload_new_product(product, profiling_info)
          cafe24manager.refresh()
          successful_node += 1
        except:
          print(traceback.format_exc())
          failed_node += 1
        #print('node time: ', time.time() - node_time)
        #print(profiling_info)
    except:
      profiling_info['total_time'] = time.time() - total_time
      print(profiling_info)
      print(traceback.format_exc())
      return profiling_info
    print('s/f', successful_node, '/', failed_node)
    profiling_info['total_time'] = time.time() - total_time
    profiling_info['successful_node'] = successful_node
    profiling_info['failed_node'] = failed_node
    print(profiling_info)
    return profiling_info

  def post(self):
    parser = reqparse.RequestParser()
    parser.add_argument('req_type')
    parser.add_argument('mall_id')
    parser.add_argument('user_id')
    parser.add_argument('user_pwd')
    parser.add_argument('client_id')
    parser.add_argument('client_secret')
    parser.add_argument('redirect_uri')
    parser.add_argument('scope')
    parser.add_argument('execution_id')
    parser.add_argument('transform_id')
    #parser.add_argument('transformations')
    args = parser.parse_args()
    if args['req_type'] == 'upload_products': 
      return self.upload_products(args)
      #return self.upload_products_of_execution(args)
    elif args['req_type'] == "run_upload_driver":
      return self.run_upload_driver(args)
    return {}


app = Flask(__name__)
api = Api(app)
api.add_resource(Cafe24SingleUploader, '/upload/cafe24')

if __name__ == '__main__':
  cafe24api = Cafe24SingleUploader()
  args = {}
  args['execution_id'] = 649
  args['label'] = 7
  args['clients'] = [
      {'client_id': 'oc4Eair8IB7hToJuyjJsiA', 'client_secret': 'EMsUrt4tI1zgSt2i3icPPC'},
      {'client_id': 'lmnl9eLRBye5aZvfSU4tXE', 'client_secret': 'nKAquRGpPVsgo6GZkeniLA'},
      {'client_id': 'Vw9ygiIAJJLnLDKiAkhsDA', 'client_secret': 'p6EqNWe8DqEHRtxyzP4S4D'},
      {'client_id': 'UyLmMdVBOJHvYy0VF4pcpA', 'client_secret': 'dy2CzhMiK9OrLMHyIq37mC'},
      {'client_id': 'AafM42MiBie2mB3mRMM0bE', 'client_secret': 'Lv3S8HfvZCxdXifxfb2QMP'},
      {'client_id': 'f8rDSAXoWiwPPIBchadCfH', 'client_secret': 'f1yQdvaSN6OLD19qJ1m7oD'},
      {'client_id': 'nj0kecRmH6IEn0zFZecHZM', 'client_secret': 'xJ4a9htZGhogr1H2mZNibB'},
      {'client_id': 'nP5GWlrOER7kbYVu6QEtGA', 'client_secret': 'mctijmPmOp8lKOaex0VlLF'},
      {'client_id': 'LkgQ03ETLtTiCRfmYa5dgD', 'client_secret': '49XJnilLP96vlcKWu8zr8A'},
      {'client_id': 'ZIfFO0T6HSHZX4QPpf86EF', 'client_secret': 'oqUEjpMBONgiRMmyE4zAvA'},
      {'client_id': 'fDhrr2B1DyQEvuaHUPUD1D', 'client_secret': 'WyCr0qkfHKWWZlWl8fcxiK'},
      {'client_id': 'K0JPoImnDJXn8giYecK5yE', 'client_secret': 'TaW51jMeHaZTLveIjfhXe2'},
      {'client_id': 'UzhOkGW6H5An6QaYpfMHQA', 'client_secret': 'swcXg2pEFVFjdBanSUfqaC'},
      {'client_id': 'ojrikKQeGiBcVkUBybQGYB', 'client_secret': 'zHdUFOUSHZUkRV4QJdoRvD'},
      {'client_id': 'avZzMvjjCx4mNz8OOKLjcB', 'client_secret': 'H2OTer0OIi3WZyvS7gYeiP'},
      {'client_id': 'ehLMLKGobqVOxsoYgu5W1E', 'client_secret': 'tTWPLjC9IdCp9sMK0j4JKD'}
    ]
  f = open('./cafe24_zalando.py')
  args['code'] = f.read()
  f.close()
  args['mall_id'] = 'mallmalljmjm'
  args['user_id'] = 'mallmalljmjm'
  args['user_pwd'] = 'Dlwjdgns2'
  args['redirect_uri'] = 'https://www.google.com'
  args['scope'] = 'mall.write_product mall.read_product mall.read_category mall.write_category mall.read_collection mall.write_collection'
  cafe24api.upload_products(args)
  #app.run(debug=True, host='0.0.0.0', port=5002) 
