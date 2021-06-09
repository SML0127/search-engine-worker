import time
import sys
import traceback
import json
import argparse

from managers.redis_manager import *
from engine.exporter import Exporter
from plugin.cafe24.single_uploader import Cafe24SingleUploader
from functools import partial
print_flushed = partial(print, flush=True)

class Cafe24Driver():

  def __init__(self):
    pass

  def init_managers(self, settings):
    self.redis_manager = RedisManager()
    self.redis_manager.connect(settings)
    self.redis_manager.create_rq(self.redis_manager.get_connection(),settings['redis_queue'])

  def wait(self, running_tasks):
    start_time = time.time()
    successful_tasks, failed_tasks = [], []
    while len(running_tasks) > 0:
      indexes = []
      for idx, task in enumerate(running_tasks):
        stat = self.redis_manager.get_status(task)
        if stat == 'finished':
          indexes.append(idx)
          successful_tasks.append(task)
        elif stat == 'failed':
          indexes.append(idx)
          failed_tasks.append(task)
      for val in sorted(indexes, reverse = True):
        running_tasks.pop(val)
      print_flushed("### SUCCESSFUL: {}, FAILED: {}, RUNNING: {} ".format(len(successful_tasks), len(failed_tasks), len(running_tasks)))
      time.sleep(10)

    for task in successful_tasks:
      if (type(self.redis_manager.get_result(task)) == type({})):
        for key, item in self.redis_manager.get_result(task).items():
          print_flushed(key, item)
    print_flushed('elapsed time per step:', time.time() - start_time)
    time.sleep(900)


  def run(self, args, node_ids):
    try:
      #self.init(program)
      #program['lm'].logging_execution_start(program['execution_id'])
      running_tasks = []

      num_product_per_task = 20
      print_flushed("# of task "+str(int(len(node_ids))))
      end = int(len(node_ids) / num_product_per_task) + 1
      print_flushed("end "+str(int(end)))
      num_threads_per_worker = args['num_threads']

      num_workers, max_num_workers = 0, args['max_num_workers']

      clients = args['clients']

      if len(clients) < max_num_workers * num_threads_per_worker:
        print_flushed("The clients are not enough")
        raise

      clients_per_worker = []
      for i in range(max_num_workers):
        clients_per_worker.append(clients[i * num_threads_per_worker: (i + 1) * num_threads_per_worker])

      for idx in range(end):
        job = {}
        job['node_ids'] = node_ids[(idx)*num_product_per_task:(idx+1)*num_product_per_task]
        job['args'] = args.copy()
        job['args']['clients'] = clients_per_worker[num_workers]
        #print_flushed(job)
        running_tasks.append(self.redis_manager.enqueue(job))
        num_workers += 1
        if num_workers == max_num_workers:
          self.wait(running_tasks)
          num_workers = 0
      self.wait(running_tasks)
    except Exception as e:
      print_flushed("-------Raised Exception in DRIVER-------")
      print_flushed(e)
      print_flushed("---------------------------------------")
      print_flushed("--------------STACK TRACE--------------")
      print_flushed(str(traceback.format_exc()))
      print_flushed("---------------------------------------")
    finally:
      pass

  def run_from_db(self, args):
    pass

  def run_from_file(self, args):
    label, exec_id = args['label'], args['execution_id']
    start_time = time.time()
    exporter = Exporter()
    exporter.init()
    node_ids = exporter.graph_mgr.find_nodes_of_execution_with_label(exec_id, label)
    exporter.close()
    print_flushed("num of nodes: ", len(node_ids))
    #print_flushed(args)
    self.run(args, node_ids)
    print_flushed("num of nodes: ", len(node_ids))
    print_flushed("elapsed time: ", time.time() - start_time)

  def single_run_from_file(self, args):
    start_time = time.time()
    uploader = Cafe24SingleUploader()
    uploader.upload_products(args)
    print_flushed("elapsed time: ", time.time() - start_time)

  def execute(self, args):
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
      parser.add_argument('--cafe24_c', required=False, help='')
      parser.add_argument('--cafe24_eid', required=False)
      parser.add_argument('--cafe24_label', required=False)
      parser.add_argument('--cafe24_host', required=False)
      parser.add_argument('--cafe24_port', required=False)
      parser.add_argument('--cafe24_queue', required=False)
      parser.add_argument('--cafe24_code', required=False)
      parser.add_argument('--cafe24_mall', required=False)
      sys_args, unknown = parser.parse_known_args()
      f = open(sys_args.cafe24_mall)
      targs = json.load(f)
      targs.update(args)
      args = targs
      f.close()
      f = open(sys_args.cafe24_code)
      args['code'] = f.read()
      f.close()
      if sys_args.cafe24_eid != None: args['execution_id'] = sys_args.cafe24_eid
      args['label'] = sys_args.cafe24_label
      if sys_args.cafe24_c == 'run':
        self.init_managers({'redis_host': sys_args.cafe24_host, 'redis_port': sys_args.cafe24_port, 'redis_queue': sys_args.cafe24_queue})
        self.run_from_file(args)
      elif sys_args.cafe24_c == 'single_run':
        self.single_run_from_fiㅣe(args)
    except Exception as e:
      print_flushed(str(traceback.format_exc()))
      raise e

if __name__ == "__main__":
  driver = Cafe24Driver()
  driver.execute({})

