import traceback
import time

class Program():
  pass

class OpenURL():

  def run0(self, rm, results):

    task = self.props
    task['parent_task_id'] = 0
    task['parent_node_id'] = 0
    #print(task)
    running_task = rm.enqueue(task)
    stat = False
    while True:
      stat = rm.get_status(running_task)
      #print(stat)
      if stat == 'finished': 
        stat = True
        break
      elif stat == 'failed': 
        stat = False
        break
      time.sleep(1)
    print('### STAGE {} - {}'.format(self.props['id'], 'success' if stat else 'fail'))
    if stat and type(rm.get_result(running_task)) == type({}):
      #print(rm.get_result(running_task))
      for key, item in rm.get_result(running_task).items():
        results[key] = results.get(key, []) + item

  def rerun(self, rm, results, previous_tasks):
    if len(previous_tasks) == 0: return
    task = self.props
    task['parent_task_id'] = 0
    task['parent_node_id'] = 0
    previous_task = previous_tasks[0]
    task['previous_task_id'] = previous_task[0]
    task['url'] = previous_task[1]
    running_task = rm.enqueue(task)
    stat = False
    while True:
      stat = rm.get_status(running_task)
      #print(stat)
      if stat == 'finished':
        stat = True
        break
      elif stat == 'failed':
        stat = False
        break
      time.sleep(1)
    print('### STAGE {} - {}'.format(self.props['id'], 'success' if stat else 'fail'))
    if stat and type(rm.get_result(running_task)) == type({}):
      #print(rm.get_result(running_task))
      for key, item in rm.get_result(running_task).items():
        results[key] = results.get(key, []) + item

  def run(self, rm, results, previous_tasks):
    if previous_tasks == None:
      self.run0(rm,results)
    else:
      self.rerun(rm, results, previous_tasks)

class BFSIterator():

  def wait(self, rm, running_tasks, results):
    successful_tasks, failed_tasks = [], []
    while len(running_tasks) > 0:
      indexes = []
      for idx, task in enumerate(running_tasks):
        stat = rm.get_status(task)
        if stat == 'finished':
          indexes.append(idx)
          successful_tasks.append(task)
        elif stat == 'failed':
          indexes.append(idx)
          failed_tasks.append(task)
      for val in sorted(indexes, reverse = True):
        running_tasks.pop(val)
      print("### STAGE {} - SUCCESSFUL: {}, FAILED: {}, RUNNING: {} ".format(self.props['id'], len(successful_tasks), len(failed_tasks), len(running_tasks)))
      time.sleep(10)

    for stask in successful_tasks:
      task_result = rm.get_result(stask)
      if (type(task_result) == type({})):
        for key, item in task_result.items():
          #print(key, item)
          results[key] = results.get(key, []) + item
      else:
        print("No result of task")
    return len(failed_tasks)

  def run0(self, rm, results):
    max_num_tasks = int(self.props.get('max_num_tasks', -1))
    max_num_local_tasks = int(self.props.get('max_num_local_tasks', -1))
    task = self.props
    running_tasks, num_tasks = [], 0
    input_op_id = task['input']
    chunk_size, max_chunk_size = 0, self.props.get('max_num_worker', 200)
    for (parent_task_id, parent_node_id, urls) in results.get(input_op_id, []):
      if max_num_tasks > -1 and num_tasks >= max_num_tasks: break
      task['parent_task_id'] = parent_task_id
      task['parent_node_id'] = parent_node_id
      num_local_tasks = 0
      for url in urls:
        if max_num_tasks > -1 and num_tasks >= max_num_tasks:
          break
        if max_num_local_tasks > -1 and num_local_tasks >= max_num_local_tasks:
          break
        task['url'] = url
        running_tasks.append(rm.enqueue(task))
        num_tasks += 1
        num_local_tasks += 1
        chunk_size += 1
        if chunk_size == max_chunk_size:
          chunk_size = 0
          self.wait(rm, running_tasks, results)
    if task['input'] in results: del results[task['input']]
    self.wait(rm, running_tasks, results)

  def rerun(self, rm, results, previous_tasks):
    if len(previous_tasks) == 0: return
    max_num_tasks = -1
    max_num_local_tasks = -1
    task = self.props
    running_tasks, num_tasks = [], 0
    input_op_id = task['input']
    chunk_size, max_chunk_size = 0, self.props.get('max_num_worker', 200)
    for (previous_task_id, url) in previous_tasks:
      if max_num_tasks > -1 and num_tasks >= max_num_tasks: break
      task['parent_task_id'] = -1
      task['parent_node_id'] = -1
      task['previous_task_id'] = previous_task_id
      task['url'] = url
      running_tasks.append(rm.enqueue(task))
      chunk_size += 1
      if chunk_size == max_chunk_size:
        chunk_size = 0
        self.wait(rm, running_tasks, results)
    self.wait(rm, running_tasks, results)

  def run1(self, rm, results):
    max_num_tasks = int(self.props.get('max_num_tasks', -1))
    max_num_local_tasks = int(self.props.get('max_num_local_tasks', -1))
    
    task = self.props
  
    initial_values = self.props['initial_values']
    increments = self.props['increments']
    query = self.props['url_query']
    
    running_tasks, num_tasks = [], 0
    chunk_size, max_chunk_size = 0, self.props.get('max_num_worker', 20)
    #print(results)
    num_failed = 0
    for (parent_task_id, parent_node_id, urls) in results.get(task['input'], []):
      if max_num_tasks > -1 and num_tasks >= max_num_tasks: break
      task['parent_task_id'] = parent_task_id
      task['parent_node_id'] = parent_node_id
      num_local_tasks = 0
      for url in urls:
        while True:
          if max_num_tasks > -1 and num_tasks >= max_num_tasks: 
            break
          if max_num_local_tasks > -1 and num_local_tasks >= max_num_local_tasks: 
            break
          values = list(map(lambda x, y: int(x) + num_local_tasks * int(y), initial_values, increments))
          #print (url, query, values)
          task['url'] = url + (query % tuple(values))
          running_tasks.append(rm.enqueue(task))
          num_tasks += 1
          num_local_tasks += 1
          chunk_size += 1
          if chunk_size == max_chunk_size:
            chunk_size = 0
            num_failed = self.wait(rm, running_tasks, results)
          if num_failed > 0: break
        if num_failed > 0: break
      if num_failed > 0: break
    if task['input'] in results: del results[task['input']]
    self.wait(rm, running_tasks, results)

  def run(self, rm, results, previous_tasks):
    if previous_tasks != None:
      self.rerun(rm, results, previous_tasks)
    if len(self.props.get('url_query', '').strip()) > 0:
      self.run1(rm, results)
    else:
      self.run0(rm, results)

driver_operators = {
  'BFSIterator': BFSIterator,
  'OpenURL': OpenURL
}

def materialize(lop):
  pop = driver_operators[lop['name']]()
  pop.props = lop
  return pop
