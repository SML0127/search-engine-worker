from util.pse_errors import *
from lxml import html
import time
from lxml import etree
import traceback
import random

class GlovalVariable():

  def __init__(self):
    self.msg = []
    self.err_msg = []
    self.results = {}
    self.web_mgr = None
    self.graph_mgr = None
    self.task_url = None
    self.task_zipcode_url = None
    self.task_id = None
    self.exec_id = None
    self.profiling_info = {}
    self.stack_nodes = []
    self.stack_indices = []

  def append_msg(self, msg):
    self.msg.append(msg)

  def append_err_msg(self, msg):
    self.err_msg.append(msg)

  def get_msg(self):
    return "\n".join(self.msg)

  def get_err_msg(self):
    return "\n".join(self.err_msg)


class BaseOperator():
  
  def __init__(self):
    self.props = {}
    self.operators = []
    pass

  def __str__(self):
    print(__class__.__name__)

  def __repr__(self):
    pass

  def before(self, gvar):
    pass

  def after(self, gvar):
    pass

  def run(self, gvar):
    self.before(gvar)
    for op in self.operators:
      op.run(gvar)
    self.after(gvar)
  
  def rollback(self, gvar):
    pass

  def set_query(self, query, stack_indices, indices):
    indices = indices.split(',') if len(indices) > 0 else []
    return query % (tuple(list(map(lambda x: stack_indices[int(x)] + 1, indices))))


class BFSIterator(BaseOperator):

  def run(self, gvar):
    op_name = "BFSIterator"
    try:
      op_start = time.time()
      op_id = self.props['id']
      parent_node_id = self.props.get('parent_node_id', 0)
      label = self.props['label']
      node_id = gvar.graph_mgr.create_node(gvar.task_id, parent_node_id, label)
      gvar.stack_nodes.append(node_id)
      gvar.stack_indices.append(0)

      print("task_id:", gvar.task_id)
      print("op_id:", op_id)
      print("task_url:", gvar.task_url)

      gvar.web_mgr.load(gvar.task_url)
      time.sleep(1) 
      gvar.graph_mgr.insert_node_property(gvar.stack_nodes[-1], 'url', gvar.task_url)
      gvar.web_mgr.wait_loading()
      time.sleep(self.props.get('delay', 0))

      if gvar.task_url != gvar.web_mgr.get_current_url():
        time.sleep(5)


      chaptcha_xpath = '//input[@id=\'captchacharacters\']' # for amazon
      check_chaptcha = gvar.web_mgr.get_elements_by_selenium_(chaptcha_xpath)
      sleep_time = 900
      random_time = random.randrange(1,61)
      sleep_time += int(random_time)
      while(len(check_chaptcha) != 0):
         print("Wait {} secs".format(str(sleep_time)))
         time.sleep(sleep_time)
         gvar.web_mgr.load(gvar.task_url)
         gvar.web_mgr.wait_loading()
         random_time = random.randrange(1,61)
         sleep_time += 300 + int(random_time)
         check_chaptcha = gvar.web_mgr.get_elements_by_selenium_(chaptcha_xpath)



      chaptcha_xpath = '//body[contains(text(),\'Reference\')]' # for rakuten
      check_chaptcha = gvar.web_mgr.get_elements_by_selenium_(chaptcha_xpath)
      sleep_time = 1
      while(len(check_chaptcha) != 0):
         print('Restart chrome') # for rakuten.jp
         gvar.web_mgr.restart(sleep_time)
         time.sleep(5)
         gvar.web_mgr.load(gvar.task_url)
         #gvar.graph_mgr.insert_node_property(gvar.stack_nodes[-1], 'url', gvar.task_url)
         gvar.web_mgr.wait_loading()
         time.sleep(self.props.get('delay', 0))
         sleep_time += 0
         if gvar.task_url != gvar.web_mgr.get_current_url():
           time.sleep(5)
         check_chaptcha = gvar.web_mgr.get_elements_by_selenium_(chaptcha_xpath)




      invalid_amazon_xpath = '//img[@alt=\'Dogs of Amazon\']'
      invalid_jomashop_xpath = '//div[@class=\'image-404\'] | //div[@class=\'product-buttons\']//span[contains(text(),\'OUT OF STOCK\')] | //div[contains(text(),\'Sold Out\')] |  //span[contains(text(),\'Ships In\')] |  //span[contains(text(),\'Contact us for\')]'
      invalid_jalando_xpath = '//h2[contains(text(),\'Out of stock\')] | //h1[contains(text(),\'find this page\')]'
      invalid_page_xpath = invalid_amazon_xpath + ' | ' + invalid_jomashop_xpath + ' | ' + invalid_jalando_xpath 
      is_invalid_page = gvar.web_mgr.get_elements_by_selenium_(invalid_page_xpath)
      if len(is_invalid_page) != 0:
        return




      if 'query' in self.props:
        gvar.web_mgr.get_elements_by_selenium_strong_(self.props['query'])

      if 'btn_query' in self.props and int(self.props['page_id']) != 1:
        res = gvar.web_mgr.get_value_by_selenium_strong(self.props['btn_query'],'alltext')
        print('btn cur :', res)
        print(self.props['page_id'])

        #if (int(res) != int(self.props['page_id'])) and res.isdigit():
        #  print('page number in button != page number in url')
        #  raise

        if str(self.props['page_id']) not in res:
          print('page number in button != page number in url')
          raise

 
      gvar.graph_mgr.insert_node_property(gvar.stack_nodes[-1], 'html', gvar.web_mgr.get_html())
      #+ page=%d에 준 값과 url 비교
      #+ 선택된 버튼의 값과 url의 page 값비교

      for op in self.operators:
        op_name = op.props['name']
        op.run(gvar)
        
      
      op_time = time.time() - op_start
      gvar.profiling_info[op_id] = { 'op_time' : op_time }
    except Exception as e:
      fname = '/home/pse/PSE-engine/htmls/%s.html' % str(gvar.task_id)
      gvar.web_mgr.store_page_source(fname)
      print("error html:", fname)
      err_msg = '================================== URL ==================================\n'
      err_msg += ' ' + str(gvar.task_url) + '\n\n'
      err_msg += '=============================== Opeartor ==================================\n'
      err_msg += ' ' + str(op_name) + '\n\n'
      err_msg += '================================ STACK TRACE ============================== \n' + str(traceback.format_exc())
      gvar.graph_mgr.log_err_msg_of_task(gvar.task_id, err_msg)

      if type(e) is OperatorError:
        raise e
      raise OperatorError(e, self.props['id'])


class OpenNode(BaseOperator):
  
  def run(self, gvar):
    try:
      op_start = time.time()
      op_id = self.props['id']
      label = self.props['label']
      parent_node_id = gvar.stack_nodes[-1]
      
      query = self.props['query']
      if 'indices' in self.props:
        query = self.set_query(query, gvar.stack_indices, self.props['indices'])
      
      essential = self.props.get("essential", False)
      if type(essential) != type(True): essential = eval(essential)
      if essential: 
        elements = gvar.web_mgr.get_elements_by_selenium_strong_(query)
      else: 
        elements = gvar.web_mgr.get_elements_by_selenium_(query)

      num_elements = len(elements)
      print(num_elements, query)
      if num_elements == 0 and int(self.props.get('self', 0)) == 1:
        num_elements = 1
      
      for i in range(num_elements):
        print(i,"-th loop#############################################")
        node_id = gvar.graph_mgr.create_node(gvar.task_id, parent_node_id, label)
        gvar.stack_nodes.append(node_id)
        gvar.stack_indices.append(i)
        for op in self.operators:
          op.run(gvar)
        gvar.stack_nodes.pop()
        gvar.stack_indices.pop()
      op_time = time.time() - op_start
      gvar.profiling_info[op_id] = { 'op_time' : op_time }
    except Exception as e:
      if type(e) is OperatorError:
        raise e
      raise OperatorError(e, self.props['id'])

class SendPhoneKeyOperator(BaseOperator):
  def run(self, gvar):
    try:
      op_id = self.props['id']
      op_start = time.time()
      print("Do SendPhoneKeys")
      
      number = input("varification number: ") 
      query = self.props["query"]
      gvar.web_mgr.end_keys_to_elements_strong(query,number)
      time.sleep(int(column.get('delay', 0)))

      op_time = time.time() - op_start
      gvar.profiling_info[op_id] = { 'op_time' : op_time }
    except Exception as e:
      raise OperatorError(e, self.props['id'])



class WaitOperator(BaseOperator):
  def run(self, gvar):
    try:
      op_id = self.props['id']
      print("Do Wait {} secs".format(self.props.get('wait',0)))
      time.sleep(int(self.props.get('wait', 0)))
    except Exception as e:
      raise OperatorError(e, self.props['id'])


class ScrollOperator(BaseOperator):
  def run(self, gvar):
    try:
      op_id = self.props['id']
      print("Do Scroll")
      op_start = time.time()
      gvar.web_mgr.scroll_to_bottom()

      op_time = time.time() - op_start
      gvar.profiling_info[op_id] = { 'op_time' : op_time }
    except Exception as e:
      fname = '/home/pse/PSE-engine/htmls/%s.html' % str(gvar.task_id)
      raise OperatorError(e, self.props['id'])



class HoverOperator(BaseOperator):
  def run(self, gvar):
    try:
      op_id = self.props['id']
      print("Do Hover")
      op_start = time.time()
      xpath = self.props['query']
      gvar.web_mgr.move_to_elements(xpath)

      op_time = time.time() - op_start
      gvar.profiling_info[op_id] = { 'op_time' : op_time }
    except Exception as e:
      fname = '/home/pse/PSE-engine/htmls/%s.html' % str(gvar.task_id)
      raise OperatorError(e, self.props['id'])



class LoginOperator(BaseOperator):
  def run(self, gvar):
    try:
      op_id = self.props['id']
      op_start = time.time()
      print("before login")
      print(gvar.web_mgr.get_current_url())
      print("Do Login")
      gvar.web_mgr.login_by_xpath(self.props["user_id"],self.props["pwd"],self.props["user_id_query"],self.props["pwd_query"],self.props["click_query"])
      time.sleep(int(self.props.get('delay', 10)))
      op_time = time.time() - op_start
      print("after login")
      print(gvar.web_mgr.get_current_url())
      fname = '/home/pse/PSE-engine/htmls/test.html'
      gvar.web_mgr.store_page_source(fname)
      gvar.profiling_info[op_id] = { 'op_time' : op_time }
    except Exception as e:
      raise OperatorError(e, self.props['id'])



class SendKeysOperator(BaseOperator):
  def run(self, gvar):
    try:
      op_id = self.props['id']
      op_start = time.time()
      print("Do Input (SendKeys)")
      for column in self.props["queries"]:
        query = column["query"]
        gvar.web_mgr.send_keys_to_elements(query, column['value'])
      op_time = time.time() - op_start
      gvar.profiling_info[op_id] = { 'op_time' : op_time }
    except Exception as e:
      raise OperatorError(e, self.props['id'])
        

class ClickOperator(BaseOperator):

  def run(self, gvar):
    try:
      time_sleep = int(self.props.get('delay', 0))
      op_id = self.props['id']
      op_start = time.time()
      print("Do Click")
      for column in self.props["queries"]:
        query = column["query"]
        if 'indices' in column:
          query = self.set_query(query, gvar.stack_indices, column['indices'])
        essential = column.get("essential", False)
        repeat = column.get("repeat", False)
        if type(essential) != type(True): essential = eval(essential)
        if type(repeat) != type(True): repeat = eval(repeat)
        if repeat:
          gvar.web_mgr.click_elements_repeat(query, time_sleep)
        else:
          if essential:
            gvar.web_mgr.click_elements_strong(query)
          else:
            gvar.web_mgr.click_elements(query)
        time.sleep(int(column.get('delay', 5)))
      op_time = time.time() - op_start
      gvar.profiling_info[op_id] = { 'op_time' : op_time }
    except Exception as e:
      raise OperatorError(e, self.props['id'])

class MoveCursorOperator(BaseOperator):

  def run(self, gvar):
    try:
      op_id = self.props['id']
      op_start = time.time()
      print("Do MoveCursor")
      for column in self.props["queries"]:
        query = column["query"]
        if 'indices' in column:
          query = self.set_query(query, gvar.stack_indices, column['indices'])
        essential = column.get("essential", False)
        if type(essential) != type(True): essential = eval(essential)
        if essential:
          gvar.web_mgr.move_to_elements_strong(query)
        else:
          gvar.web_mgr.move_to_elements(query)
      op_time = time.time() - op_start
      gvar.profiling_info[op_id] = { 'op_time' : op_time }
    except Exception as e:
      raise OperatorError(e, self.props['id'])

class Expander(BaseOperator):

  def run_0(self, gvar):
    op_start = time.time()
    op_id = self.props['id']
    gvar.results[op_id] = [(gvar.task_id, gvar.stack_nodes[-1], [gvar.web_mgr.get_current_url()])]
    op_time = time.time() - op_start
    gvar.profiling_info[op_id] = { 'op_time' : op_time }
   

  def run_1(self, gvar):

    op_start = time.time()

    op_id = self.props['id']
    query = self.props['query']
    if 'indices' in self.props:
      query = self.set_query(query, gvar.stack_indices, self.props['indices'])
    attr = self.props["attr"]
    site = self.props.get("site", None)
    attr_delimiter = self.props.get("attr_delimiter", None)
    attr_idx = self.props.get("attr_idx", None)
    suffix = self.props.get("suffix", "")
    self_url = self.props.get('matchSelf', False)
    if type(self_url) != type(True): self_url = eval(self_url)
    no_matching_then_self = self.props.get('noMatchSelf',False)
    if type(no_matching_then_self) != type(True): no_matching_then_self = eval(no_matching_then_self)
    cur_url = gvar.web_mgr.get_current_url()

    xpaths_time = time.time()
    result = gvar.web_mgr.get_values_by_selenium(query, attr)
    xpaths_time = time.time() - xpaths_time



    #if url_query is not None:
    #  for idx, res in enumerate(result):
    #    result[idx] = int(result[idx])
    #  if len(result) == 0:
    #    if no_matching_then_self == 1: result = [gvar.web_mgr.get_current_url()]
    #  else:
    #    for idx, res in enumerate(result):
    #      result[idx] = cur_url.split('?')[0] + (url_query % int(result[idx]))
    #else:
    if attr_delimiter is not None:
      for idx, res in enumerate(result):
        result[idx] = result[idx].split(attr_delimiter)[attr_idx] + str(suffix)
      if len(result) == 0:
        self_url = 1
        if no_matching_then_self == 1: result = [gvar.web_mgr.get_current_url()]
      else:
        self_url = 0

    #if site is not None:
    #  if len(result) == 0:
    #    if self_url == 1: result = [gvar.web_mgr.get_current_url()]
    #    else:
    #      essential = self.props.get("essential", False)
    #      if type(essential) != type(True): essential = eval(essential)
    #      if essential: raise
    #    print(result)
    #  else:
    #    for idx, res in enumerate(result):
    #        result[idx] = str(site) + str(res)
    #    if no_matching_then_self == 1:
    #      result.append(gvar.web_mgr.get_current_url())
    #    while str(site) in result: result.remove(str(site))
    #    print(result)
    if len(result) == 0:
      if no_matching_then_self == 1: result = [gvar.web_mgr.get_current_url()]
      else:
        essential = self.props.get("essential", False)
        if type(essential) != type(True): essential = eval(essential)
        if essential: raise
    else:
      if self_url == 1:
        result.append(gvar.web_mgr.get_current_url())


    gvar.results[op_id] = [(gvar.task_id, gvar.stack_nodes[-1], result)]
    op_time = time.time() - op_start
    gvar.profiling_info[op_id] = { 
      'op_time' : op_time, 
      'xpaths_time': xpaths_time, 
      'num_elements':  len(result)
    }
    print(result)
    return

  def run(self, gvar):
    try:
      if len(self.props.get("query", "").strip()) > 0:
        return self.run_1(gvar)
      else:
        return self.run_0(gvar)
    except Exception as e:
      raise OperatorError(e, self.props['id'])



class ValuesScrapper(BaseOperator):

  def before(self, gvar):
    try:
      op_time = time.time()
      print('Do ValuesScrapper')
      op_id = self.props['id']
      pairs = self.props['queries']
      result = {}

      build_time = time.time()
      gvar.web_mgr.build_lxml_tree()
      build_time = time.time() - build_time

      xpaths_time = time.time()
      for pair in pairs:
        key = pair['key']
        xpath = pair['query']
        attr = pair['attr']
        print(pair)
        if xpath == '':
          if attr == 'url': result[key] = str(gvar.web_mgr.get_current_url()).strip()
        else:
          if 'indices' in pair:
            print(xpath)
            print(gvar.stack_indices)
            print(pair['indices'])
            xpath = self.set_query(xpath, gvar.stack_indices, pair['indices'])
          essential = pair.get('essential', False)
          if type(essential) != type(True): essential = eval(essential)
          if attr == 'outerHTML': 
            if essential:
              result[key] = gvar.web_mgr.get_subtree_with_style_strong(xpath)
            else:
              result[key] = gvar.web_mgr.get_subtree_with_style(xpath)
            continue
          if attr == 'innerHTML': 
            if essential:
              result[key] = gvar.web_mgr.get_subtree_no_parent_with_style_strong(xpath)
            else:
              result[key] = gvar.web_mgr.get_subtree_no_parent_with_style(xpath)
            continue
          if essential: 
            result[key] = gvar.web_mgr.get_value_by_lxml_strong(xpath, attr)
          else:
            result[key] = gvar.web_mgr.get_value_by_lxml(xpath, attr)
      xpaths_time = time.time() - xpaths_time
      db_time = time.time()
      for key, value in result.items():
        gvar.graph_mgr.insert_node_property(gvar.stack_nodes[-1], key, value)
      db_time = time.time() - db_time
    
      op_time = time.time() - op_time
      gvar.profiling_info[op_id] = { 
        'op_time' : op_time, 
        'build_time': build_time, 
        'xpaths_num': len(pairs), 
        'xpaths_time': xpaths_time, 
        'db_num': len(result), 
        'db_time': db_time 
      }
    except Exception as e:
      raise OperatorError(e, self.props['id'])


class ListsScrapper(BaseOperator):

  def run(self, gvar):
    try:
      op_time = time.time()
      print('Do ListsScrapper')
    
      op_id = self.props['id']
      queries = self.props['queries']
      
      build_time = time.time()
      gvar.web_mgr.build_lxml_tree()
      build_time = time.time() - build_time

      result = {}

      xpaths_time = time.time()

      for query in queries:
        key = query['key']
        xpath = query['query']
        if 'indices' in query:
          xpath = self.set_query(xpath, gvar.stack_indices, query['indices'])
        attr = query['attr']
        essential = query.get('essential', False)
        if type(essential) != type(True): essential = eval(essential)
        if essential: 
          result[key] = gvar.web_mgr.get_values_by_lxml_strong(xpath, attr)
        else:
          result[key] = gvar.web_mgr.get_values_by_lxml(xpath, attr)
      
      xpaths_time = time.time() - xpaths_time

      print(result)

      db_time = time.time()
      for key, value in result.items():
        gvar.graph_mgr.insert_node_property(gvar.stack_nodes[-1], key, value)
      db_time = time.time() - db_time

      op_time = time.time() - op_time
      gvar.profiling_info[op_id] = { 
        'op_time' : op_time, 
        'build_time': build_time,
        'xpaths_time': xpaths_time,
        'db_time': db_time,
        'num_results': len(result)
      }
    except Exception as e:
      raise OperatorError(e, self.props['id'])



class DictsScrapper(BaseOperator):

  def run(self, gvar):
    try:
      op_time = time.time()
      print('Do dictionary scrapper')
      op_id = self.props['id']
      queries = self.props['queries']

      build_time = time.time()
      gvar.web_mgr.build_lxml_tree()
      build_time = time.time() - build_time

      result = {}

      xpaths_time = time.time()

      for query in queries:
        key = query['key']
        rows_query = query['rows_query']
        if 'rows_indices' in query:
          rows_query = self.set_query(rows_query, gvar.stack_indices, query['rows_indices'].strip())
        key_query = query['key_query']
        if 'key_indices' in query:
          key_query = self.set_query(key_query, gvar.stack_indices, query['key_indices'].strip())
        key_attr = query['key_attr']
        value_query = query['value_query']
        if 'value_indices' in query:
          value_query = self.set_query(value_query, gvar.stack_indices, query['value_indices'].strip())
        value_attr = query['value_attr']
          
        essential = query.get('essential', False)
        if type(essential) != type(True): essential = eval(essential)

        if essential:
          result[key] = gvar.web_mgr.get_key_values_by_lxml_strong(rows_query, key_query, key_attr, value_query, value_attr)
        else:
          result[key] = gvar.web_mgr.get_key_values_by_lxml(rows_query, key_query, key_attr, value_query, value_attr)
        title_query = query['title_query']
        result[key]['dictionary_title0'] = gvar.web_mgr.get_value_by_lxml(title_query, 'alltext')

      xpaths_time = time.time() - xpaths_time

      db_time = time.time()
      for key, value in result.items():
        gvar.graph_mgr.insert_node_property(gvar.stack_nodes[-1], key, value)
      db_time = time.time() - db_time
      
      print(result)
    
      op_time = time.time() - op_time
      gvar.profiling_info[op_id] = { 
        'op_time' : op_time, 
        'build_time': build_time,
        'xpaths_time': xpaths_time,
        'db_time': db_time,
        'num_results': len(result)
      }
    except Exception as e:
      raise OperatorError(e, self.props['id'])


worker_operators = {
  'BFSIterator': BFSIterator,
  'SendKeysOperator': SendKeysOperator,
  'LoginOperator': LoginOperator,
  'SendPhoneKeyOperator': SendPhoneKeyOperator,
  'ClickOperator': ClickOperator,
  'Expander': Expander,
  'ValuesScrapper': ValuesScrapper,
  'ListsScrapper': ListsScrapper,
  'DictsScrapper': DictsScrapper,
  'OpenNode': OpenNode,
  'OpenURL': BFSIterator,
  'Wait': WaitOperator,
  'Scroll': ScrollOperator,
  'Hover': HoverOperator,
  'Input': SendKeysOperator,
}


def materialize(lop, isRecursive):
  
  pop = worker_operators[lop["name"]]()
  pop.props = lop

  if isRecursive == False:
    pop.operators = lop['ops']
    return pop
  
  pop.operators = []
  for lchild in lop.get('ops', []):
    pchild = materialize(lchild, isRecursive)
    pop.operators.append(pchild)

  return pop

if __name__ == '__main__':
  gvar = GlovalVariable()

  gvar.graph_mgr = GraphManager()
  gvar.graph_mgr.connect("host=141.223.197.36 port=5434 user=smlee password=smlee dbname=pse")
  gvar.web_mgr = WebManager()
  gvar.task_id = 0
  gvar.exec_id = 0
  gvar.task_url = "https://www.amazon.com/Sensodyne-Pronamel-Whitening-Strengthening-Toothpaste/dp/B0762LYFKP?pf_rd_p=9dbbfba7-e756-51ca-b790-09e9b92beee1&pf_rd_r=EG4J8ZAJZNB9B3HBQ9G1&pd_rd_wg=W8hx6&ref_=pd_gw_ri&pd_rd_w=kynj4&pd_rd_r=6365323e-7c16-4273-a2c5-5d85b04565f5"
  gvar.task_zipcode_url = "https://www.amazon.com/gp/delivery/ajax/address-change.html?locationType=LOCATION_INPUT&zipCode=94024&storeContext=offce-products&deviceType=web&pageType=detail&actionSource=glow"
  bfs_iterator = BFSIterator()
  bfs_iterator.props = { 'id': 1, 'query': "//span[@id='productTitle']" }
  bfs_iterator.run(gvar)  
 
