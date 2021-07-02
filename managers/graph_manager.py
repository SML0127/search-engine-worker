import psycopg2
import traceback
import json
import hashlib
import urllib.request
import cfscrape
import pathlib
import string
from io import BytesIO
from PIL import Image
import requests
from price_parser import Price
from datetime import datetime, timedelta, date
from urllib.parse import urlparse
from url_normalize import url_normalize
from functools import partial
print_flushed = partial(print, flush=True)

def is_hex_str(s):
    return set(s).issubset(string.hexdigits)

class GraphManager():

  def __init__(self):
    self.gp_conn = None
    self.gp_cur = None
    self.pg_conn = None
    self.pg_cur = None

  def init(self, settings):
    try:
      self.conn_info = settings['graph_db_conn_info']
      self.pg_conn = psycopg2.connect(self.conn_info)
      self.gp_conn = psycopg2.connect(self.conn_info)
      self.pg_conn.autocommit = True
      self.gp_conn.autocommit = True
      self.pg_cur = self.pg_conn.cursor()
      self.gp_cur = self.pg_cur
    except Exception as e:
      print_flushed(str(traceback.format_exc()))
      raise e

  def connect(self, pg_info, gp_info=''):
    try:
      self.pg_conn = psycopg2.connect(pg_info)
      self.pg_cur = self.pg_conn.cursor()
      if gp_info != '' and pg_info != gp_info:
        self.gp_conn = psycopg2.connect(gp_info)
        self.gp_cur = self.gp_conn.cursor()
      else:
        self.gp_conn = self.pg_conn
        self.gp_cur = self.pg_cur
    except Exception as e:
      print_flushed(str(traceback.format_exc()))
      raise e

  def disconnect(self):
    self.gp_conn.commit()
    self.pg_conn.commit()
    self.gp_conn.close()
    self.pg_conn.close()
    pass

  def create_db(self):
    try:
      query = 'create table node (id bigserial primary key, parent_id bigint, task_id bigint, label integer);'
      self.pg_cur.execute(query)
      self.pg_conn.commit()
    except Exception as e:
      self.pg_conn.rollback()
      print_flushed(str(traceback.format_exc()))
      raise e
    try:
      query = 'create table node_property(id bigserial primary key, node_id bigint, key varchar(1048), value json);'
      self.gp_cur.execute(query)
      self.gp_conn.commit()
    except Exception as e:
      self.gp_conn.rollback()
      print_flushed(str(traceback.format_exc()))
      raise e

  def drop_db(self):
    try:
      query = 'drop table if exists node;'
      self.pg_cur.execute(query)
      self.pg_conn.commit()
    except Exception as e:
      self.pg_conn.rollback()
      print_flushed(str(traceback.format_exc()))
      raise e
    try:
      query = 'drop table if exists node_property;'
      self.gp_cur.execute(query)
      self.gp_conn.commit()
    except Exception as e:
      self.gp_conn.rollback()
      print_flushed(str(traceback.format_exc()))
      raise e

  def create_node(self, task_id, parent_id, label):
    try:
      query = 'insert into node(task_id, parent_id, label) '
      query += 'values(%s, %s, %s)'
      query += 'returning id;'
      self.pg_cur.execute(query, (str(task_id), str(parent_id), str(label)))
      result = self.pg_cur.fetchone()[0]
      self.pg_conn.commit()
      query = 'COMMIT;'
      self.pg_cur.execute(query)
      return result
    except Exception as e:
      self.pg_conn.rollback()
      print_flushed(str(traceback.format_exc()))
      raise e

  def find_nodes_of_execution(self, exec_id):
    try:
      query = 'select n.id from node n, stage s, task t, (select max(n1.label) as max_label from node n1, stage s1, task t1 where n1.task_id = t1.id and t1.stage_id = s1.id and s1.execution_id = '+str(exec_id)+') as l where n.task_id = t.id and t.stage_id = s.id and s.execution_id = '+str(exec_id)+' and n.label = l.max_label;'
      self.pg_cur.execute(query)
      result = self.pg_cur.fetchall()
      self.pg_conn.commit()
      return list(map(lambda x: x[0], result))
    except Exception as e:
      self.pg_conn.rollback()
      print_flushed(str(traceback.format_exc()))
      raise e

  

  def find_nodes_of_execution_with_label(self, exec_id, label):
    try:
      query =  'select n.id'
      query += ' from node n, stage s, task t'
      query += ' where n.task_id = t.id and t.stage_id = s.id and s.execution_id = %s and n.label = %s order by n.id asc'
      print_flushed(query % (str(exec_id), str(label)))
      self.pg_cur.execute(query, (str(exec_id), str(label)))
      result = self.pg_cur.fetchall()
      self.pg_conn.commit()
      return list(map(lambda x: x[0], result))
    except Exception as e:
      self.pg_conn.rollback()
      print_flushed(str(traceback.format_exc()))
      raise e

  def find_nodes_of_task_with_label(self, task_id, label):
    try:
      query =  'select id'
      query += ' from node n'
      query += ' where n.task_id = %s and n.label = %s;'
      self.pg_cur.execute(query, (str(task_id), str(label)))
      result = self.pg_cur.fetchall()
      self.pg_conn.commit()
      return list(map(lambda x: x[0], result))
    except Exception as e:
      self.pg_conn.rollback()
      print_flushed(str(traceback.format_exc()))
      raise e

  def find_n_hop_neighbors(self, node_id, labels):
    try:
      query =  'select n{}.id'.format(len(labels), len(labels))
      query += ' from node n1'
      for i in range(2, len(labels) + 1):
        query += ', node n{}'.format(i)
      query += ' where n1.parent_id = {} and n1.label = {}'.format(node_id, labels[0])
      for i in range(2, len(labels) + 1):
        query += ' and n{}.parent_id = n{}.id and n{}.label = {}'.format(i, i-1, i, labels[i-1])
      query += ' order by n{}.id;'.format(len(labels), len(labels))
      self.pg_cur.execute(query)
      result = list(map(lambda x: x[0], self.pg_cur.fetchall()))
      self.pg_conn.commit()
      return result
    except Exception as e:
      self.pg_conn.rollback()
      print_flushed(str(traceback.format_exc()))
      raise e


  def insert_node_property(self, nodeId, key, value):
    try:
      query =  'INSERT INTO node_property (node_id, key, value) '
      query += 'VALUES (%s, %s, %s)'
      value = json.dumps(value)
      self.gp_cur.execute(query, (str(nodeId), str(key), str(value)))
      self.gp_conn.commit()
      if key == 'url':
        query_mpid = "select my_product_id from url_to_mpid where url = '{}'".format(value)
        self.gp_cur.execute(query_mpid)
        rows = self.gp_cur.fetchall()
        if len(rows) == 0:
           query_insert = "insert into url_to_mpid(url) values('{}')".format(value)
           self.gp_cur.execute(query_insert)
           self.gp_conn.commit()


    except:
      self.gp_conn.rollback()
      print_flushed(str(traceback.format_exc()))
      raise



  def get_mpid_from_mysite_without_up_to_date(self, exec_id):
    try:
      query = 'select job_id from execution where id = {}'.format(exec_id)
      self.gp_cur.execute(query)
      job_id = self.gp_cur.fetchone()[0]
      self.gp_conn.commit()

      query = "select mpid from job_source_view where job_id = {} and status != 4 and status != 0".format(job_id)
      #query = "select mpid from job_source_view where job_id = {} and status =3".format(job_id)
      self.gp_cur.execute(query)
      tmp = self.gp_cur.fetchall()
      self.gp_conn.commit()
      result = []
      for i in tmp:
        result.append(i[0])
      return result
    except:
      self.gp_conn.rollback()
      print_flushed(str(traceback.format_exc()))
      raise
 
 

  def get_mpid_from_mysite(self, exec_id):
    try:
      query = 'select job_id from execution where id = {}'.format(exec_id)
      self.gp_cur.execute(query)
      job_id = self.gp_cur.fetchone()[0]
      self.gp_conn.commit()

      query = "select mpid from job_source_view where job_id = {} and status != 4".format(job_id)
      self.gp_cur.execute(query)
      tmp = self.gp_cur.fetchall()
      self.gp_conn.commit()
      result = []
      for i in tmp:
        result.append(i[0])
      return result
    except:
      self.gp_conn.rollback()
      print_flushed(str(traceback.format_exc()))
      raise
 
  def get_node_properties_from_mysite(self, job_id, mpid):
    try:
      #query = 'select job_id from execution where id = {}'.format(exec_id)
      #self.gp_cur.execute(query)
      #job_id = self.gp_cur.fetchone()[0]

      query = "select column_name from information_schema.columns where table_name = 'job_source_view'";
      self.gp_cur.execute(query)
      col_names_tmp = self.gp_cur.fetchall()
      col_names = []
      for i in col_names_tmp:
        col_names.append(i[0])

      values = ''
      for name in col_names:
        values += str(name) + ', '
      values = values[0:-2]
      
      query = 'select '+values+' from job_source_view where mpid = {} and job_id = {}'.format(mpid, job_id)
      self.gp_cur.execute(query)
      col_values = self.gp_cur.fetchall()
      col_values = col_values[0]
      
      result = {}
      for i in range(0, len(col_names)):
        if col_names[i] == 'name' or col_names[i] == 'price' or col_names[i] == 'shipping_price':
           if is_hex_str(col_values[i]) == True:
             try:
                result[col_names[i]] = bytes.fromhex(col_values[i]).decode()
             except:
                result[col_names[i]] = col_values[i]
                pass
           else:
             result[col_names[i]] = col_values[i]
        else:    
           result[col_names[i]] = col_values[i]
      

      query = 'select image_url from job_thumbnail_source_view where mpid = {} and job_id = {}'.format(mpid, job_id)
      self.gp_cur.execute(query)
      rows = self.gp_cur.fetchall()
      result['images'] = []
      for row in rows:
        result['images'].append(row[0])
      

      query = 'select key, value from job_description_source_view where mpid = {} and job_id = {}'.format(mpid, job_id)
      self.gp_cur.execute(query)
      rows = self.pg_cur.fetchall()
 
      for row in rows:
        if 'sha256' not in row[0]:
          result[row[0]] = bytes.fromhex(row[1]).decode()


      query = "select option_name, option_value, stock from job_option_source_view where mpid = {} and job_id = {}".format(mpid, job_id)
      self.gp_cur.execute(query)
      rows = self.gp_cur.fetchall()
      self.gp_conn.commit()
      result['option_name'] = []
      result['option_value'] = {}
      for row in rows:
        op_n = bytes.fromhex(row[0]).decode()
        op_v = bytes.fromhex(row[1]).decode()
        #op_v_stock = row[2]
        if op_n not in result['option_name']:
          result['option_name'].append(op_n)
        if (op_n != 'option_matrix_col_name' and op_n != 'option_matrix_row_name'):
          if result['option_value'].get(op_n,None) == None:
            result['option_value'][op_n] = []
          result['option_value'][op_n].append(op_v)
      tmp_list = result['option_name']
      if len(tmp_list) >=1:
         if tmp_list[0] == 'option_maxtrix_value':
            if tmp_list[1] == 'option_matrix_col_name' or tmp_list[1] == 'option_matrix_row_name': 
               tmp_list.reverse()
               result['option_name'] = tmp_list
      return result
    except:
      self.gp_conn.rollback()
      print_flushed(str(traceback.format_exc()))
      raise




  # get description and product name using mpid
  def get_pname_and_description_using_mpid(self, mpid):
    try:
      query = 'select url from url_to_mpid where my_product_id = {}'.format(mpid)
      self.gp_cur.execute(query)
      rows = self.gp_cur.fetchone()
      url = rows[0]
      query = "select node_id from node_property where key = 'url' and value::text like '_"+str(url)+"_'"
      self.gp_cur.execute(query)
      rows = self.gp_cur.fetchone()
      node_id = rows[0]
      query = "select value from node_property where node_id = "+str(node_id)+" and key = 'description'"
      self.gp_cur.execute(query)
      rows = self.gp_cur.fetchone()
      description = rows[0]
      query = "select value from node_property where node_id = "+str(node_id)+" and key = 'name'"
      self.gp_cur.execute(query)
      rows = self.gp_cur.fetchone()
      name = rows[0]
      self.gp_conn.commit()
      return (node_id,name, description)
    except:
      self.gp_conn.rollback()
      print_flushed(str(traceback.format_exc()))
      raise


  # get url using mpid
  def get_url_using_mpid(self, mpid):
    try:
      query = 'select url from url_to_mpid where my_product_id = {}'.format(mpid)
      self.gp_cur.execute(query)
      rows = self.gp_cur.fetchall()
      self.gp_conn.commit()
      return rows
    except:
      self.gp_conn.rollback()
      print_flushed(str(traceback.format_exc()))
      raise

  #def check_is_first_upload(self, job_id, mpid):
  #  try:
  #    query = "select tpid from job"+str(job_id)+"source_view where mpid = " + str(mpid)+";"
  #    self.gp_cur.execute(query)
  #    row = self.gp_cur.fetchone()
  #    if row is None:
  #      result = True
  #    else row if not None:
  #      result = row[0]
  #    self.gp_conn.commit()
  #    return result
  #  except:
  #    self.gp_conn.rollback()
  #    print_flushed(str(traceback.format_exc()))
  #    raise

  def check_status_of_product(self, job_id, mpid):
    try:
      query = "select status from job_source_view where mpid = {} and job_id = {};".format(mpid, job_id)
      self.gp_cur.execute(query)
      row = self.gp_cur.fetchone()
      result = row[0]
      self.gp_conn.commit()
      return int(result)
    except:
      self.gp_conn.rollback()
      print_flushed(str(traceback.format_exc()))
      raise

  
  def get_node_properties(self, nodeId):
    try:
      query = 'select key, value from node_property where node_id = {}'.format(nodeId)
      self.gp_cur.execute(query)
      rows = self.gp_cur.fetchall()
      self.gp_conn.commit()
      result = {}
      for row in rows:
        result[row[0]] = row[1]
      purl = result['url']

      query_mpid = "select my_product_id from url_to_mpid where url like '{}'".format(purl)
      self.gp_cur.execute(query_mpid)
      self.gp_conn.commit()
      rows = self.gp_cur.fetchall()
      if len(rows) == 0:
         query_insert = "insert into url_to_mpid(url) values('{}') returning my_product_id".format(purl)
         self.gp_cur.execute(query_insert)
         self.gp_conn.commit()
         rows = self.gp_cur.fetchall()
         mpid = rows[0][0]
         result['mpid'] = mpid
         query = 'COMMIT;'
         self.gp_cur.execute(query)
      else:
         result['mpid'] = rows[0][0]

      return result
    except:
      self.gp_conn.rollback()
      print_flushed(str(traceback.format_exc()))
      raise


  def get_latest_eid_from_job_id(self, job_id):
    try:
      query =  'select max(id) from execution where job_id = {}'.format(job_id)
      self.gp_cur.execute(query)
      row = self.gp_cur.fetchone()
      self.gp_conn.commit()
      exec_id = row[0]
      return exec_id
    except:
      self.gp_conn.rollback()
      print_flushed(str(traceback.format_exc()))
      raise

  def get_max_label_from_eid(self, eid):
    try:
      query = 'select max(n1.label) as max_label from node n1, stage s1, task t1 where n1.task_id = t1.id and t1.stage_id = s1.id and s1.execution_id = '+str(eid)+';'
      self.gp_cur.execute(query)
      row = self.gp_cur.fetchone()
      self.gp_conn.commit()
      exec_id = row[0]
      return exec_id
    except:
      self.gp_conn.rollback()
      print_flushed(str(traceback.format_exc()))
      raise




  def get_job_id_from_eid(self, exec_id):
    try:
      query =  'select job_id from execution where id = {}'.format(exec_id)
      self.gp_cur.execute(query)
      row = self.gp_cur.fetchone()
      self.gp_conn.commit()
      exec_id = row[0]
      return exec_id
    except:
      self.gp_conn.rollback()
      print_flushed(str(traceback.format_exc()))
      raise

  def none_to_blank(self, str):
     if not str or str is None or str == None or str == "None":
        return ''
     else:
        return str


  def set_status_for_duplicated_data(self):
    try:
      query = 'update job_source_view set status = 4 where id in (select id from job_source_view where id not in ( select max(id) from job_source_view group by groupby_key_sha256));'
      self.gp_cur.execute(query)
      self.gp_conn.commit()
      return ;
    except:
      self.gp_conn.rollback()
      print_flushed(str(traceback.format_exc()))
      raise



  #(id integer primary key generated always as identity, job_id integer, mpid integer unique, targetsite_url varchar(256), tpid integer, upload_time timestamp);
  def insert_tpid_into_history_table(self, job_id, targetsite_url, mpid, tpid):
    try:
      targetsite_url = url_normalize(targetsite_url)
      query = "insert into tpid_history(job_id, mpid, targetsite_url, tpid) values({},{},'{}',{})".format(job_id, mpid, targetsite_url, tpid)
      self.gp_cur.execute(query)
      self.gp_conn.commit()
      return
    except:
      self.gp_conn.rollback()
      print_flushed(str(traceback.format_exc()))
      raise


  def delete_from_tpid_mapping_table(self, tpid):
    try:

      query = "BEGIN;  Lock table tpid_mapping in EXCLUSIVE MODE; delete from tpid_mapping where tpid = {}; COMMIT;".format(tpid)
      print_flushed(query)
      self.gp_cur.execute(query)
      self.gp_conn.commit()

      return 
    except:
      self.gp_conn.rollback()
      print_flushed(str(traceback.format_exc()))
      raise



  def update_tpid_into_mapping_table(self, job_id, tpid, mpid, targetsite_url):
    try:
      #query = "select count(*) from tpid_mapping where job_id = {} and targetsite_url = '{}' and mpid = {}".format(job_id, targetsite_url, mpid)
      targetsite_url = url_normalize(targetsite_url)
      query = "select count(*) from tpid_mapping where targetsite_url = '{}' and mpid = {}".format(targetsite_url, mpid)
      print_flushed(query)
      self.gp_cur.execute(query)
      rows = self.gp_cur.fetchone()
      result = rows[0]
      print_flushed("count: ", result)
      if int(result) == 0:
        self.insert_tpid_into_mapping_table(job_id, targetsite_url, mpid, tpid)
      else: 

        query = "BEGIN;  Lock table tpid_mapping in EXCLUSIVE MODE; update tpid_mapping set tpid = {}, upload_time = now() where targetsite_url = '{}' and mpid = {}; COMMIT".format(tpid, targetsite_url, mpid)
        print_flushed(query)
        self.gp_cur.execute(query)
        self.gp_conn.commit()

      return 
    except:
      self.gp_conn.rollback()
      print_flushed(str(traceback.format_exc()))
      raise


  def check_is_item_uploaded(self, job_id, targetsite_url, mpid):
    try:
      targetsite_url = url_normalize(targetsite_url)
      query = "select count(*) from tpid_mapping where targetsite_url = '{}' and mpid = {}".format(targetsite_url, mpid)
      print_flushed("select count(*) from tpid_mapping where targetsite_url = '{}' and mpid = {}".format(targetsite_url, mpid))
      self.gp_cur.execute(query)
      rows = self.gp_cur.fetchone()
      result = rows[0]
      print_flushed(result)
      if int(result) == 0:
        return False
      else:
        return True
    except:
      self.gp_conn.rollback()
      print_flushed(str(traceback.format_exc()))
      raise


  def insert_tpid_into_mapping_table(self, job_id, targetsite_url, mpid, tpid):
    try:
      targetsite_url = url_normalize(targetsite_url)
      while True:
        query = "BEGIN;  Lock table tpid_mapping in EXCLUSIVE MODE; insert into tpid_mapping(job_id, mpid, targetsite_url, tpid) values({},{},'{}',{}); COMMIT;".format(job_id, mpid, targetsite_url, tpid)
        print_flushed(query)
        self.gp_cur.execute(query)
        self.gp_conn.commit()

        print_flushed('------------------ Check is inserted ----------------')
        query = "select count(*) from tpid_mapping where targetsite_url = '{}' and mpid = {}".format(targetsite_url, mpid)
        print_flushed(query)
        self.gp_cur.execute(query)
        rows = self.gp_cur.fetchone()
        result = rows[0]
        print_flushed("count: ", result)
        if int(result) != 0:
          break;
        else:
          print_flushed("Fail Insert mpid = {}!!".format(mpid)) 
      print_flushed("Success Insert mpid = {}".format(mpid)) 
      return
    except:
      self.gp_conn.rollback()
      print_flushed(str(traceback.format_exc()))
      raise


  def get_tpid_for_hide(self):
    try:
      query =  "select t1.mpid, t1.tpid from tpid_mapping_backup as t1, (select t1.job_id, t1.mpid from (select t1.job_id, t2.mpid from job_id_to_site_code as t1 , mpid_now as t2 where concat(10,CAST(t2.site_code AS text),'') = CAST(t1.site_code as text)) as t1 left join (select v1.mpid from (select t1.job_id, t2.mpid from job_id_to_site_code as t1 , mpid_now as t2 where concat(10,CAST(t2.site_code AS text),'') = CAST(t1.site_code as text)) as v1 inner join job_source_view as v2 on v1.mpid = v2.mpid and v1.job_id = v2.job_id) as t2 on t1.mpid = t2.mpid where t2.mpid is NULL) as t2 where t1.job_id = t2.job_id and t1.mpid = t2.mpid and t1.targetsite_url like '%now%';"
      self.gp_cur.execute(query)
      rows = self.gp_cur.fetchall()
      return result
    except:
      self.gp_conn.rollback()
      print_flushed(str(traceback.format_exc()))
      raise




  def get_tpid(self, job_id, targetsite_url, mpid):
    try:
      targetsite_url = url_normalize(targetsite_url)
      #query =  "select tpid from tpid_mapping where job_id = {} and targetsite_url = '{}' and mpid = {}".format(job_id, targetsite_url, mpid)
      query =  "select tpid from tpid_mapping where targetsite_url = '{}' and mpid = {}".format(targetsite_url, mpid)
      print_flushed(query)
      self.gp_cur.execute(query)
      rows = self.gp_cur.fetchone()
      result = rows[0]
      print_flushed('tpid: ', result)
      return result
    except:
      self.gp_conn.rollback()
      print_flushed(str(traceback.format_exc()))
      raise



  def get_num_threads_in_job_configuration_onetime(self, job_id):
    try:
      query = 'select num_thread from targetsite_job_configuration where job_id = {}'.format(job_id)
      self.gp_cur.execute(query)
      result = self.gp_cur.fetchone()
      self.gp_conn.commit()
      return result[0] 
    except:
      self.gp_conn.rollback()
      print_flushed(str(traceback.format_exc()))
      raise


  def get_num_worker_in_job_configuration(self, job_id):
    try:
      query = 'select num_worker from targetsite_job_configuration where job_id = {}'.format(job_id)
      self.gp_cur.execute(query)
      result = self.gp_cur.fetchone()
      self.gp_conn.commit()
      return result[0] 
    except:
      self.gp_conn.rollback()
      print_flushed(str(traceback.format_exc()))
      raise




  def get_num_threads_in_job_configuration(self, job_id):
    try:
      query = 'select num_thread from job_configuration where job_id = {}'.format(job_id)
      self.gp_cur.execute(query)
      result = self.gp_cur.fetchone()
      self.gp_conn.commit()
      return result[0] 
    except:
      self.gp_conn.rollback()
      print_flushed(str(traceback.format_exc()))
      raise


  def get_num_worker_in_job_configuration(self, job_id):
    try:
      query = 'select num_worker from job_configuration where job_id = {}'.format(job_id)
      self.gp_cur.execute(query)
      result = self.gp_cur.fetchone()
      self.gp_conn.commit()
      return result[0] 
    except:
      self.gp_conn.rollback()
      print_flushed(str(traceback.format_exc()))
      raise






  def update_last_sm_date_in_job_configuration(self, sm_time, job_id):
    try:
      query =  "update job_configuration set last_sm_date = '{}' where job_id = {}".format(sm_time, job_id)
      self.gp_cur.execute(query)
      self.gp_conn.commit()
      return 
    except:
      self.gp_conn.rollback()
      print_flushed(str(traceback.format_exc()))
      raise

  def get_targetsite(self, target_id):
    try:                                                                                         
      query = 'select targetsite_url, targetsite_id from targetsite_job_configuration where id = {}'.format(target_id)  
      self.gp_cur.execute(query)
      res = self.gp_cur.fetchone()
      url = bytes.fromhex(res[0]).decode()
      tid = res[1]
                                    
      query = 'select gateway from targetsite where id = {}'.format(tid)              
      self.gp_cur.execute(query)        
      res = self.gp_cur.fetchone()

      gateway = bytes.fromhex(res[0]).decode()
                                             
      return url, gateway                   
    except:                                
      self.gp_conn.rollback()             
      print_flushed(str(traceback.format_exc()))  
      raise

  def get_site_code_from_job_id(self, job_id):
    try:
     
      query =  "select site_code from job_id_to_site_code where job_id = {}".format(job_id)
      self.gp_cur.execute(query)
      result = self.gp_cur.fetchone()
      site_code = result[0]

      return site_code
    except:
      self.gp_conn.rollback()
      print_flushed(str(traceback.format_exc()))
      raise



  def get_mpid_in_job_source_view_using_status(self, status):
    try:
      query =  "select mpid from job_source_view_backup where status = {}".format(status)
      self.gp_cur.execute(query)
      result = self.gp_cur.fetchall()
      self.gp_conn.commit()
      return result
    except:
      self.gp_conn.rollback()
      print_flushed(str(traceback.format_exc()))
      raise

  def get_shipping_prd_mpid_using_stage_id(self, stage_id):
    try:
      query = "select distinct(value::text) from node_property where key = 'url' and  node_id in (select node_id from node_property where node_id in (select id from node where task_id in (select id from task where stage_id = {})) and key = 'stock' and value::text like '%Ship%');".format(stage_id)
      #query = "select distinct(value::text) from node_property where key = 'url' and  node_id in (select node_id from node_property where node_id in (select id from node where task_id in (select id from task where stage_id = 2948 or stage_id = 2951 or stage_id = 2954 or stage_id = 2957 or stage_id = 2960 or stage_id = 2963 or stage_id = 2966 or stage_id = 2969 or stage_id = 2972 or stage_id = 2975 or stage_id = 2978)) and key = 'stock' and value::text like '%Ship%');"
      #query = "select my_product_id from url_to_mpid where cast(url as varchar(2048)) in (select cast(value::text as varchar(2048)) from node_property where key = 'url' and  node_id in (select node_id from node_property where node_id in (select id from node where task_id in (select id from task where stage_id = {})) and key = 'stock' and value::text like '%Ship%'));".format(stage_id)
      self.gp_cur.execute(query)
      rows = self.gp_cur.fetchall()
      url = []
      for i in rows:
         url.append(i[0].replace('"',"'"))
      
      query = "select my_product_id from url_to_mpid where url in ("
      for val in url:
         query += val+", "
      query = query[:-3] + "');"

      self.gp_cur.execute(query)
      rows = self.gp_cur.fetchall()
      mpids = []
      for i in rows:
         mpids.append(i[0])
      return mpids 
    except:
      self.gp_conn.rollback()
      print_flushed(str(traceback.format_exc()))
      raise
 
  def get_cnum_from_job_configuration(self, job_id):
    try:
      query =  "select cnum from job_configuration where job_id = {}".format(job_id)
      self.gp_cur.execute(query)
      result = self.gp_cur.fetchone()
      return result[0]
    except:
      self.gp_conn.rollback()
      print_flushed(str(traceback.format_exc()))
      raise
 
 
  def get_cnum_from_targetsite_job_configuration_using_tsid(self, tsid):
    try:
      query =  "select cnum from targetsite_job_configuration where id = {}".format(tsid)
      self.gp_cur.execute(query)
      result = self.gp_cur.fetchone()
      return result[0]
    except:
      self.gp_conn.rollback()
      print_flushed(str(traceback.format_exc()))
      raise

 
  def get_targetsite_id_using_job_id(self, job_id):
    try:
      query = 'select id from targetsite_job_configuration where job_id = {}'.format(job_id)  
      self.gp_cur.execute(query)
      results = self.gp_cur.fetchall()
      final_results = []
      for res in results:
        final_results.append(res[0])
      return final_results
    except:
      self.gp_conn.rollback()
      print_flushed(str(traceback.format_exc()))
      raise



 
  def get_job_configuration(self, job_id):
    try:
      query =  "select cnum from job_configuration where job_id = {}".format(job_id)
      self.gp_cur.execute(query)
      result = self.gp_cur.fetchone()
      return result[0]
    except:
      self.gp_conn.rollback()
      print_flushed(str(traceback.format_exc()))
      raise



  def get_selected_gateway_configuration_program_onetime(self, tsid):
    try:
      query =  "select cid from targetsite_job_configuration where id = {}".format(tsid)
      self.gp_cur.execute(query)
      result = self.gp_cur.fetchone()
      cid = result[0]

      query =  "select configuration from gateway_configuration where id = {}".format(cid)
      self.gp_cur.execute(query)
      result = self.gp_cur.fetchone()
      #result = bytes.fromhex(result[0]).decode() 

      return result[0]
    except:
      self.gp_conn.rollback()
      print_flushed(str(traceback.format_exc()))
      raise



  def get_selected_gateway_configuration_program(self, job_id):
    try:
      query =  "select cid  from job_configuration where job_id = {}".format(job_id)
      self.gp_cur.execute(query)
      result = self.gp_cur.fetchone()
      cid = result[0]

      query =  "select configuration from gateway_configuration where id = {}".format(cid)
      self.gp_cur.execute(query)
      result = self.gp_cur.fetchone()
      #result = bytes.fromhex(result[0]).decode() 

      return result[0]
    except:
      self.gp_conn.rollback()
      print_flushed(str(traceback.format_exc()))
      raise



  def get_selected_transformation_program_onetime(self, target_id):
    try:
      query =  "select transformation_program_id  from targetsite_job_configuration where id = {}".format(target_id)
      self.gp_cur.execute(query)
      result = self.gp_cur.fetchone()
      tpid = result[0]

      query =  "select transformation_program from transformation_program where id = {}".format(tpid)
      self.gp_cur.execute(query)
      result = self.gp_cur.fetchone()
      result = bytes.fromhex(result[0]).decode() 

      return result
    except:
      self.gp_conn.rollback()
      print_flushed(str(traceback.format_exc()))
      raise


  def get_shipping_fee(self, delivery_company):
    try:
      delivery_company = delivery_company.encode('UTF-8').hex()
      query = "select id from delivery_companies where name like '{}'".format(delivery_company)
      self.gp_cur.execute(query)
      dcid = self.gp_cur.fetchone()[0]
      
      query =  "select min_kg, max_kg, fee from shipping_fee where delivery_company_id = {}".format(dcid)
      self.gp_cur.execute(query)
      rows = self.gp_cur.fetchall()
      
      delivery_charge_list = [[float(row[0]), float(row[1]), float(row[2])] for row in rows]
      self.gp_conn.commit()

      return delivery_charge_list
    except:
      self.gp_conn.rollback()
      print_flushed(str(traceback.format_exc()))
      raise


  def get_pricing_information_onetime(self, tsid):
    try:
      query =  "select * from targetsite_job_configuration where id = {}".format(tsid)
      self.gp_cur.execute(query)
      row = self.gp_cur.fetchone()
      self.gp_conn.commit()
      result = {}
      result['exchange_rate'] = row[9]
      result['tariff_rate'] = row[10]
      result['vat_rate'] = row[11]
      result['tariff_threshold'] = row[12]
      result['margin_rate'] = row[13]
      result['min_margin'] = row[14]
      result['delivery_company'] = row[15]
      result['default_weight'] = row[18]
      query =  "select exchange_rate from exchange_rate order by id desc limit 1"
      self.gp_cur.execute(query)
      row = self.gp_cur.fetchone()[0]
      result['dollar2krw'] = row['USD']
      return result
    except:
      self.gp_conn.rollback()
      print_flushed(str(traceback.format_exc()))
      raise


  def get_pricing_information(self, job_id):
    try:
      #id | job_id | targetsite_id |      category       |   exchange_rate    | tariff_rate | vat_rate | tariff_threshold | margin_rate | min_margin | delivery_company | shipping_cost
      query =  "select * from pricing_information where job_id = {}".format(job_id)
      self.gp_cur.execute(query)
      row = self.gp_cur.fetchone()
      self.gp_conn.commit()
      result = {}
      result['exchange_rate'] = row[4]
      result['tariff_rate'] = row[5]
      result['vat_rate'] = row[6]
      result['tariff_threshold'] = row[7]
      result['margin_rate'] = row[8]
      result['min_margin'] = row[9]
      result['shipping_cost'] = row[11]
      result['delivery_company'] = row[10]
      query =  "select exchange_rate from exchange_rate order by id desc limit 1"
      self.gp_cur.execute(query)
      row = self.gp_cur.fetchone()[0]
      result['dollar2krw'] = row['USD']
      return result
    except:
      self.gp_conn.rollback()
      print_flushed(str(traceback.format_exc()))
      raise

  def logging_all_uploaded_product(self, job_id, execution_id, mpid, origin_product, converted_product, targetsite_url, cnum, status):
    try:
      targetsite_url = url_normalize(targetsite_url)
      if origin_product.get('Error', '') == '':
        try:
          origin_product['sm_date'] = origin_product['sm_date'].strftime('%Y-%m-%d %H:%M:%S')
          origin_product['option_name'] = repr(origin_product['option_name'])
          origin_product['option_value'] = repr(origin_product['option_value'])
        except:
          origin_product['sm_date'] = ''
          pass
        #origin_product['html'] =  origin_product['html'].encode('UTF-8').hex()
        #if is_hex_str(origin_product['price']) == True:
        #  origin_product['price'] = origin_product['price'].encode('UTF-8').hex()
        #converted_product['description'] = converted_product['description'].encode('UTF-8').hex()
      #for key in sorted(origin_product.keys()):
      origin_product = json.dumps(origin_product).encode('UTF-8').hex()
      converted_product = json.dumps(converted_product).encode('UTF-8').hex()
      query =  "insert into all_uploaded_product(job_id, execution_id, mpid, origin_product, converted_product, targetsite_url, cnum, status) values({}, {}, {}, '{}', '{}','{}',{}, {})".format(job_id, execution_id, mpid,  json.dumps(origin_product), json.dumps(converted_product), targetsite_url, cnum, status)
      self.gp_cur.execute(query)
      self.gp_conn.commit()
      return 
    except:
      self.gp_conn.rollback()
      print_flushed('@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@')
      print_flushed(origin_product)
      print_flushed('@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@')
      print_flushed(converted_product)
      print_flushed('@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@')
      print_flushed(str(traceback.format_exc()))
      raise




  def create_row_job_current_working(self, job_id):
    try:
      query =  "insert into job_current_working(job_id) values({})".format(job_id)
      self.gp_cur.execute(query)
      self.gp_conn.commit()
      return 
    except:
      self.gp_conn.rollback()
      print_flushed(str(traceback.format_exc()))
      raise


  def log_err_msg_of_task(self, task_id, err_msg):
    try:
      err_msg = err_msg.replace("'",'"')
      query = "insert into failed_task_detail(task_id, err_msg) values({},'{}')".format(task_id,err_msg)
      self.gp_cur.execute(query)
      self.gp_conn.commit()
      return 
    except:
      self.gp_conn.rollback()
      print_flushed(str(traceback.format_exc()))
      raise

  #failed_target_site_detail (id integer primary key generated always as identity, sm_history_id integer, mpid bigint, err_msg text);
  def log_err_msg_of_upload(self, mpid, err_msg, mt_history_id):
    try:
      err_msg = err_msg.replace("'",'"')
      if mpid == -1:
        
        query = "select count(*) from failed_target_site_detail where mpid = {} and mt_history_id = {}".format(mpid, mt_history_id)
        self.gp_cur.execute(query)
        self.gp_cur.execute(query)
        result = self.gp_cur.fetchone()[0]
        if int(result) == 0:
          query = "insert into failed_target_site_detail(mpid, err_msg, mt_history_id) values({},'{}',{})".format(mpid,err_msg, mt_history_id)
        else:
          query = "update failed_target_site_detail set err_msg = err_msg || '{}' where mpid = {} and mt_history_id = {}".format(err_msg, mpid, mt_history_id) 
       
        self.gp_cur.execute(query)
        self.gp_conn.commit()
      else:
        query = "insert into failed_target_site_detail(mpid, err_msg, mt_history_id) values({},'{}',{})".format(mpid,err_msg, mt_history_id)
        print_flushed(query)
        self.gp_cur.execute(query)
        self.gp_conn.commit()
      return 
    except:
      self.gp_conn.rollback()
      print_flushed(traceback.format_exc())
      raise




  def log_to_job_current_targetsite_working(self, log, job_id):
    try:
      query = "update job_current_working set targetsite_working = targetsite_working || '{}' where job_id = {}".format(log, job_id)
      self.gp_cur.execute(query)
      self.gp_conn.commit()
      return 
    except:
      self.gp_conn.rollback()
      print_flushed(str(traceback.format_exc()))
      raise



  def log_to_job_current_mysite_working(self, log, job_id):
    try:
      #query =  "update job_current_working set mysite_working = '{}' where job_id = {}".format(log, job_id)
      query = "update job_current_working set mysite_working = mysite_working || '{}' where job_id = {}".format(log, job_id)
      self.gp_cur.execute(query)
      self.gp_conn.commit()
      return 
    except:
      self.gp_conn.rollback()
      print_flushed(str(traceback.format_exc()))
      raise


  def re_log_to_job_current_mysite_working(self, log, job_id):
    try:
      query =  "update job_current_working set mysite_working = '{}' where job_id = {}".format(log, job_id)
      self.gp_cur.execute(query)
      self.gp_conn.commit()
      return 
    except:
      self.gp_conn.rollback()
      print_flushed(str(traceback.format_exc()))
      raise



  def re_log_to_job_current_crawling_working(self, log, job_id):
    try:
      query =  "update job_current_working set crawling_working = '{}' where job_id = {}".format(log, job_id)
      self.gp_cur.execute(query)
      self.gp_conn.commit()
      return 
    except:
      self.gp_conn.rollback()
      print_flushed(str(traceback.format_exc()))
      raise



  def log_to_job_current_crawling_working(self, log, job_id):
    try:
      query = "update job_current_working set crawling_working = crawling_working || '{}' where job_id = {}".format(log, job_id)
      self.gp_cur.execute(query)
      self.gp_conn.commit()
      return 
    except:
      self.gp_conn.rollback()
      print_flushed(str(traceback.format_exc()))
      raise




  def check_exist_in_job_source_view(self, mpid):
    try:
      query =  "select count(*) from job_source_view where mpid = {} and status != 3 and status != 4".format(mpid)
      self.gp_cur.execute(query)
      rows = self.gp_cur.fetchone()
      self.gp_conn.commit()
      result = rows[0]
      return result
    except:
      self.gp_conn.rollback()
      print_flushed(str(traceback.format_exc()))
      raise


  def update_last_mt_date_in_job_configuration(self, mt_time, job_id, tsid):
    try:
      query =  "update job_configuration set last_mt_date = '{}' where job_id = {} and tsid = {}".format(mt_time, job_id, tsid)
      self.gp_cur.execute(query)
      self.gp_conn.commit()
      return
    except:
      self.gp_conn.rollback()
      print_flushed(str(traceback.format_exc()))
      raise


  def check_string_is_int(self, s):
    try: 
        int(s)
        return True
    except ValueError:
        return False



  # for jomashop
  def check_stock_for_jomashop(self, input_dictionary):
    try:
      stock = '0'
      if 'stock' in input_dictionary:
        if input_dictionary['stock'] is None:
          stock = '0'
        else:
          if self.check_string_is_int(input_dictionary['stock']):
            stock = str(input_dictionary['stock'])
          else:
            if 'in stock' in str(input_dictionary['stock']).lower():
              stock = '999'
            else:
              stock = '0'

      return stock
    except:
      print_flushed(str(traceback.format_exc()))
      raise

  def json_default(self, value):
    if isinstance(value, date): 
       return str(value.strftime('%Y-%m-%d %H:%M:%S'))
    elif isinstance(value, set):
       return ''
    print_flushed(value.replace("'","\'"))
    return value.replace("'","\'")
    raise TypeError('not JSON serializable')

  def get_client(self, mall_id, job_id):
    try:
      query = "BEGIN;  Lock table cafe24_client_id in EXCLUSIVE MODE; update cafe24_client_id set use_now = 1, get_time = now(), job_id = {} where id in (select min(id) from cafe24_client_id where use_now = -1 and mall_id = '{}') returning client_id, client_secret;".format(job_id, mall_id)
      self.gp_cur.execute(query)
      result = self.gp_cur.fetchone()
      self.gp_conn.commit()
      print_flushed(result)
      #query =  "update cafe24_client_id set use_now = 1 where id in (select min(id) from cafe24_client_id where use_now = -1 and mall_id = '{}') returning client_id, client_secret".format(mall_id)

      query = 'COMMIT;'
      self.gp_cur.execute(query)
      self.gp_conn.commit()
      #result = self.gp_cur.fetchone()
      return result
    except Exception as e:
      print_flushed(str(traceback.format_exc()))
      return None
      #self.pg_conn.rollback()
      #print_flushed(str(traceback.format_exc()))
      #raise e

  def return_client(self, cid, cs):
    try:
      query =  "update cafe24_client_id set use_now = -1,get_time = Null, job_id = Null where client_id = '{}' and client_secret = '{}';".format(cid, cs)

      self.gp_cur.execute(query)
      self.gp_conn.commit()
      return 
    except Exception as e:
      self.gp_conn.rollback()
      print_flushed(str(traceback.format_exc()))
      raise e


  def get_zipcode(self, src_url, zipcode):
    try:
      query =  "select count(zipcode) from url_and_zipcode where url = '{}';".format(src_url)
      self.gp_cur.execute(query)
      result = self.gp_cur.fetchone()[0]
      if int(result) == 0:
        query = "insert into url_and_zipcode(url,zipcode) values('{}','{}')".format(src_url, zipcode) 
        self.gp_cur.execute(query)
        self.gp_conn.commit() 
        return zipcode
      else:
        query =  "select zipcode from url_and_zipcode where url = '{}';".format(src_url)
        self.gp_cur.execute(query)
        result = self.gp_cur.fetchone()[0]
        if str(result) != str(zipcode):
          query = "update url_and_zipcode set zipcode = '{}' where url = '{}'".format(zipcode, src_url)
          self.gp_cur.execute(query)
          self.gp_conn.commit()
          return zipcode
        else:
          return result
    except Exception as e:
      self.gp_conn.rollback()
      print_flushed(str(traceback.format_exc()))
      raise e

  def update_zipcode(self, url, zipcode):
    try:
      src_url = urlparse(url).netloc 
      query =  "select count(zipcode) from url_and_zipcode where url = '{}';".format(url)
      self.gp_cur.execute(query)
      result = self.gp_cur.fetchone()[0]
      if int(result) == 0:
         query = "insert into url_and_zipcode(url,zipcode) values('{}','{}')".format(src_url, zipcode)
      else:
         query = "update url_and_zipcode set zipcode = '{}' where url = '{}'".format(zipcode, src_url)
      self.gp_cur.execute(query)
      self.gp_conn.commit()
      return
    except Exception as e:
      self.gp_conn.rollback()
      print_flushed(str(traceback.format_exc()))
      raise e




  def check_stock(self, input_dictionary):
    try:
      stock = '0'
      if 'stock' in input_dictionary:
        if input_dictionary['stock'] is None:
          if 'out_of_stock' in input_dictionary:
            # stock is None & out of stock is None => 999
            if input_dictionary['out_of_stock'] is None:
              stock = '999'
            # stock is None & out of stock is not None => 0
            else:
              stock = '0'
          # stock is None & out of stock does not crawled => 999
          else:
            stock = '999'
        else:
          # stock is not None & stock is integer => use that value
          if self.check_string_is_int(input_dictionary['stock']):
            stock = str(input_dictionary['stock'])
          # stock is not None & stock is 'in stock' => 999
          else:
            if 'in stock' in str(input_dictionary['stock']).lower():
              stock = '999'
            # stock is not None & stock is other string => 0
            else:
              stock = '0'
      else:
        if 'out_of_stock' in input_dictionary:
          # stock does not crawled & out of stock is None => 999
          if input_dictionary['out_of_stock'] is None:
            stock = '999'
          # stock does not crawled & out of stock is not None => 0
          else:
            stock = '0'
        # stock does not crawled & out of stock does not crawled => 999
        else:
          stock = '999'
      return stock
    except:
      print_flushed(str(traceback.format_exc()))
      raise

