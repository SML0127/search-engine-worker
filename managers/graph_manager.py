import psycopg2
import traceback
import json
import hashlib
import urllib.request
import cfscrape
import pathlib
from io import BytesIO
from PIL import Image
import requests
from price_parser import Price
from datetime import datetime, timedelta, date
from urllib.parse import urlparse

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
      print(str(traceback.format_exc()))
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
      print(str(traceback.format_exc()))
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
      print(str(traceback.format_exc()))
      raise e
    try:
      query = 'create table node_property(id bigserial primary key, node_id bigint, key varchar(1048), value json);'
      self.gp_cur.execute(query)
      self.gp_conn.commit()
    except Exception as e:
      self.gp_conn.rollback()
      print(str(traceback.format_exc()))
      raise e

  def drop_db(self):
    try:
      query = 'drop table if exists node;'
      self.pg_cur.execute(query)
      self.pg_conn.commit()
    except Exception as e:
      self.pg_conn.rollback()
      print(str(traceback.format_exc()))
      raise e
    try:
      query = 'drop table if exists node_property;'
      self.gp_cur.execute(query)
      self.gp_conn.commit()
    except Exception as e:
      self.gp_conn.rollback()
      print(str(traceback.format_exc()))
      raise e

  def create_node(self, task_id, parent_id, label):
    try:
      query = 'insert into node(task_id, parent_id, label) '
      query += 'values(%s, %s, %s)'
      query += 'returning id;'
      self.pg_cur.execute(query, (str(task_id), str(parent_id), str(label)))
      result = self.pg_cur.fetchone()[0]
      self.pg_conn.commit()
      return result
    except Exception as e:
      self.pg_conn.rollback()
      print(str(traceback.format_exc()))
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
      print(str(traceback.format_exc()))
      raise e

  
  def update_mysite(self, job_id):
      try:
          #mpid, status, c_date, sm_date,  url, p_name, sku, list_price, price, origin, company, image_url, num_options, num_images, m_category
          query = "create view job"+job_id+"_update_view as (select v1.price as prev_price, v2.price new_price, v1.url as v1_url, v2.url as v2_url, v1.stock as prev_stock, v2.stock as new_stock, v1.num_options as prev_num_options, v2.num_options as new_num_options, v1.job_id as job_id1, v2.job_id as job_id2, v1.mpid as mpid, v2.mpid as prd2, v1.p_name_sha256 as prev_name, v2.p_name_sha256 as new_name from job_source_view as v1 full outer join job"+job_id+"_source_view_latest as v2 on v1.url_sha256 = v2.url_sha256)"
          self.pg_cur.execute(query)
          self.pg_conn.commit()
          query = "select * from job"+job_id+"_update_view"
          self.pg_cur.execute(query)
          result = self.pg_cur.fetchall()
          self.pg_conn.commit()
          time_gap = timedelta(hours=9)
          # 0 = up to date 1 = changed 2 = New 3 = Deleted
          
          # temp
          targetsite_url = 'http://mallmalljmjm.cafe24.com'
          up_to_date = 0
          changed = 0
          new_item = 0
          deleted = 0
          prd_in_other_job = 0
          new_but_out_of_stock = 0
          print("# of product to compare: ", len(result))
          max_update_chunk = 30
          update_chunk = 0
          for product in result:
             # New
             prd={}
             prd['prev_price'] = product[0] if product[0] is not None else 0
             prd['new_price'] = product[1] if product[1] is not None else 0
             prd['v1_url'] = product[2] if product[2] is not None else ''
             prd['v2_url'] = product[3] if product[3] is not None else ''
             prd['prev_stock'] = product[4] if product[4] is not None else '0'
             prd['new_stock'] = product[5] if product[5] is not None else '0'
             prd['prev_num_options'] = product[6] if product[6] is not None else 0
             prd['new_num_options'] = product[7] if product[7] is not None else 0
             prd['job_id1'] = product[8] if product[8] is not None else -1
             prd['job_id2'] = product[9] if product[9] is not None else -1
             mpid = product[10]
             new_prd_mpid = product[11]
             prd['prev_name'] = product[12] if product[12] is not None else ''
             prd['new_name'] = product[13] if product[13] is not None else ''
             is_changed = False        
             is_new = False      
             #tpid = self.get_tpid(job_id, targetsite_url, mpid) 
             # temp
             tpid = 1 
             # different job_id, job_id1 == -1 job_id2 != -1
             if prd.get('job_id1') != prd.get('job_id2') and prd.get('job_id1') == -1:
                # Deleted
                if prd.get('new_stock') == '0':
                  new_but_out_of_stock += 1
                else:
                  new_item += 1
                  is_new = True
                  query = "insert into job_source_view (job_id, mpid, status, c_date, sm_date, url, url_sha256, p_name, p_name_sha256, sku, list_price, price, origin, company, image_url, num_options, num_images, m_category, groupby_key_sha256, spid, stock) (select job_id, mpid, 2, c_date, sm_date, url, url_sha256, p_name, p_name_sha256, sku, list_price, price, origin, company, image_url, num_options, num_images, m_category, groupby_key_sha256, spid, stock from job"+job_id+"_source_view_latest where url = '{}');".format(prd['v2_url']) 
                  #query = "insert into job_source_view select * from job"+job_id+"_source_view_latest where url = '{}'".format(prd['v2_url']) 
                  self.pg_cur.execute(query)
                  self.pg_conn.commit()              
           
             # different job_id, job_id2 == -1 -> job_id1 is same job -> deleted
             elif prd.get('job_id1') != prd.get('job_id2') and prd.get('job_id2') == -1 and int(prd.get('job_id1')) == int(job_id):
                
                query = "select status from job_source_view where mpid = {} and job_id = {}".format(mpid, job_id) 
                self.pg_cur.execute(query)
                status = self.pg_cur.fetchone()[0]
                if status == 3:
                   up_to_date += 1
                else:
                   deleted += 1
                   query = "update job_source_view set status = 3, stock = 0 where url = '{}'".format(prd['v1_url']) 
                   self.pg_cur.execute(query)
                   self.pg_conn.commit()
                
             # different job_id, job_id2 == -1 -> job_id1 is other job
             elif prd.get('job_id1') != prd.get('job_id2') and prd.get('job_id2') == -1  and int(prd.get('job_id1')) != int(job_id) :
                prd_in_other_job += 1

             elif (prd.get('job_id1') == prd.get('job_id2') and prd.get('job_id1') != -1):
                # Deleted
                if prd.get('v1_url') != '' and  prd.get('v2_url') == '':
                   query = "select status from job_source_view where mpid = {} and job_id = {}".format(mpid, job_id) 
                   self.pg_cur.execute(query)
                   status = self.pg_cur.fetchone()[0]
                   if status == 3:
                      up_to_date += 1
                   else:
                      deleted += 1
                      query = "update job_source_view set status = 3, stock = 0 where url = '{}'".format(prd['v1_url']) 
                      self.pg_cur.execute(query)
                      self.pg_conn.commit()
                # Deleted
                #elif prd.get('prev_stock') != '0' and  prd.get('new_stock') == '0':
                elif prd.get('new_stock') == '0':
                   query = "select status from job_source_view where mpid = {} and job_id = {}".format(mpid, job_id) 
                   self.pg_cur.execute(query)
                   status = self.pg_cur.fetchone()[0]
                   if status == 3:
                      up_to_date += 1
                   else:
                     deleted += 1
                     query = "update job_source_view set status = 3, stock = 0 where url = '{}'".format(prd['v1_url']) 
                     self.pg_cur.execute(query)
                     self.pg_conn.commit()               
                elif prd.get('prev_name') != prd.get('new_name'):
                   is_changed = True
                # Changed: num option 0 -> larger than 1 
                elif prd.get('prev_num_options', 0) == 0 and  prd.get('new_num_options', 0) >= 1:
                   query = "select option_name, option_value, price, stock from job_option_source_view where job_id = {} and mpid = {}".format(job_id, mpid)
                   self.pg_cur.execute(query)
                   res1 = self.pg_cur.fetchall()
                   res1_list = []
                   for idx, val in enumerate(res1):
                      res1_list.append((val[0], val[1], val[2], val[3]))

                   query = "select option_name, option_value, price, stock from job"+job_id+"_option_source_view_latest where mpid = {}".format(mpid)
                   self.pg_cur.execute(query)
                   res2 = self.pg_cur.fetchall()
                   res2_list = []
                   for idx, val in enumerate(res2):
                      res2_list.append((val[0], val[1], val[2], val[3]))
                   is_changed = True
                # Changed: num option larger than 1 -> 0
                elif prd.get('prev_num_options', 0) >= 1 and  prd.get('new_num_options', 0) == 0:
                   is_changed = True
                # Changed: both of num option larger than 1 but diff
                elif prd.get('prev_num_options', 0) >=1 and prd.get('new_num_options', 0) >=1 and (prd.get('prev_num_options', 0) != prd.get('new_num_options', 0) ):
                   is_changed = True
                # both of num option larger than 1 and same -> check each option
                # mpid integer, option_name varchar(2048), option_value varchar(2048), list_price varchar(64), price varchar(64), stock integer, stock_status integer, msg varchar(2048)
                elif prd.get('prev_num_options', 0) >=1 and prd.get('new_num_options', 0) >=1 and (prd.get('prev_num_options', 0) == prd.get('new_num_options', 0)):
                   query = "select option_name, option_value, price, stock from job_option_source_view where job_id = {} and mpid = {}".format(job_id, mpid)
                   self.pg_cur.execute(query)
                   res1 = self.pg_cur.fetchall()
                   res1_list = []
                   for idx, val in enumerate(res1):
                      res1_list.append((val[0], val[1], val[2], val[3]))

                   query = "select option_name, option_value, price, stock from job"+job_id+"_option_source_view_latest where mpid = {}".format(mpid)
                   self.pg_cur.execute(query)
                   res2 = self.pg_cur.fetchall()
                   res2_list = []
                   for idx, val in enumerate(res2):
                      res2_list.append((val[0], val[1], val[2], val[3]))

                   is_same = True
                     
                   if is_same == False:
                      is_changed = True
                   else:
                      query = "select image_url_sha256 from job_thumbnail_source_view where mpid = {} and job_id = {}".format(mpid, job_id)
                      self.pg_cur.execute(query)
                      res1 = self.pg_cur.fetchall()
                      res1_list = []
                      for idx, val in enumerate(res1):
                         res1_list.append(val[0])

                      query = "select image_url_sha256 from job"+job_id+"_thumbnail_source_view_latest where mpid = {}".format(mpid)
                      self.pg_cur.execute(query)
                      res2 = self.pg_cur.fetchall()
                      res2_list = []
                      for idx, val in enumerate(res2):
                         res2_list.append(val[0])

                      is_same_img_list = True
                      if sorted(res1_list) != sorted(res2_list):
                         is_same_img_list = False

                      if is_same_img_list == False:
                         is_changed = True
                      elif is_same_img_list == True:
                         query = "select value from job_description_source_view where mpid = {} and key = '{}' and job_id = {}".format(mpid, 'description_sha256', job_id)
                         self.pg_cur.execute(query)
                         res = self.pg_cur.fetchone()
                         res1_desc = res[0]

                         query = "select value from job"+job_id+"_description_source_view_latest where mpid = {} and key = '{}'".format(mpid, 'description_sha256')
                         self.pg_cur.execute(query)
                         res = self.pg_cur.fetchone()
                         res2_desc = res[0]


                         if res1_desc != res2_desc:
                            is_changed = True
                         # New
                         elif (prd.get('prev_price') == 0 and prd.get('new_price') != 0):
                            new_item += 1
                            is_new = True
                            query = "insert into job_source_view (job_id, mpid, status, c_date, sm_date, url, url_sha256, p_name, p_name_sha256, sku, list_price, price, origin, company, image_url, num_options, num_images, m_category, groupby_key_sha256, spid, stock) (select job_id, mpid, 2, c_date, sm_date, url, url_sha256, p_name, p_name_sha256, sku, list_price, price, origin, company, image_url, num_options, num_images, m_category, groupby_key_sha256, spid, stock from job"+job_id+"_source_view_latest where url = '{}');".format(prd['v2_url']) 
                            self.pg_cur.execute(query)
                            self.pg_conn.commit()
                            query = "update job_source_view set status = 2 where url = '{}'".format(prd['v2_url']) 
                            self.pg_cur.execute(query)
                            self.pg_conn.commit()
                         # Deleted
                         elif (prd.get('prev_price') != 0 and prd.get('new_price') == 0):
                            query = "select status from job_source_view where mpid = {} and job_id = {}".format(mpid, job_id) 
                            self.pg_cur.execute(query)
                            status = self.pg_cur.fetchone()[0]
                            if status == 3:
                               up_to_date += 1
                            else:
                               deleted += 1
                               query = "update job_source_view set status = 3, stock = 0 where url = '{}'".format(prd['v2_url']) 
                               self.pg_cur.execute(query)
                               self.pg_conn.commit()
                         # Updated
                         elif (prd.get('prev_price') != 0 and prd.get('new_price') != 0 and prd.get('prev_price') != prd.get('new_price')):
                            is_changed = True
                         # Same price
                         elif (prd.get('prev_price') != 0 and prd.get('new_price') != 0 and prd.get('prev_price') == prd.get('new_price')):
                            # stock changed 
                            if (prd.get('new_stock') != prd.get('prev_stock')):
                               is_changed = True
                            # up to date
                            else:
                               up_to_date += 1
                               query = "update job_source_view set status = 0 where url = '{}'".format(prd['v2_url']) 
                               self.pg_cur.execute(query)
                               self.pg_conn.commit()
                  
                # both of num option 0
                elif prd.get('prev_num_options', 0) == 0 and prd.get('new_num_options', 0) == 0:
                   query = "select image_url_sha256 from job_thumbnail_source_view where mpid = {} and job_id = {}".format(mpid, job_id)
                   self.pg_cur.execute(query)
                   res1 = self.pg_cur.fetchall()
                   res1_list = []
                   for idx, val in enumerate(res1):
                      res1_list.append(val[0])

                   query = "select image_url_sha256 from job"+job_id+"_thumbnail_source_view_latest where mpid = {}".format(mpid)
                   self.pg_cur.execute(query)
                   res2 = self.pg_cur.fetchall()
                   res2_list = []
                   for idx, val in enumerate(res2):
                      res2_list.append(val[0])

                   is_same_img_list = True
                   if sorted(res1_list) != sorted(res2_list):
                      is_same_img_list = False

                   if is_same_img_list == False:
                      is_changed = True
                   else:
                      query = "select value from job_description_source_view where mpid = {} and key = '{}' and job_id = {}".format(mpid, 'description_sha256', job_id)
                      self.pg_cur.execute(query)
                      res = self.pg_cur.fetchone()
                      res1_desc = res[0]

                      query = "select value from job"+job_id+"_description_source_view_latest where mpid = {} and key = '{}'".format(mpid, 'description_sha256')
                      self.pg_cur.execute(query)
                      res = self.pg_cur.fetchone()
                      res2_desc = res[0]

                      if res1_desc != res2_desc:
                         query = "select value from job_description_source_view where mpid = {} and key = '{}' and job_id = {}".format(mpid, 'description', job_id)
                         self.pg_cur.execute(query)
                         res = self.pg_cur.fetchone()
                         res1_desc = res[0]

                         query = "select value from job"+job_id+"_description_source_view_latest where mpid = {} and key = '{}'".format(mpid, 'description')
                         self.pg_cur.execute(query)
                         res = self.pg_cur.fetchone()
                         res2_desc = res[0]

                         is_changed = True
                      # New
                      elif (prd.get('prev_price') == 0 and prd.get('new_price') != 0):
                         new_item += 1
                         is_new = True
                         query = "insert into job_source_view (job_id, mpid, status, c_date, sm_date, url, url_sha256, p_name, p_name_sha256, sku, list_price, price, origin, company, image_url, num_options, num_images, m_category, groupby_key_sha256, spid, stock) (select job_id, mpid, 2, c_date, sm_date, url, url_sha256, p_name, p_name_sha256, sku, list_price, price, origin, company, image_url, num_options, num_images, m_category, groupby_key_sha256, spid, stock from job"+job_id+"_source_view_latest where url = '{}');".format(prd['v2_url']) 
                         self.pg_cur.execute(query)
                         self.pg_conn.commit()
                         query = "update job_source_view_latest set status = 2 where url = '{}'".format(prd['v2_url']) 
                         self.pg_cur.execute(query)
                         self.pg_conn.commit()
                      # Deleted
                      elif (prd.get('prev_price') != 0 and prd.get('new_price') == 0):
                         query = "select status from job_source_view where mpid = {} and job_id = {}".format(mpid, job_id) 
                         self.pg_cur.execute(query)
                         status = self.pg_cur.fetchone()[0]
                         if status == 3:
                            up_to_date += 1
                         else:
                            deleted += 1
                            query = "update job_source_view set status = 3, stock = 0 where url = '{}'".format(prd['v2_url']) 
                            self.pg_cur.execute(query)
                            self.pg_conn.commit()
                      # Updated
                      elif (prd.get('prev_price') != 0 and prd.get('new_price') != 0 and prd.get('prev_price') != prd.get('new_price')):
                         is_changed = True
                      # Same price
                      elif (prd.get('prev_price') != 0 and prd.get('new_price') != 0 and prd.get('prev_price') == prd.get('new_price')):
                         # stock changed 
                         if (prd.get('new_stock') != prd.get('prev_stock')):
                            is_changed = True
                         # up to date
                         else:
                            up_to_date += 1
                            query = "update job_source_view set status = 0 where url = '{}'".format(prd['v2_url']) 
                            self.pg_cur.execute(query)
                            self.pg_conn.commit()

             if is_changed == True:
                changed += 1
                query = 'delete from job_source_view where mpid = {}'.format(mpid)
                self.pg_cur.execute(query)
                self.pg_conn.commit()
                #query = "insert into job_source_view( job_id, mpid, status, c_date, sm_date, url, url_sha256, p_name, p_name_sha256, sku, list_price, price, stock,origin, company, image_url, num_options, num_images, m_category, tpid, groupby_key_sha256, spid) (select  job_id, mpid, 1, c_date, sm_date, url, url_sha256, p_name, p_name_sha256, sku, list_price, price, stock, origin, company, image_url, num_options, num_images, m_category, '"+tpid+"', groupby_key_sha256, spid from job"+job_id+"_source_view_latest where mpid = {});".format(mpid)
                query = "insert into job_source_view( job_id, mpid, status, c_date, sm_date, url, url_sha256, p_name, p_name_sha256, sku, list_price, price, stock,origin, company, image_url, num_options, num_images, m_category, groupby_key_sha256, spid) (select  job_id, mpid, 1, c_date, sm_date, url, url_sha256, p_name, p_name_sha256, sku, list_price, price, stock, origin, company, image_url, num_options, num_images, m_category, groupby_key_sha256, spid from job"+job_id+"_source_view_latest where mpid = {});".format(mpid)
                self.pg_cur.execute(query)
                self.pg_conn.commit()
                 
                #self.insert_tpid_into_history_table(job_id, targetsite_url, mpid, tpid)
                #self.update_tpid_into_mapping_table(job_id, targetsite_url, mpid, tpid)

                query = 'delete from job_description_source_view where job_id = {} and mpid = {}'.format(job_id, mpid)
                self.pg_cur.execute(query)
                self.pg_conn.commit()
                query = "insert into job_description_source_view select * from job"+job_id+"_description_source_view_latest where mpid = {};".format(mpid)
                self.pg_cur.execute(query)
                self.pg_conn.commit()

                query = 'delete from job_option_source_view where job_id = {} and mpid = {}'.format(job_id, mpid)
                self.pg_cur.execute(query)
                self.pg_conn.commit()
                query = "insert into job_option_source_view select  * from job"+job_id+"_option_source_view_latest where mpid = {};".format(mpid)
                self.pg_cur.execute(query)
                self.pg_conn.commit()
               
                query = 'select * from job_thumbnail_source_view where job_id = {} and mpid = {}'.format(job_id, mpid)
                self.pg_cur.execute(query)
                
                query = 'delete from job_thumbnail_source_view where job_id = {} and mpid = {}'.format(job_id, mpid)
                self.pg_cur.execute(query)
                self.pg_conn.commit()
                query = "insert into job_thumbnail_source_view select * from job"+job_id+"_thumbnail_source_view_latest where mpid = {};".format(mpid)
                self.pg_cur.execute(query)
                self.pg_conn.commit()
                query = 'select * from job_thumbnail_source_view where job_id = {} and mpid = {}'.format(job_id, mpid)
                self.pg_cur.execute(query)
             elif is_new == True:
                query = "insert into job_description_source_view select * from job"+job_id+"_description_source_view_latest where mpid = {};".format(new_prd_mpid)
                self.pg_cur.execute(query)
                self.pg_conn.commit()

                query = "insert into job_option_source_view select  * from job"+job_id+"_option_source_view_latest where mpid = {};".format(new_prd_mpid)
                self.pg_cur.execute(query)
                self.pg_conn.commit()

                query = "insert into job_thumbnail_source_view select * from job"+job_id+"_thumbnail_source_view_latest where mpid = {};".format(new_prd_mpid)
                self.pg_cur.execute(query)
                self.pg_conn.commit()                        
             update_chunk += 1
             if update_chunk == max_update_chunk:
                cur_time = datetime.utcnow() + time_gap 
                cur_time = cur_time.strftime('%Y-%m-%d %H:%M:%S')
                self.log_to_job_current_mysite_working('{}\n[Running] \nUp-to-date: {} items\nChanged: {} items\n New: {} items\n Deleted: {} items\n'.format(cur_time, up_to_date, changed, new_item, deleted), job_id) 
                update_chunk = 0
             
          cur_time = datetime.utcnow() + time_gap 
          cur_time = cur_time.strftime('%Y-%m-%d %H:%M:%S')
          self.log_to_job_current_mysite_working('{}\n[Finished] \nUp-to-date: {} items\nChanged: {} items\n New: {} items\n Deleted: {} items\n'.format(cur_time, up_to_date, changed, new_item, deleted), job_id)
           

          print("# of up to date: ", up_to_date)
          #############
          print("# of changed: ", changed)
          #############
          print("# of new item: ", new_item)
          ############3
          print("# of deleted item: ", deleted)
          print("# of new but out of stock item: ", new_but_out_of_stock)
          print("# of item in other job (deleted by unique constraint): ", prd_in_other_job)
          query = "insert into job_update_statistics_history(job_id, up_to_date, changed, new_item, deleted) values({}, {}, {}, {}, {})".format(job_id, up_to_date, changed, new_item, deleted)
          self.pg_cur.execute(query)
          self.pg_conn.commit()

          query = "drop view if exists job"+job_id+"_update_view"
          self.pg_cur.execute(query)
          query = "drop table if exists job"+job_id+"_source_view"
          self.pg_cur.execute(query)


          query = "drop table if exists job"+job_id+"_source_view_latest"
          self.pg_cur.execute(query)
          query = "drop table if exists job"+job_id+"_option_source_view_latest"
          self.pg_cur.execute(query)
          query = "drop table if exists job"+job_id+"_description_source_view_latest"
          self.pg_cur.execute(query)
          query = "drop table if exists job"+job_id+"_thumbnail_source_view_latest"
          self.pg_cur.execute(query)
          self.pg_conn.commit()
          return { "success": True}
      except:
          self.pg_conn.rollback()
          print(traceback.format_exc())
          return { "success": False, "traceback": str(traceback.format_exc()) }       


  def find_nodes_of_execution_with_label(self, exec_id, label):
    try:
      query =  'select n.id'
      query += ' from node n, stage s, task t'
      query += ' where n.task_id = t.id and t.stage_id = s.id and s.execution_id = %s and n.label = %s order by n.id asc'
      print(query % (str(exec_id), str(label)))
      self.pg_cur.execute(query, (str(exec_id), str(label)))
      result = self.pg_cur.fetchall()
      self.pg_conn.commit()
      return list(map(lambda x: x[0], result))
    except Exception as e:
      self.pg_conn.rollback()
      print(str(traceback.format_exc()))
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
      print(str(traceback.format_exc()))
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
      print(str(traceback.format_exc()))
      raise e


  def insert_node_property(self, nodeId, key, value):
    try:
      query =  'INSERT INTO node_property (node_id, key, value) '
      query += 'VALUES (%s, %s, %s)'
      value = json.dumps(value)
      self.gp_cur.execute(query, (str(nodeId), str(key), str(value)))
      self.gp_conn.commit()
    except:
      self.gp_conn.rollback()
      print(str(traceback.format_exc()))
      raise


  def insert_node_property_to_tmp_mysite(self, job_id, value, groupbykeys):
    try:
      # create tmp source_view
      job_id = str(job_id)
      query = 'create table if not exists job'+str(job_id)+'_source_view_latest (job_id integer, id integer primary key generated always as identity, mpid integer, status integer, c_date timestamp, sm_date timestamp, url varchar(2048), url_sha256  varchar(64), p_name varchar(2048), p_name_sha256 varchar(64), sku varchar(2048), list_price  varchar(64), price  varchar(64), stock varchar(64), origin varchar(2048), company varchar(2048), image_url varchar(2048), num_options integer, num_images integer, m_category varchar(2048), tpid varchar(128), spid varchar(128), groupby_key_sha256 varchar(64));'
      self.pg_cur.execute(query)
      self.pg_conn.commit()


      # create tmp description_source_view
      query = 'create table if not exists job'+str(job_id)+'_description_source_view_latest (job_id integer, mpid integer, key varchar(2048), value text)'
      self.pg_cur.execute(query)
      self.pg_conn.commit()


      # create tmp option_source_view
      query = 'create table if not exists job'+str(job_id)+'_option_source_view_latest (job_id integer, mpid integer, option_name varchar(2048), option_value varchar(2048), list_price varchar(64), price varchar(64), stock integer, stock_status integer, msg varchar(2048))'
      self.pg_cur.execute(query)
      self.pg_conn.commit()


      # create tmp thumbnail_source_view
      query = 'create table if not exists job'+str(job_id)+'_thumbnail_source_view_latest (job_id integer, mpid integer, image_url varchar(2048), image_url_sha256 varchar(64))'
      self.pg_cur.execute(query)
      self.pg_conn.commit()


      # insert data to tmp source_view
      query =  'INSERT INTO job'+str(job_id)+'_source_view_latest (job_id, mpid, status, c_date, sm_date, url, url_sha256, p_name, p_name_sha256, sku, list_price, price, stock, origin, company, image_url, num_options, num_images, groupby_key_sha256, spid) '
      query += 'VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'
      mpid = self.none_to_blank(str(value['result_for_source_view']['my_product_id']))
      status = self.none_to_blank(str(value['result_for_source_view']['status']))
      c_date = self.none_to_blank(str(value['result_for_source_view']['c_date']))
      sm_date = self.none_to_blank(str(value['result_for_source_view']['sm_date']))
      url = self.none_to_blank(str(value['result_for_source_view']['url']))
      url_sha256 = str(hashlib.sha256(url.encode('UTF-8','strict')).hexdigest())
      p_name_origin = self.none_to_blank(str(value['result_for_source_view']['p_name']))
      p_name = p_name_origin.encode('UTF-8','strict').hex()
      p_name_sha256 = str(hashlib.sha256(p_name_origin.encode('UTF-8','strict')).hexdigest())
      sku = self.none_to_blank(str(value['result_for_source_view']['sku']))
      list_price = self.none_to_blank(str(value['result_for_source_view']['list_price']))
      price = self.none_to_blank(str(value['result_for_source_view']['price']))
      stock = self.check_stock(value['result_for_source_view']) 
      if stock == '0':
         #print("Out ot stock. mpid = {}, url = {}".format(mpid, url))
         return
      origin = self.none_to_blank(str(value['result_for_source_view']['origin']))
      company = self.none_to_blank(str(value['result_for_source_view']['company']))
      image_url = self.none_to_blank(str(value['result_for_source_view']['image_url']))
      num_options = self.none_to_blank(str(value['result_for_source_view']['num_options']))
      num_images = self.none_to_blank(str(value['result_for_source_view']['num_images']))
      spid = self.none_to_blank(str(value['result_for_source_view']['spid']))

      groupbykeys = sorted(groupbykeys)
      groupby_key = ''
      for key in groupbykeys:
         if key == 'description':
            groupby_key += value['result_for_desc']['value'].get('description', '')
         else:
            groupby_key += self.none_to_blank(str(value['result_for_source_view'][key])) 
      groupby_key = str(groupby_key)
      groupby_key_sha256 = str(hashlib.sha256(groupby_key.encode()).hexdigest()) 
      self.gp_cur.execute(query, (job_id, mpid, status, c_date, sm_date, url, url_sha256, p_name, p_name_sha256, sku, list_price, price, stock, origin, company, image_url, num_options, num_images, groupby_key_sha256, spid))
      self.gp_conn.commit()
 
 
      # insert data to tmp description_source_view
      query =  'INSERT INTO job'+str(job_id)+'_description_source_view_latest (job_id, mpid, key, value) '
      query += 'VALUES (%s,%s, %s, %s)'
      mpid = str(value['result_for_desc']['my_product_id'])
      for key in value['result_for_desc']['key']:
          if key == 'description':
              attr = self.none_to_blank(str(value['result_for_desc']['value'][key]))
              self.gp_cur.execute(query, (job_id, mpid, 'description_sha256', str(hashlib.sha256(attr.encode()).hexdigest())))
          attr = self.none_to_blank(str(value['result_for_desc']['value'][key]))
          attr = attr.encode('UTF-8').hex()
          self.gp_cur.execute(query, (job_id, mpid, key, attr))
      self.gp_conn.commit()




      # insert data to tmp option_source_view
      query =  'INSERT INTO job'+str(job_id)+'_option_source_view_latest (job_id, mpid, option_name, option_value, list_price, price, stock, stock_status, msg)'
      query += 'VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)'
      mpid = str(value['result_for_option']['my_product_id'])
      for option_name in value['result_for_option']['option_names']:
          for option_val in value['result_for_option']['option_values'][option_name]:
              list_price = self.none_to_blank(str(value['result_for_option']['list_price']))
              price = self.none_to_blank(str(value['result_for_option']['price']))
              stock = self.check_stock(value['result_for_option']) 
              stock_status = self.none_to_blank(str(value['result_for_option']['stock_status'])) 
              msg = self.none_to_blank(str(value['result_for_option']['msg'])) 
              self.gp_cur.execute(query, (job_id, mpid, option_name.encode('utf-8').hex(), option_val.encode('UTF-8').hex(), list_price, price, stock, stock_status, msg))
      self.gp_conn.commit()


      # insert data to tmp option_source_view
      query =  'INSERT INTO job'+str(job_id)+'_thumbnail_source_view_latest (job_id, mpid, image_url, image_url_sha256)'
      query += 'VALUES (%s, %s, %s, %s)'

      mpid = str(value['result_for_option']['my_product_id'])
      for img_url in value['result_for_thumbnail']['image_urls']:
          if img_url is not None:
             self.gp_cur.execute(query, (job_id, mpid, str(img_url), str(hashlib.sha256(img_url.encode()).hexdigest())))
      self.gp_conn.commit()
    except:
      self.gp_conn.rollback()
      print(str(traceback.format_exc()))
      raise





  def insert_node_property_to_tmp_mysite_for_test_targetsite(self, job_id, value, groupbykeys):
    try:
      # create tmp source_view
      job_id = str(job_id)
      query = 'create table if not exists job'+str(job_id)+'_source_view_latest (job_id integer, id integer primary key generated always as identity, mpid integer, status integer, c_date timestamp, sm_date timestamp, url varchar(2048), url_sha256  varchar(64), p_name varchar(2048), p_name_sha256 varchar(64), sku varchar(2048), list_price  varchar(64), price  varchar(64), stock varchar(64), origin varchar(2048), company varchar(2048), image_url varchar(2048), num_options integer, num_images integer, m_category varchar(2048), tpid varchar(128), spid varchar(128), groupby_key_sha256 varchar(64));'
      self.pg_cur.execute(query)
      self.pg_conn.commit()


      # create tmp description_source_view
      query = 'create table if not exists job'+str(job_id)+'_description_source_view_latest (job_id integer, mpid integer, key varchar(2048), value text)'
      self.pg_cur.execute(query)
      self.pg_conn.commit()


      # create tmp option_source_view
      query = 'create table if not exists job'+str(job_id)+'_option_source_view_latest (job_id integer, mpid integer, option_name varchar(2048), option_value varchar(2048), list_price varchar(64), price varchar(64), stock integer, stock_status integer, msg varchar(2048))'
      self.pg_cur.execute(query)
      self.pg_conn.commit()


      # create tmp thumbnail_source_view
      query = 'create table if not exists job'+str(job_id)+'_thumbnail_source_view_latest (job_id integer, mpid integer, image_url varchar(2048), image_url_sha256 varchar(64))'
      self.pg_cur.execute(query)
      self.pg_conn.commit()


      # insert data to tmp source_view
      query =  'INSERT INTO job'+str(job_id)+'_source_view_latest (job_id, mpid, status, c_date, sm_date, url, url_sha256, p_name, p_name_sha256, sku, list_price, price, stock, origin, company, image_url, num_options, num_images, groupby_key_sha256, spid) '
      query += 'VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'
      mpid = self.none_to_blank(str(value['result_for_source_view']['my_product_id']))
      status = self.none_to_blank(str(value['result_for_source_view']['status']))
      c_date = self.none_to_blank(str(value['result_for_source_view']['c_date']))
      sm_date = self.none_to_blank(str(value['result_for_source_view']['sm_date']))
      url = self.none_to_blank(str(value['result_for_source_view']['url']))
      url_sha256 = str(hashlib.sha256(url.encode('UTF-8','strict')).hexdigest())
      p_name_origin = self.none_to_blank(str(value['result_for_source_view']['p_name']))
      if int(mpid) == 24931 or int(mpid) == 24983 or int(mpid) == 25003:
         p_name_origin = 'TEST PRODUCT NAME CHANGE : ' + p_name_origin
      p_name = p_name_origin.encode('UTF-8','strict').hex()
      p_name_sha256 = str(hashlib.sha256(p_name_origin.encode('UTF-8','strict')).hexdigest())
      sku = self.none_to_blank(str(value['result_for_source_view']['sku']))
      list_price = self.none_to_blank(str(value['result_for_source_view']['list_price']))
      price = self.none_to_blank(str(value['result_for_source_view']['price']))
      if int(mpid) == 24953 or int(mpid) == 24944 or int(mpid) == 25000:
         price = '777.77'
      stock = self.none_to_blank(str(value['result_for_source_view']['stock']))
      if stock == '':
         stock = '0'
      else:
         stock = '999'
      # for update test
      if int(mpid) == 25036 or int(mpid) == 25004 or int(mpid) == 25076:
         stock = '777'
      origin = self.none_to_blank(str(value['result_for_source_view']['origin']))
      company = self.none_to_blank(str(value['result_for_source_view']['company']))
      image_url = self.none_to_blank(str(value['result_for_source_view']['image_url']))
      # for update test
      if int(mpid) == 25042 or int(mpid) == 25043 or int(mpid) == 25051:
         image_url = 'https://cdn2.jomashop.com/media/catalog/product/t/i/tissot-t-touch-ii-multi-function-silver-dial-titanium-ladies-watch-t0472204608600.jpg'
      num_options = self.none_to_blank(str(value['result_for_source_view']['num_options']))
      num_images = self.none_to_blank(str(value['result_for_source_view']['num_images']))
      # for update test
      if int(mpid) == 25027 or int(mpid) == 25029 or int(mpid) == 25060:
         num_options = '1' 
      # for update test
      #if int(mpid) == 25042 or int(mpid) == 25043 or int(mpid) == 25051:
      #   num_images = '3'
      spid = self.none_to_blank(str(value['result_for_source_view']['spid']))

      groupbykeys = sorted(groupbykeys)
      groupby_key = ''
      for key in groupbykeys:
         if key == 'description':
            # for update test
            if int(mpid) == 24991 or int(mpid) == 24982 or int(mpid) == 25012: 
               value['result_for_desc']['value'][key] = "TEST for description Change" + value['result_for_desc']['value'][key] 
            groupby_key += value['result_for_desc']['value'].get('description', '')
         else:
            groupby_key += self.none_to_blank(str(value['result_for_source_view'][key])) 
      groupby_key = str(groupby_key)
      #groupby_key = str(p_name_origin) + str(Price.fromstring(str(price)).amount_float) + str(value['result_for_desc'].get('description', '') + str(spid))
      #query =  'INSERT INTO job'+str(job_id)+'_source_view_latest (mpid, status, c_date, sm_date, url, url_sha256, p_name, p_name_sha256, sku, list_price, price, origin, company, image_url, num_options, num_images, groupby_key_sha256, spid) '
      groupby_key_sha256 = str(hashlib.sha256(groupby_key.encode()).hexdigest()) 
      self.gp_cur.execute(query, (job_id, mpid, status, c_date, sm_date, url, url_sha256, p_name, p_name_sha256, sku, list_price, price, stock, origin, company, image_url, num_options, num_images, groupby_key_sha256, spid))
      self.gp_conn.commit()
 
 
      # insert data to tmp description_source_view
      query =  'INSERT INTO job'+str(job_id)+'_description_source_view_latest (job_id, mpid, key, value) '
      query += 'VALUES (%s,%s, %s, %s)'
      mpid = str(value['result_for_desc']['my_product_id'])
      for key in value['result_for_desc']['key']:
          if key == 'description':
              attr = self.none_to_blank(str(value['result_for_desc']['value'][key]))
              self.gp_cur.execute(query, (job_id, mpid, 'description_sha256', str(hashlib.sha256(attr.encode()).hexdigest())))
          attr = self.none_to_blank(str(value['result_for_desc']['value'][key]))
          attr = attr.encode('UTF-8').hex()
          self.gp_cur.execute(query, (job_id, mpid, key, attr))
      self.gp_conn.commit()




      # insert data to tmp option_source_view
      query =  'INSERT INTO job'+str(job_id)+'_option_source_view_latest (job_id, mpid, option_name, option_value, list_price, price, stock, stock_status, msg)'
      query += 'VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)'
      mpid = str(value['result_for_option']['my_product_id'])
      # for update test
      if int(mpid) == 25027 or int(mpid) == 25029 or int(mpid) == 25060: 
          value['result_for_option']['option_names'] = ['test option'] 
          value['result_for_option']['option_values'] = {'test option': ['option val1', 'option val2']}
      for option_name in value['result_for_option']['option_names']:
          for option_val in value['result_for_option']['option_values'][option_name]:
              list_price = self.none_to_blank(str(value['result_for_option']['list_price']))
              price = self.none_to_blank(str(value['result_for_option']['price']))
              stock = self.none_to_blank(str(value['result_for_option']['stock']))
              if stock == '':
                 stock = '0'
              else:
                 stock = '999'
              stock_status = self.none_to_blank(str(value['result_for_option']['stock_status'])) 
              msg = self.none_to_blank(str(value['result_for_option']['msg'])) 
              self.gp_cur.execute(query, (job_id, mpid, option_name.encode('utf-8').hex(), option_val.encode('UTF-8').hex(), list_price, price, stock, stock_status, msg))
              #self.gp_cur.execute(query, (mpid, hashlib.sha256(option_name.encode('utf-8')), hashlib.sha256(option_val.encode('UTF-8')), list_price, price, stock, stock_status, msg))
      self.gp_conn.commit()


      # insert data to tmp option_source_view
      query =  'INSERT INTO job'+str(job_id)+'_thumbnail_source_view_latest (job_id, mpid, image_url, image_url_sha256)'
      query += 'VALUES (%s, %s, %s, %s)'
      if int(mpid) == 25042 or int(mpid) == 25043 or int(mpid) == 25051:
         if value['result_for_thumbnail']['image_urls'] is not None:
            value['result_for_thumbnail']['image_urls'].insert(0,'https://cdn2.jomashop.com/media/catalog/product/t/i/tissot-t-touch-ii-multi-function-silver-dial-titanium-ladies-watch-t0472204608600.jpg')

      mpid = str(value['result_for_option']['my_product_id'])
      for img_url in value['result_for_thumbnail']['image_urls']:
          if img_url is not None:
             self.gp_cur.execute(query, (job_id, mpid, str(img_url), str(hashlib.sha256(img_url.encode()).hexdigest())))
      self.gp_conn.commit()
    except:
      self.gp_conn.rollback()
      print(str(traceback.format_exc()))
      raise




  def insert_node_property_to_mysite_oldversion(self, job_id, value):
    try:
      # create source_view
      query = 'create table if not exists job'+str(job_id)+'_source_view (mpid integer, status integer, c_date timestamp, sm_date timestamp,  url varchar(2048), url_sha256  varchar(64), p_name varchar(2048), p_name_sha256  varchar(64), sku varchar(2048), list_price varchar(64), price varchar(64), origin varchar(2048), company varchar(2048), image_url varchar(2048), num_options integer, num_images integer, m_category varchar(2048) );'
      self.pg_cur.execute(query)
      self.pg_conn.commit()


      # create description_source_view
      query = 'create table if not exists job'+str(job_id)+'_description_source_view (mpid integer, key varchar(2048), value text)'
      self.pg_cur.execute(query)
      self.pg_conn.commit()


      # create option_source_view
      query = 'create table if not exists job'+str(job_id)+'_option_source_view (mpid integer, option_name varchar(2048), option_value varchar(2048), list_price varchar(64), price varchar(64), stock integer, stock_status integer, msg varchar(2048))'
      self.pg_cur.execute(query)
      self.pg_conn.commit()


      # create thumbnail_source_view
      query = 'create table if not exists job'+str(job_id)+'_thumbnail_source_view (mpid integer, image_url varchar(2048), image_url_sha256 varchar(64))'
      self.pg_cur.execute(query)
      self.pg_conn.commit()


      # insert data to source_view
      query =  'INSERT INTO job'+str(job_id)+'_source_view (mpid, status, c_date, sm_date, url,url_sha256, p_name, p_name_sha256, sku, list_price, price, stock, origin, company, image_url, num_options, num_images) '
      query += 'VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'
      mpid = int(value['result_for_source_view']['my_product_id'])
      status = int(value['result_for_source_view']['status'])
      c_date = str(value['result_for_source_view']['c_date'])
      sm_date = str(value['result_for_source_view']['sm_date'])
      url = str(value['result_for_source_view']['url'])
      url_sha256 = str(hashlib.sha256(url.encode('UTF-8','strict')).hexdigest())
      p_name_origin = str(value['result_for_source_view']['p_name'])
      p_name = p_name_origin.encode('UTF-8','strict').hex()
      p_name_sha256 = str(hashlib.sha256(p_name_origin.encode('UTF-8','strict')).hexdigest())
      sku = str(value['result_for_source_view']['sku'])
      list_price = float(value['result_for_source_view']['list_price'])
      price = float(value['result_for_source_view']['price'])
      origin = str(value['result_for_source_view']['origin'])
      company = str(value['result_for_source_view']['company'])
      image_url = str(value['result_for_source_view']['image_url'])
      num_options = int(value['result_for_source_view']['num_options'])
      num_images = int(value['result_for_source_view']['num_images'])
      self.gp_cur.execute(query, (mpid, status, c_date, sm_date, url, url_sha256, p_name, p_name_sha256, sku, list_price, price, stock, origin, company, image_url, num_options, num_images))
      self.gp_conn.commit()
      

      # insert data to description_source_view
      query =  'INSERT INTO job'+str(job_id)+'_description_source_view (mpid, key, value) '
      query += 'VALUES (%s, %s, %s)'
      mpid = int(value['result_for_desc']['my_product_id'])
      for key in value['result_for_desc']['key']:
          if key == 'description':
              attr = str(value['result_for_desc']['value'][key])
              self.gp_cur.execute(query, (mpid, 'description_sha256', str(hashlib.sha256(attr.encode()).hexdigest())))
          attr = str(value['result_for_desc']['value'][key])
          attr = attr.encode('UTF-8').hex()
          self.gp_cur.execute(query, (mpid, key, attr))
      self.gp_conn.commit()


      # insert data to option_source_view
      query =  'INSERT INTO job'+str(job_id)+'_option_source_view (mpid, option_name, option_value, list_price, price, stock, stock_status, msg) '
      query += 'VALUES (%s, %s, %s, %s, %s, %s, %s, %s)'
      mpid = str(value['result_for_option']['my_product_id'])
      for option_name in value['result_for_option']['option_names']:
          for option_val in value['result_for_option']['option_values'][option_name]:
              list_price = float(value['result_for_option']['list_price']) 
              price = float(value['result_for_option']['price']) 
              stock = int(value['result_for_option']['stock']) 
              stock_status = str(value['result_for_option']['stock_status']) 
              msg = str(value['result_for_option']['msg']) 
              #self.gp_cur.execute(query, (mpid, option_name.encode('utf-8').hex(), option_val.encode('UTF-8').hex(), list_price, price, stock, stock_status, msg))
              self.gp_cur.execute(query, (mpid, str(hashlib.sha256(option_name.encode('utf-8')).hexdigest()), str(hashlib.sha256(option_val.encode('UTF-8')).hexdigest()), list_price, price, stock, stock_status, msg))
      self.gp_conn.commit()


      # insert data to thumbnail_source_view
      query =  'INSERT INTO job'+str(job_id)+'_thumbnail_source_view (mpid,image_url, image_url_sha256)'
      query += 'VALUES (%s, %s, %s)'
      mpid = str(value['result_for_option']['my_product_id'])
      for img_url in value['result_for_thumbnail']['image_urls']:
          self.gp_cur.execute(query, (mpid, str(img_url), str(hashlib.sha256(img_url.encode()).hexdigest())))
      self.gp_conn.commit()
    except:
      self.gp_conn.rollback()
      print(str(traceback.format_exc()))
      raise




  def insert_node_property_to_mysite(self, job_id, value, groupbykeys):
    try:

      # insert data to source_view
      #(id integer primary key generated always as identity, job_id integer, mpid integer default nextval('my_product_id'), status integer, c_date timestamp, sm_date timestamp,  url varchar(2048), url_sha256 varchar(64) UNIQUE, p_name varchar(2048), p_name_sha256 varchar(64), sku varchar(2048), list_price varchar(64), price varchar(64), origin varchar(2048), company varchar(2048), image_url varchar(2048), num_options integer, num_images integer, m_category varchar(2048), tpid varchar(2048), groupby_key_sha256 varchar(64)
      job_id = str(job_id)
      query =  'INSERT INTO job_source_view (job_id, mpid, status, c_date, sm_date, url, url_sha256, p_name, p_name_sha256, sku, list_price, price, stock, origin, company, image_url, num_options, num_images, groupby_key_sha256, spid)'
      query += 'VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'

      mpid = self.none_to_blank(str(value['result_for_source_view']['my_product_id']) )
      status = self.none_to_blank(str(value['result_for_source_view']['status']))
      c_date = self.none_to_blank(str(value['result_for_source_view']['c_date']))
      sm_date = self.none_to_blank(str(value['result_for_source_view']['sm_date']))
      url = self.none_to_blank(str(value['result_for_source_view']['url']))
      url_sha256 = str(hashlib.sha256(url.encode('UTF-8','strict')).hexdigest())
      p_name_origin = self.none_to_blank(str(value['result_for_source_view']['p_name']))
      p_name = str(p_name_origin.encode('UTF-8','strict').hex())
      p_name_sha256 = str(hashlib.sha256(p_name_origin.encode('UTF-8','strict')).hexdigest())
      sku = self.none_to_blank(str(value['result_for_source_view']['sku']))
      list_price = self.none_to_blank(str(value['result_for_source_view']['list_price']))
      price = self.none_to_blank(str(value['result_for_source_view']['price']))
      stock = self.check_stock(value['result_for_source_view']) 
      if stock == '0':
         #print("Out ot stock. mpid = {}, url = {}".format(mpid, url))
         return
      origin = self.none_to_blank(str(value['result_for_source_view']['origin']))
      company = self.none_to_blank(str(value['result_for_source_view']['company']))
      image_url = self.none_to_blank(str(value['result_for_source_view']['image_url']))
      num_options = self.none_to_blank(str(value['result_for_source_view']['num_options']))
      num_images = self.none_to_blank(str(value['result_for_source_view']['num_images']))
      spid = self.none_to_blank(str(value['result_for_source_view']['spid']))
      
      groupbykeys = sorted(groupbykeys)
      groupby_key = ''
      for key in groupbykeys:
         if key == 'description':
            groupby_key += value['result_for_desc']['value'].get('description', '')
         else:
            groupby_key += self.none_to_blank(str(value['result_for_source_view'][key])) 
      groupby_key = str(groupby_key)
      groupby_key_sha256 = str(hashlib.sha256(groupby_key.encode()).hexdigest()) 
      self.gp_cur.execute(query, (job_id, mpid, status, c_date, sm_date, url, url_sha256, p_name, p_name_sha256, sku, list_price, price, stock, origin, company, image_url, num_options, num_images, groupby_key_sha256, spid))
      self.gp_conn.commit()

            

      # insert data to description_source_view
      # (job_id integer, mpid integer, key varchar(2048), value text)
      query =  'INSERT INTO job_description_source_view (job_id, mpid, key, value) '
      query += 'VALUES (%s, %s, %s, %s)'
      mpid = int(value['result_for_desc']['my_product_id'])
      for key in value['result_for_desc']['key']:
          if key == 'description':
              attr = self.none_to_blank(str(value['result_for_desc']['value'][key]))
              self.gp_cur.execute(query, (job_id, str(mpid), 'description_sha256', str(hashlib.sha256(attr.encode()).hexdigest())))
          attr = str(value['result_for_desc']['value'][key])
          attr = attr.encode('UTF-8').hex()
          self.gp_cur.execute(query, (job_id, mpid, key, attr))
      self.gp_conn.commit()


      # insert data to option_source_view
      # (job_id integer, mpid integer, option_name varchar(2048), option_value varchar(2048), list_price varchar(64), price varchar(64), stock integer, stock_status integer, msg varchar(2048))
      query =  'INSERT INTO job_option_source_view (job_id, mpid, option_name, option_value, list_price, price, stock, stock_status, msg) '
      query += 'VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)'
      mpid = str(value['result_for_option']['my_product_id'])
      for option_name in value['result_for_option']['option_names']:
          for option_val in value['result_for_option']['option_values'][option_name]:
              list_price = self.none_to_blank(str(value['result_for_option']['list_price'])) 
              price = self.none_to_blank(str(value['result_for_option']['price'])) 
              stock = self.check_stock(value['result_for_option']) 
              stock_status = self.none_to_blank(str(value['result_for_option']['stock_status']))
              msg = self.none_to_blank(str(value['result_for_option']['msg'])) 
              self.gp_cur.execute(query, (job_id, mpid, str(hashlib.sha256(option_name.encode('utf-8')).hexdigest()), str(hashlib.sha256(option_val.encode('UTF-8')).hexdigest()), list_price, price, stock, stock_status, msg))
      self.gp_conn.commit()


      # insert data to thumbnail_source_view
      # (job_id integer, mpid integer, image_url varchar(2048), image_url_sha256 varchar(64))
      query =  'INSERT INTO job_thumbnail_source_view (job_id, mpid, image_url, image_url_sha256)'
      query += 'VALUES (%s, %s, %s, %s)'
      mpid = str(value['result_for_option']['my_product_id'])
      for img_url in value['result_for_thumbnail']['image_urls']:
          self.gp_cur.execute(query, (job_id, mpid, str(img_url), str(hashlib.sha256(img_url.encode()).hexdigest())))
      self.gp_conn.commit()
    except:
      self.gp_conn.rollback()
      print(str(traceback.format_exc()))
      raise


  def test(self):
    try:
      query = 'select * from job_source_view where job_id = 183;'
      self.gp_cur.execute(query)
      res = self.gp_cur.fetchall()
      self.gp_conn.commit()
      print(res)

    except:
      self.gp_conn.rollback()
      print(str(traceback.format_exc()))
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
      print(str(traceback.format_exc()))
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
      print(str(traceback.format_exc()))
      raise
 
  def get_node_properties_from_mysite(self, exec_id, mpid):
    try:
      query = 'select job_id from execution where id = {}'.format(exec_id)
      self.gp_cur.execute(query)
      job_id = self.gp_cur.fetchone()[0]

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
        if col_names[i] == 'p_name':
           result[col_names[i]] = bytes.fromhex(col_values[i]).decode()
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

      #query = "select column_name from information_schema.columns where table_name = 'job"+str(job_id)+"_option_source_view'";
      #self.gp_cur.execute(query)
      #col_names = self.gp_cur.fetchall()
      #self.gp_conn.commit()

      query = 'select option_name, option_value, stock from job_option_source_view where mpid = {} and job_id = {}'.format(mpid, job_id)
      self.gp_cur.execute(query)
      rows = self.gp_cur.fetchall()
      self.gp_conn.commit()
      result['option_name'] = set()
      result['option_value'] = {}
      for row in rows:
        op_n = bytes.fromhex(row[0]).decode()
        op_v = bytes.fromhex(row[1]).decode()
        #op_v_stock = row[2]
        result['option_name'].add(op_n)
        if result['option_value'].get(op_n,None) == None:
          result['option_value'][op_n] = []
        result['option_value'][op_n].append(op_v)
      
      return result
    except:
      self.gp_conn.rollback()
      print(str(traceback.format_exc()))
      raise



 
  def get_node_properties_from_mysite_oldversion(self, exec_id, mpid):
    try:
      query = 'select job_id from execution where id = {}'.format(exec_id)
      self.gp_cur.execute(query)
      job_id = self.gp_cur.fetchone()[0]
      self.gp_conn.commit()

      query = "select column_name from information_schema.columns where table_name = 'job"+str(job_id)+"_source_view'";
      self.gp_cur.execute(query)
      col_names_tmp = self.gp_cur.fetchall()
      self.gp_conn.commit()

      query = 'select * from job'+str(job_id)+'_source_view where mpid = {}'.format(mpid)
      self.gp_cur.execute(query)
      col_values = self.gp_cur.fetchall()
      col_values = col_values[0]
      self.gp_conn.commit()
      col_names = []
      for i in col_names_tmp:
        col_names.append(i[0])
      result = {}
      for i in range(0, len(col_names)):
        if col_names[i] == 'p_name':
           result[col_names[i]] = bytes.fromhex(col_values[i]).decode()
        else:    
           result[col_names[i]] = col_values[i]
      

      query = 'select * from job'+str(job_id)+'_thumbnail_source_view where mpid = {}'.format(mpid)
      self.gp_cur.execute(query)
      rows = self.gp_cur.fetchall()
      self.gp_conn.commit()
      result['images'] = []
      for row in rows:
        result['images'].append(row[1])
      

      query = 'select * from job'+str(job_id)+'_description_source_view where mpid = {}'.format(mpid)
      self.gp_cur.execute(query)
      rows = self.pg_cur.fetchall()
      self.pg_conn.commit()
 
      for row in rows:
        result[row[1]] = bytes.fromhex(row[2]).decode()

      #query = "select column_name from information_schema.columns where table_name = 'job"+str(job_id)+"_option_source_view'";
      #self.gp_cur.execute(query)
      #col_names = self.gp_cur.fetchall()
      #self.gp_conn.commit()

      query = 'select * from job'+str(job_id)+'_option_source_view where mpid = {}'.format(mpid)
      self.gp_cur.execute(query)
      rows = self.gp_cur.fetchall()
      self.gp_conn.commit()
      result['option_name'] = set()
      result['option_value'] = {}
      print(rows)
      for row in rows:
        print(row[1])
        op_n = bytes.fromhex(row[1]).decode()
        print(op_n)
        print(row[2])
        op_v = bytes.fromhex(row[2]).decode()
        print(op_v)
        result['option_name'].add(op_n)
        if result['option_value'].get(op_n,None) == None:
          result['option_value'][op_n] = []
        result['option_value'][op_n].append(op_v)
      
      print(result)
      return result
    except:
      self.gp_conn.rollback()
      print(str(traceback.format_exc()))
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
      print(str(traceback.format_exc()))
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
      print(str(traceback.format_exc()))
      raise



  def update_targetsite_product_id(self, job_id, tpid, mpid, targetsite_url):
    try:
      query = "update tpid_mapping set tpid = "+str(tpid)+" where mpid = {} and job_id = {} and targetsite_url = '{}';".format(mpid, job_id, targetsite_url)
      print(query)
      self.gp_cur.execute(query)
      self.gp_conn.commit()
      return
    except:
      self.gp_conn.rollback()
      print(str(traceback.format_exc()))
      raise

  def update_targetsite_product_id_oldversion(self, job_id, product_no, mpid):
    try:
      query = "update job_source_view set tpid = "+str(product_no)+" where mpid = {} and job_id = {};".format(mpid, job_id)
      self.gp_cur.execute(query)
      self.gp_conn.commit()
      return
    except:
      self.gp_conn.rollback()
      print(str(traceback.format_exc()))
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
  #    print(str(traceback.format_exc()))
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
      print(str(traceback.format_exc()))
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
      else:
         result['mpid'] = rows[0][0]

      return result
    except:
      self.gp_conn.rollback()
      print(str(traceback.format_exc()))
      raise


  def get_node_properties_oldversion(self, nodeId):
    try:
      query =  'select key, value'
      query += ' from node_property'
      query += ' where node_id = {}'.format(nodeId) 
      self.gp_cur.execute(query)
      rows = self.gp_cur.fetchall()
      self.gp_conn.commit()
      result = {}
      print(rows)
      for row in rows:
        #print(row[1])
        result[row[0]] = row[1]
      print(result)
      return result
    except:
      self.gp_conn.rollback()
      print(str(traceback.format_exc()))
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
      print(str(traceback.format_exc()))
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
      print(str(traceback.format_exc()))
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
      print(str(traceback.format_exc()))
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
      print(str(traceback.format_exc()))
      raise



  #(id integer primary key generated always as identity, job_id integer, mpid integer unique, targetsite_url varchar(256), tpid integer, upload_time timestamp);
  def insert_tpid_into_history_table(self, job_id, targetsite_url, mpid, tpid):
    try:
      query = "insert into tpid_history(job_id, mpid, targetsite_url, tpid) values({},{},'{}',{})".format(job_id, mpid, targetsite_url, tpid)
      self.gp_cur.execute(query)
      self.gp_conn.commit()
      return
    except:
      self.gp_conn.rollback()
      print(str(traceback.format_exc()))
      raise


  def delete_from_tpid_mapping_table(self, tpid):
    try:
      query = "delete from tpid_mapping where tpid = {}".format(tpid)
      self.gp_cur.execute(query)
      self.gp_conn.commit()
      return 
    except:
      self.gp_conn.rollback()
      print(str(traceback.format_exc()))
      raise



  def update_tpid_into_mapping_table(self, job_id, tpid, mpid, targetsite_url):
    try:
      query = "select count(*) from tpid_mapping where job_id = {} and targetsite_url = '{}' and mpid = {}".format(job_id, targetsite_url, mpid)
      self.gp_cur.execute(query)
      rows = self.gp_cur.fetchone()
      self.gp_conn.commit()
      result = rows[0]
      if int(result) == 0:
        self.insert_tpid_into_mapping_table(job_id, targetsite_url, mpid, tpid)
      else: 
        query = "update tpid_mapping set tpid = {} where job_id = {} and targetsite_url = '{}' and mpid = {}".format(tpid, job_id, targetsite_url, mpid)
        self.gp_cur.execute(query)
        self.gp_conn.commit()
      return 
    except:
      self.gp_conn.rollback()
      print(str(traceback.format_exc()))
      raise


  def check_is_item_uploaded(self, job_id, targetsite_url, mpid):
    try:
      query = "select count(*) from tpid_mapping where job_id = {} and targetsite_url like '%{}%' and mpid = {}".format(job_id, targetsite_url, mpid)
      self.gp_cur.execute(query)
      rows = self.gp_cur.fetchone()
      self.gp_conn.commit()
      result = rows[0]
      if int(result) == 0:
        return False
      else:
        return True
    except:
      self.gp_conn.rollback()
      print(str(traceback.format_exc()))
      raise


  def insert_tpid_into_mapping_table(self, job_id, targetsite_url, mpid, tpid):
    try:
      query = "insert into tpid_mapping(job_id, mpid, targetsite_url, tpid) values({},{},'{}',{})".format(job_id, mpid, targetsite_url, tpid)
      self.gp_cur.execute(query)
      self.gp_conn.commit()
      return
    except:
      self.gp_conn.rollback()
      print(str(traceback.format_exc()))
      raise


  def get_tpid(self, job_id, targetsite_url, mpid):
    try:
      query =  "select tpid from tpid_mapping where job_id = {} and targetsite_url = '{}' and mpid = {}".format(job_id, targetsite_url, mpid)
      self.gp_cur.execute(query)
      rows = self.gp_cur.fetchone()
      self.gp_conn.commit()
      result = rows[0]
      return result
    except:
      self.gp_conn.rollback()
      print(str(traceback.format_exc()))
      raise


  def get_tpid_oldversion(self, job_id, mpid):
    try:
      query =  'select tpid from job_source_view where job_id = {} and mpid = {}'.format(job_id, mpid)
      self.gp_cur.execute(query)
      rows = self.gp_cur.fetchone()
      self.gp_conn.commit()
      result = rows[0]
      return result
    except:
      self.gp_conn.rollback()
      print(str(traceback.format_exc()))
      raise


  #(id integer primary key generated always as identity, execution_id integer,  start_time timestamp, end_time timestamp, job_id integer);
  def insert_sm_history(self, execution_id, start_date, job_id):
    try:
      query =  "insert into sm_history(job_id, execution_id, start_time) values({},{},'{}') returning id".format(job_id, execution_id, start_date)
      self.gp_cur.execute(query)
      rows = self.gp_cur.fetchone()
      self.gp_conn.commit()
      result = rows[0]
      return result
    except:
      self.gp_conn.rollback()
      print(str(traceback.format_exc()))
      raise

  def update_sm_history(self, end_date, input_id):
    try:
      query =  "update sm_history set end_time = '{}' where id = {}".format(end_date, input_id)
      self.gp_cur.execute(query)
      self.gp_conn.commit()
      return 
    except:
      self.gp_conn.rollback()
      print(str(traceback.format_exc()))
      raise

  #(id integer primary key generated always as identity, sm_history_id bigint, start_time timestamp, end_time timestamp, targetsite text, job_id integer);
  def insert_mt_history(self, targetsite, start_time, job_id):
    try:
      query =  "insert into mt_history(job_id, targetsite, start_time) values({},'{}','{}') returning id".format(job_id, targetsite, start_time)
      self.gp_cur.execute(query)
      rows = self.gp_cur.fetchone()
      self.gp_conn.commit()
      result = rows[0]
      return result
    except:
      self.gp_conn.rollback()
      print(str(traceback.format_exc()))
      raise

  def update_mt_history(self, end_date, input_id):
    try:
      query =  "update mt_history set end_time = '{}' where id = {}".format(end_date, input_id)
      self.gp_cur.execute(query)
      self.gp_conn.commit()
      return 
    except:
      self.gp_conn.rollback()
      print(str(traceback.format_exc()))
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
      print(str(traceback.format_exc()))
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
      print(str(traceback.format_exc()))
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
      print(str(traceback.format_exc()))
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
      print(str(traceback.format_exc()))
      raise






  def update_last_sm_date_in_job_configuration(self, sm_time, job_id):
    try:
      query =  "update job_configuration set last_sm_date = '{}' where job_id = {}".format(sm_time, job_id)
      self.gp_cur.execute(query)
      self.gp_conn.commit()
      return 
    except:
      self.gp_conn.rollback()
      print(str(traceback.format_exc()))
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
      print(str(traceback.format_exc()))  
      raise

  def get_targetsiteOld(self, job_id):  
    try:                                                                                         
      query = 'select targetsite_id from job_configuration where job_id = {}'.format(job_id)  
      self.gp_cur.execute(query)
      res = self.gp_cur.fetchone()[0]
                                    
      query = 'select url, gateway from targetsite where id = {}'.format(res)              
      self.gp_cur.execute(query)        
      res = self.gp_cur.fetchone()     

      url = bytes.fromhex(res[0]).decode()
      gateway = bytes.fromhex(res[1]).decode()
                                             
      return url, gateway                   
    except:                                
      self.gp_conn.rollback()             
      print(str(traceback.format_exc()))  
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
      print(str(traceback.format_exc()))
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
      print(str(traceback.format_exc()))
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
      print(str(traceback.format_exc()))
      raise
 
  def get_cnum_from_job_configuration(self, job_id):
    try:
      query =  "select cnum from job_configuration where job_id = {}".format(job_id)
      self.gp_cur.execute(query)
      result = self.gp_cur.fetchone()
      return result[0]
    except:
      self.gp_conn.rollback()
      print(str(traceback.format_exc()))
      raise
 
 
  def get_cnum_from_targetsite_job_configuration_using_tsid(self, tsid):
    try:
      query =  "select cnum from targetsite_job_configuration where id = {}".format(tsid)
      self.gp_cur.execute(query)
      result = self.gp_cur.fetchone()
      return result[0]
    except:
      self.gp_conn.rollback()
      print(str(traceback.format_exc()))
      raise
 
  def add_worker(self, ip, port):
    try:
      query = "insert into registered_workers(ip, port) values('{}', {}) returning id".format(ip, port)
      self.gp_cur.execute(query)
      wid = self.gp_cur.fetchone()[0]
      self.gp_conn.commit()
      return wid 
    except:
      self.gp_conn.rollback()
      print(str(traceback.format_exc()))
      raise
 
  def delete_worker(self, worker_name):
    try:
      query = "delete from registered_workers where id = '{}'".format(worker_name.split('-')[1])
      self.gp_cur.execute(query)
      self.gp_conn.commit()
      return 
    except:
      self.gp_conn.rollback()
      print(str(traceback.format_exc()))
      raise

 
  def get_workers(self):
    try:
      query =  "select id, ip, port from registered_workers"
      self.gp_cur.execute(query)
      result = self.gp_cur.fetchall()
      return result
    except:
      self.gp_conn.rollback()
      print(str(traceback.format_exc()))
      raise


 
  def get_workers_ip_and_port(self, worker_name):
    try:
      query =  "select ip, port from registered_workers where id = '{}'".format(worker_name.split('-')[1])
      self.gp_cur.execute(query)
      result = self.gp_cur.fetchone()
      return result
    except:
      self.gp_conn.rollback()
      print(str(traceback.format_exc()))
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
      print(str(traceback.format_exc()))
      raise



 
  def get_job_configuration(self, job_id):
    try:
      query =  "select cnum from job_configuration where job_id = {}".format(job_id)
      self.gp_cur.execute(query)
      result = self.gp_cur.fetchone()
      return result[0]
    except:
      self.gp_conn.rollback()
      print(str(traceback.format_exc()))
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
      print(str(traceback.format_exc()))
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
      print(str(traceback.format_exc()))
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
      print(str(traceback.format_exc()))
      raise



  def get_selected_transformation_programOld(self, job_id):
    try:
      query =  "select transformation_program_id  from job_configuration where job_id = {}".format(job_id)
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
      print(str(traceback.format_exc()))
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
      print(str(traceback.format_exc()))
      raise




  def get_pricing_information_onetime(self, tsid):
    try:
# id | job_id | targetsite_id |     targetsite_label     |                         targetsite_url                         |        t_category        | transformation_program_id | cid | cnum |   exchange_rate    | tariff_rate | vat_rate | tariff_threshold | margin_rate | min_margin | delivery_company | shipping_cost
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
      print(str(traceback.format_exc()))
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
      print(str(traceback.format_exc()))
      raise

  def logging_all_uploaded_product(self, job_id, execution_id, mpid, origin_product, converted_product, targetsite_url, cnum):
    try:
      #query =  "insert into all_uploaded_product(job_id, execution_id, mpid, origin_product, converted_product, targetsite_url, cnum) values({}, {}, {}, '{}', '{}','{}',{})".format(job_id, execution_id, mpid,  json.dumps(origin_product, default=self.json_default), json.dumps(converted_product,default=self.json_default ), targetsite_url, cnum)
      try:
        if origin_product['sm_date'] != '' and origin_product['sm_date'] != None:
          origin_product['sm_date'] = origin_product['sm_date'].strftime('%Y-%m-%d %H:%M:%S')
      except:
        origin_product['sm_date'] = ''
        pass
      origin_product['html'] =  origin_product['html'].encode('UTF-8').hex()
      origin_product['option_name'] = repr(origin_product['option_name']).encode('UTF-8').hex()
      origin_product['option_value'] = repr(origin_product['option_value']).encode('UTF-8').hex()
      converted_product['description'] = converted_product['description'].encode('UTF-8').hex()
      #converted_product['option_name'] = converted_product['option_name'].encode('UTF-8').hex()
      #converted_product['option_value'] = converted_product['option_value'].encode('UTF-8').hex()
      #print(origin_product)
      #print('@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@')
      for key in sorted(origin_product.keys()):
        if key not in ['mpid', 'url','p_name', 'name', 'price', 'stock', 'sku', 'list_price', 'origin', 'company', 'html', 'option_name', 'option_value', 'brand',  'item_no', 'front_image', 'pricing_information', 'option_value', 'shipping_fee']:
          origin_product.pop(key)
          
      #print(origin_product)
      #print('@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@')
      #print(converted_product)
      #print('@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@')
      query =  "insert into all_uploaded_product(job_id, execution_id, mpid, origin_product, converted_product, targetsite_url, cnum) values({}, {}, {}, '{}', '{}','{}',{})".format(job_id, execution_id, mpid,  json.dumps(origin_product), json.dumps(converted_product), targetsite_url, cnum)
      self.gp_cur.execute(query)
      self.gp_conn.commit()
      return 
    except:
      self.gp_conn.rollback()
      print(str(traceback.format_exc()))
      raise




  def create_row_job_current_working(self, job_id):
    try:
      query =  "insert into job_current_working(job_id) values({})".format(job_id)
      self.gp_cur.execute(query)
      self.gp_conn.commit()
      return 
    except:
      self.gp_conn.rollback()
      print(str(traceback.format_exc()))
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
      print(str(traceback.format_exc()))
      raise

  #failed_target_site_detail (id integer primary key generated always as identity, sm_history_id integer, mpid bigint, err_msg text);
  def log_err_msg_of_upload(self, mpid, err_msg, mt_history_id):
    try:
      err_msg = err_msg.replace("'",'"')
      query = "insert into failed_target_site_detail(mpid, err_msg, mt_history_id) values({},'{}',{})".format(mpid,err_msg, mt_history_id)
      self.gp_cur.execute(query)
      self.gp_conn.commit()
      return 
    except:
      self.gp_conn.rollback()
      raise




  def log_to_job_current_targetsite_working(self, log, job_id):
    try:
      query = "update job_current_working set targetsite_working = targetsite_working || '{}' where job_id = {}".format(log, job_id)
      self.gp_cur.execute(query)
      self.gp_conn.commit()
      return 
    except:
      self.gp_conn.rollback()
      print(str(traceback.format_exc()))
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
      print(str(traceback.format_exc()))
      raise


  def re_log_to_job_current_mysite_working(self, log, job_id):
    try:
      query =  "update job_current_working set mysite_working = '{}' where job_id = {}".format(log, job_id)
      self.gp_cur.execute(query)
      self.gp_conn.commit()
      return 
    except:
      self.gp_conn.rollback()
      print(str(traceback.format_exc()))
      raise



  def re_log_to_job_current_crawling_working(self, log, job_id):
    try:
      query =  "update job_current_working set crawling_working = '{}' where job_id = {}".format(log, job_id)
      self.gp_cur.execute(query)
      self.gp_conn.commit()
      return 
    except:
      self.gp_conn.rollback()
      print(str(traceback.format_exc()))
      raise



  def log_to_job_current_crawling_working(self, log, job_id):
    try:
      query = "update job_current_working set crawling_working = crawling_working || '{}' where job_id = {}".format(log, job_id)
      self.gp_cur.execute(query)
      self.gp_conn.commit()
      return 
    except:
      self.gp_conn.rollback()
      print(str(traceback.format_exc()))
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
      print(str(traceback.format_exc()))
      raise


  def update_last_mt_date_in_job_configuration(self, mt_time, job_id, tsid):
    try:
      query =  "update job_configuration set last_mt_date = '{}' where job_id = {} and tsid = {}".format(mt_time, job_id, tsid)
      self.gp_cur.execute(query)
      self.gp_conn.commit()
      return
    except:
      self.gp_conn.rollback()
      print(str(traceback.format_exc()))
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
      print(str(traceback.format_exc()))
      raise

  def json_default(self, value):
    if isinstance(value, date): 
       return str(value.strftime('%Y-%m-%d %H:%M:%S'))
    elif isinstance(value, set):
       return ''
    print(value.replace("'","\'"))
    return value.replace("'","\'")
    raise TypeError('not JSON serializable')

  def get_client(self, mall_id, job_id):
    try:
      query = "BEGIN;  Lock table cafe24_client_id in ACCESS EXCLUSIVE MODE;"
      self.gp_cur.execute(query)
      self.gp_conn.commit()
      query = "update cafe24_client_id set use_now = 1, get_time = now(), job_id = {} where id in (select min(id) from cafe24_client_id where use_now = -1 and mall_id = '{}') returning client_id, client_secret;".format(job_id, mall_id)
      self.gp_cur.execute(query)
      result = self.gp_cur.fetchone()
      self.gp_conn.commit()
      print(result)
      query = "COMMIT;"
      self.gp_cur.execute(query)
      #query =  "update cafe24_client_id set use_now = 1 where id in (select min(id) from cafe24_client_id where use_now = -1 and mall_id = '{}') returning client_id, client_secret".format(mall_id)

      #self.gp_cur.execute(query)
      #self.gp_conn.commit()
      #result = self.gp_cur.fetchone()
      return result
    except Exception as e:
      print(str(traceback.format_exc()))
      return None
      #self.pg_conn.rollback()
      #print(str(traceback.format_exc()))
      #raise e

  def return_client(self, cid, cs):
    try:
      query =  "update cafe24_client_id set use_now = -1,get_time = Null, job_id = Null where client_id = '{}' and client_secret = '{}';".format(cid, cs)

      self.gp_cur.execute(query)
      self.gp_conn.commit()
      return 
    except Exception as e:
      self.gp_conn.rollback()
      print(str(traceback.format_exc()))
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
      print(str(traceback.format_exc()))
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
      print(str(traceback.format_exc()))
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
      print(str(traceback.format_exc()))
      raise

