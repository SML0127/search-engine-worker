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


from APIManagers_for_hide import Cafe24Manager
from multiprocessing.dummy import Pool as ThreadPool
from graph_manager import GraphManager
from settings_manager import *



class Cafe24SingleUploader(Resource):

    def __init__(self):
        self.setting_manager = SettingsManager()
        self.setting_manager.setting(
            "/home/pse/PSE-engine/settings-worker.yaml")
        settings = self.setting_manager.get_settings()
        self.graph_manager = GraphManager()
        self.graph_manager.init(settings)
        self.cafe24manager = ''
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
            # return {}
            num_threads = args['num_threads']
            chunk_size = (len(mpids) // num_threads) + 1
            mpid_chunks = [mpids[i:i + chunk_size]
                           for i in range(0, len(mpids), chunk_size)]
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
            mpid_chunks = [mpids[i:i + chunk_size]
                           for i in range(0, len(mpids), chunk_size)]
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
            # print(profiling_info)
            # print(traceback.format_exc())
            return profiling_info
        profiling_info['total_time'] = time.time() - total_time
        # print(profiling_info)
        pool.close()
        pool.join()
        return profiling_info

    def upload_products_of_task_from_mpid(self, task):
        total_time = time.time()
        successful_node = 0
        failed_node = 0
        profiling_info = {}
        log_mpid = -1
        log_mt_history_id = -1
        try:
            (args, mpids) = task
            log_mt_history_id = args['mt_history_id']
            self.cafe24manager = Cafe24Manager(args)
            print("-----------------------Request auth code----------------------")
            self.cafe24manager.get_auth_code()
            print("-----------------------Request token--------------------------")
            self.cafe24manager.get_token()
            self.cafe24manager.list_brands()
            #targetsite_url = 'https://{}.cafe24.com/'.format(args['mall_id'])

            exporter = Exporter()
            exporter.init()
            exporter.import_rules_from_code(args['code'])

            #print(exec_id, label)
            # print(node_ids)
            job_id = args['job_id']
            tsid = args['tsid']
            targetsite_url, gateway = self.graph_manager.get_targetsite(tsid)
            print("tsid: ", tsid)
            print("targetsite: ", targetsite_url)
            if 'selected' in args:
                for mpid in mpids:
                    log_mpid = mpid
                    node_time = time.time()
                    try:
                        product, original_product_information = exporter.export_from_selected_mpid(
                            job_id, args['execution_id'], mpid)
                        product['targetsite_url'] = targetsite_url
                        product['mpid'] = mpid
                        status = self.graph_manager.check_status_of_product(
                            job_id, mpid)

                        # Status 0 = up to date, 1 = changed, 2 = New, 3 = Deleted 4 = Duplicated
                        print('status : ', status)
                        if gateway.upper() == 'CAFE24':
                            tpid = self.graph_manager.get_tpid(
                                job_id, targetsite_url, mpid)
                            self.cafe24manager.update_exist_product(
                                product, profiling_info, job_id, tpid)
                            self.cafe24manager.refresh()
                        cnum = self.graph_manager.get_cnum_from_targetsite_job_configuration_using_tsid(
                            tsid)
                        # smlee
                        try:
                            self.graph_manager.logging_all_uploaded_product(
                                job_id, args['execution_id'], mpid, original_product_information, product, targetsite_url, cnum)
                        except:
                            self.graph_manager.logging_all_uploaded_product(job_id, args['execution_id'], mpid, {
                                                                            'Error': 'Logging error'}, {'Error': 'Logging error'}, targetsite_url, cnum)
                            pass

                        successful_node += 1
                    except:
                        failed_node += 1
                        err_msg = '================================ Operator ================================ \n'
                        err_msg += 'Update exist product \n\n'
                        err_msg += '================================ My site product id ================================ \n'
                        err_msg += 'My site product id: ' + \
                            str(log_mpid) + '\n\n'
                        err_msg += '================================ Target site URL ================================ \n'
                        err_msg += 'URL: ' + targetsite_url + '\n\n'
                        err_msg += '================================ STACK TRACE ============================== \n' + \
                            str(traceback.format_exc())
                        self.graph_manager.log_err_msg_of_upload(
                            log_mpid, err_msg, log_mt_history_id)

            elif 'onetime' in args:
                for mpid in mpids:
                    log_mpid = mpid
                    node_time = time.time()
                    log_opertion = ''
                    try:
                        status = self.graph_manager.check_status_of_product(
                            job_id, mpid)

                        # Status 0 = up to date, 1 = changed, 2 = New, 3 = Deleted 4 = Duplicated
                        print('status : ', status)
                        if gateway.upper() == 'CAFE24':
                            is_uploaded = self.graph_manager.check_is_item_uploaded(
                                job_id, targetsite_url, mpid)
                            print('is uploaded proudct? : ', is_uploaded)
                            if is_uploaded == False and status != 3:  # upload as new item
                                product, original_product_information = exporter.export_from_mpid_onetime(
                                    job_id, args['execution_id'], mpid, tsid)
                                log_opertion = 'Create new product'

                                product['targetsite_url'] = targetsite_url
                                product['mpid'] = mpid
                                self.cafe24manager.upload_new_product(
                                    product, profiling_info, job_id)
                                cnum = self.graph_manager.get_cnum_from_targetsite_job_configuration_using_tsid(
                                    tsid)
                                # smlee
                                try:
                                    self.graph_manager.logging_all_uploaded_product(
                                        job_id, args['execution_id'], mpid, original_product_information, product, targetsite_url, cnum)
                                except:
                                    self.graph_manager.logging_all_uploaded_product(job_id, args['execution_id'], mpid, {
                                                                                    'Error': 'Logging error'}, {'Error': 'Logging error'}, targetsite_url, cnum)
                                    pass

                            elif is_uploaded == True:
                                if status == 1:
                                    product, original_product_information = exporter.export_from_mpid_onetime(
                                        job_id, args['execution_id'], mpid, tsid)
                                    log_opertion = 'Update exist product'
                                    product['targetsite_url'] = targetsite_url
                                    product['mpid'] = mpid
                                    tpid = self.graph_manager.get_tpid(
                                        job_id, targetsite_url, mpid)
                                    print('tpid : ', tpid)
                                    self.cafe24manager.update_exist_product(
                                        product, profiling_info, job_id, tpid)
                                    cnum = self.graph_manager.get_cnum_from_targetsite_job_configuration_using_tsid(
                                        tsid)
                                    # smlee
                                    try:
                                        self.graph_manager.logging_all_uploaded_product(
                                            job_id, args['execution_id'], mpid, original_product_information, product, targetsite_url, cnum)
                                    except:
                                        self.graph_manager.logging_all_uploaded_product(job_id, args['execution_id'], mpid, {
                                                                                        'Error': 'Logging error'}, {'Error': 'Logging error'}, targetsite_url, cnum)
                                        pass
                                elif status == 3:
                                    tpid = self.graph_manager.get_tpid(
                                        job_id, targetsite_url, mpid)
                                    print('tpid : ', tpid)
                                    log_opertion = 'Delete product'
                                    self.cafe24manager.hide_exist_product(
                                        profiling_info, job_id, tpid)
                                    cnum = self.graph_manager.get_cnum_from_targetsite_job_configuration_using_tsid(
                                        tsid)
                                    # smlee
                                    try:
                                        self.graph_manager.logging_all_uploaded_product(job_id, args['execution_id'], mpid, {
                                                                                        'status': '3'}, {'status': '3'}, targetsite_url, cnum)
                                    except:
                                        self.graph_manager.logging_all_uploaded_product(job_id, args['execution_id'], mpid, {
                                                                                        'Error': 'Logging error'}, {'Error': 'Logging error'}, targetsite_url, cnum)
                                        pass

                            self.cafe24manager.refresh()
                        successful_node += 1
                    except:
                        failed_node += 1
                        err_msg = '================================ Operator ================================ \n'
                        err_msg += log_operation + '\n\n'
                        err_msg += '================================ My site product id ================================ \n'
                        err_msg += 'My site product id: ' + \
                            str(log_mpid) + '\n\n'
                        err_msg += '================================ Target site URL ================================ \n'
                        err_msg += 'URL: ' + targetsite_url + '\n\n'
                        err_msg += '================================ STACK TRACE ============================== \n' + \
                            str(traceback.format_exc())
                        self.graph_manager.log_err_msg_of_upload(
                            log_mpid, err_msg, log_mt_history_id)

            else:
                for mpid in mpids:
                    log_mpid = mpid
                    node_time = time.time()
                    log_opertion = ''
                    try:
                        status = self.graph_manager.check_status_of_product(
                            job_id, mpid)
                        # Status 0 = up to date, 1 = changed, 2 = New, 3 = Deleted 4 = Duplicated
                        print('status : ', status)
                        if gateway.upper() == 'CAFE24':
                            is_uploaded = self.graph_manager.check_is_item_uploaded(
                                job_id, targetsite_url, mpid)
                            print('is uploaded proudct? : ', is_uploaded)
                            if is_uploaded == False and status != 3:  # upload as new item
                                product, original_product_information = exporter.export_from_mpid_onetime(
                                    job_id, args['execution_id'], mpid, tsid)
                                product['targetsite_url'] = targetsite_url
                                product['mpid'] = mpid
                                log_opertion = 'Create new product'
                                self.cafe24manager.upload_new_product(
                                    product, profiling_info, job_id)
                                cnum = self.graph_manager.get_cnum_from_targetsite_job_configuration_using_tsid(
                                    tsid)
                                # smlee
                            elif is_uploaded == True:
                                if status == 1:
                                    product, original_product_information = exporter.export_from_mpid_onetime(
                                        job_id, args['execution_id'], mpid, tsid)
                                    product['targetsite_url'] = targetsite_url
                                    product['mpid'] = mpid
                                    log_opertion = 'Update exist product'
                                    tpid = self.graph_manager.get_tpid(
                                        job_id, targetsite_url, mpid)
                                    self.cafe24manager.update_exist_product(
                                        product, profiling_info, job_id, tpid)
                                    cnum = self.graph_manager.get_cnum_from_targetsite_job_configuration_using_tsid(
                                        tsid)
                                    # smlee
                                    try:
                                        self.graph_manager.logging_all_uploaded_product(
                                            job_id, args['execution_id'], mpid, original_product_information, product, targetsite_url, cnum)
                                    except:
                                        self.graph_manager.logging_all_uploaded_product(job_id, args['execution_id'], mpid, {
                                                                                        'Error': 'Logging error'}, {'Error': 'Logging error'}, targetsite_url, cnum)
                                        pass
                                elif status == 3:
                                    tpid = self.graph_manager.get_tpid(
                                        job_id, targetsite_url, mpid)
                                    print('tpid : ', tpid)
                                    log_opertion = 'Delete product'
                                    self.cafe24manager.hide_exist_product(
                                        profiling_info, job_id, tpid)
                                    cnum = self.graph_manager.get_cnum_from_targetsite_job_configuration_using_tsid(
                                        tsid)
                                    # smlee
                                    try:
                                        self.graph_manager.logging_all_uploaded_product(job_id, args['execution_id'], mpid, {
                                                                                        'status': '3'}, {'status': '3'}, targetsite_url, cnum)
                                    except:
                                        self.graph_manager.logging_all_uploaded_product(job_id, args['execution_id'], mpid, {
                                                                                        'Error': 'Logging error'}, {'Error': 'Logging error'}, targetsite_url, cnum)
                                        pass

                            self.cafe24manager.refresh()

                        successful_node += 1
                    except:
                        failed_node += 1
                        err_msg = '================================ Operator ================================ \n'
                        err_msg += log_operation + '\n\n'
                        err_msg += '================================ My site product id ================================ \n'
                        err_msg += 'My site product id: ' + \
                            str(log_mpid) + '\n\n'
                        err_msg += '================================ Target site URL ================================ \n'
                        err_msg += 'URL: ' + targetsite_url + '\n\n'
                        err_msg += '================================ STACK TRACE ============================== \n' + \
                            str(traceback.format_exc())
                        self.graph_manager.log_err_msg_of_upload(
                            log_mpid, err_msg, log_mt_history_id)
            self.cafe24manager.close()
            print("Close cafe24 manager (no except)")
        except:
            profiling_info['total_time'] = time.time() - total_time
            # print(profiling_info)
            try:
                self.cafe24manager.close()
            except:
                print("Error in close cafe24 manager")
                print(traceback.format_exc())
            print("Close cafe24 manager (in except)")
            print(traceback.format_exc())
            return profiling_info
        print('s/f', successful_node, '/', failed_node)
        profiling_info['total_time'] = time.time() - total_time
        profiling_info['successful_node'] = successful_node
        profiling_info['failed_node'] = failed_node
        # print(profiling_info)
        return profiling_info


    def hide_products_of_task_from_tpid(self, args):
        try:
            self.cafe24manager = Cafe24Manager(args)
            print("-----------------------Request auth code----------------------")
            self.cafe24manager.get_auth_code()
            print("-----------------------Request token--------------------------")
            self.cafe24manager.get_token()
            
            tpids = self.graph_manager.get_tpid_for_hide()
            cnt = 0
            for tpid in tpids:
                print('tpid : ', tpid[0])
                self.cafe24manager.hide_exist_product_no_profiling(tpid[0])
                cnt = cnt + 1
                if cnt >= 30:
                    self.cafe24manager.refresh()
                    cnt = 0

            self.cafe24manager.close()
            print("Close cafe24 manager (no except)")
        except:
            try:
                self.cafe24manager.close()
            except:
                print("Error in close cafe24 manager")
                print(traceback.format_exc())
            print("Close cafe24 manager (in except)")
            print(traceback.format_exc())
        return

    # smlee
    def close(self):
        try:
            self.cafe24manager.close()
        except:
            pass

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
        # parser.add_argument('transformations')
        args = parser.parse_args()
        if args['req_type'] == 'upload_products':
            return self.upload_products(args)
            # return self.upload_products_of_execution(args)
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
    args['job_id'] = 9999
    #args['clients'] = [
    #    {'client_id': 'oc4Eair8IB7hToJuyjJsiA',
    #        'client_secret': 'EMsUrt4tI1zgSt2i3icPPC'},
    #    {'client_id': 'lmnl9eLRBye5aZvfSU4tXE',
    #        'client_secret': 'nKAquRGpPVsgo6GZkeniLA'},
    #    {'client_id': 'Vw9ygiIAJJLnLDKiAkhsDA',
    #        'client_secret': 'p6EqNWe8DqEHRtxyzP4S4D'},
    #    {'client_id': 'UyLmMdVBOJHvYy0VF4pcpA',
    #        'client_secret': 'dy2CzhMiK9OrLMHyIq37mC'},
    #    {'client_id': 'AafM42MiBie2mB3mRMM0bE',
    #        'client_secret': 'Lv3S8HfvZCxdXifxfb2QMP'},
    #    {'client_id': 'f8rDSAXoWiwPPIBchadCfH',
    #        'client_secret': 'f1yQdvaSN6OLD19qJ1m7oD'},
    #    {'client_id': 'nj0kecRmH6IEn0zFZecHZM',
    #        'client_secret': 'xJ4a9htZGhogr1H2mZNibB'},
    #    {'client_id': 'nP5GWlrOER7kbYVu6QEtGA',
    #        'client_secret': 'mctijmPmOp8lKOaex0VlLF'},
    #    {'client_id': 'LkgQ03ETLtTiCRfmYa5dgD',
    #        'client_secret': '49XJnilLP96vlcKWu8zr8A'},
    #    {'client_id': 'ZIfFO0T6HSHZX4QPpf86EF',
    #        'client_secret': 'oqUEjpMBONgiRMmyE4zAvA'},
    #    {'client_id': 'fDhrr2B1DyQEvuaHUPUD1D',
    #        'client_secret': 'WyCr0qkfHKWWZlWl8fcxiK'},
    #    {'client_id': 'K0JPoImnDJXn8giYecK5yE',
    #        'client_secret': 'TaW51jMeHaZTLveIjfhXe2'},
    #    {'client_id': 'UzhOkGW6H5An6QaYpfMHQA',
    #        'client_secret': 'swcXg2pEFVFjdBanSUfqaC'},
    #    {'client_id': 'ojrikKQeGiBcVkUBybQGYB',
    #        'client_secret': 'zHdUFOUSHZUkRV4QJdoRvD'},
    #    {'client_id': 'avZzMvjjCx4mNz8OOKLjcB',
    #        'client_secret': 'H2OTer0OIi3WZyvS7gYeiP'},
    #    {'client_id': 'ehLMLKGobqVOxsoYgu5W1E',
    #        'client_secret': 'tTWPLjC9IdCp9sMK0j4JKD'}
    #]
    args['clients'] = [
        {'client_id': 'fMQM2ezflI8CEjtf8v0g3J',
            'client_secret': 'hHDJKf1mHKwYJR5Xbe8BSK'},
        {'client_id': 'XFtTxY2KEVfIdKGzWho63F',
            'client_secret': '7VFSpj7VGRblIlOuXZJtiC'},
        {'client_id': 'O96DVOGZCp2rPU5YkJlaLB',
            'client_secret': 'dWITFh8GAtdhMVEybA425C'},
        {'client_id': '7HdXW34HN9YkdZdjfohHfP',
            'client_secret': 'Uzq5NpcsOJErm3kyx6nQFC'},
        {'client_id': 'TMyTyFMSFRwoPJXwIVBBVC',
            'client_secret': 'sCPpVDfCOyRYJta4L4KHZD'},
        {'client_id': 'wyhWZK5iC1FnalpkLKQXyA',
            'client_secret': 'YdO51Vr2HJ6pkXXtafCRDF'},
    ]
    #args['mall_id'] = 'topdepot'
    #args['user_id'] = 'topdepot'
    #args['user_pwd'] = '*wsjy0724*'
    args['mall_id'] = 'noweuro'
    args['user_id'] = 'noweuro'
    args['user_pwd'] = '!wsjy0724!'
    args['redirect_uri'] = 'https://www.google.com'
    args['scope'] = 'mall.write_product mall.read_product mall.read_category mall.write_category mall.read_collection mall.write_collection'
    #cafe24api.upload_products(args)
    cafe24api.hide_products_of_task_from_tpid(args)
    #cafe24api.hide_exist_product(profiling_info, job_id, tpid)
    #app.run(debug=True, host='0.0.0.0', port=5002)
