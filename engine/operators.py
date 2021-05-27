import string
import time
import traceback
import random

from lxml import etree
from lxml import html
from amazoncaptcha import AmazonCaptcha
from selenium.common.exceptions import TimeoutException, WebDriverException, InvalidSessionIdException

from util.pse_errors import *
from urllib.parse import urlparse


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

    def check_captcha(self, url, gvar):
        try:
            print("@@@@@@@@ Check captcha (amazon)")
            chaptcha_xpath = '//input[@id=\'captchacharacters\']'  # for amazon
            check_chaptcha = gvar.web_mgr.get_elements_by_selenium_(
                chaptcha_xpath)
            cnt = 0
            max_cnt = 3
            while(len(check_chaptcha) != 0):
                link = gvar.web_mgr.get_value_by_selenium(
                    '//form[@action="/errors/validateCaptcha"]//img', 'src')
                print('Captcha image link = {}'.format(link))
                captcha = AmazonCaptcha.fromlink(link)
                solution = captcha.solve()
                print('String in image = {}'.format(solution))
                gvar.web_mgr.send_keys_to_elements(
                    '//input[@id="captchacharacters"]', solution)
                gvar.web_mgr.click_elements('//button')
                time.sleep(5)
                gvar.web_mgr.load(url)
                check_chaptcha = gvar.web_mgr.get_elements_by_selenium_(
                    chaptcha_xpath)
                cnt = cnt + 1
                if cnt >= max_cnt:
                    raise
        except:
            raise

    def check_captcha_rakuten(self, gvar):
        try:
            print("@@@@@@@@ Check is blocked (rakuten)")
            chaptcha_xpath = '//body[contains(text(),\'Reference\')]'
            print("Taksid: {}".format(gvar.task_id))
            fname = '/home/pse/PSE-engine/htmls/%s.html' % str(gvar.task_id)
            gvar.web_mgr.store_page_source(fname)
            if gvar.web_mgr.get_html() == '<html><head></head><body></body></html>':
                print(gvar.web_mgr.get_html())
            check_chaptcha = gvar.web_mgr.get_elements_by_selenium_(
                chaptcha_xpath)
            sleep_time = 10
            while(len(check_chaptcha) != 0 or gvar.web_mgr.get_html() == '<html><head></head><body></body></html>'):
                print('@@@@@ Restart chrome')
                gvar.web_mgr.restart(sleep_time)
                gvar.web_mgr.load(gvar.task_url)
                #gvar.graph_mgr.insert_node_property(gvar.stack_nodes[-1], 'url', gvar.task_url)
                gvar.web_mgr.wait_loading()
                time.sleep(self.props.get('delay', 0))
                sleep_time += 0
                if gvar.task_url != gvar.web_mgr.get_current_url():
                    time.sleep(5)
                check_chaptcha = gvar.web_mgr.get_elements_by_selenium_(
                    chaptcha_xpath)
        except:
            raise

    def run(self, gvar):

        op_start = time.time()

        # Set up before running sub operators
        while True:
            print('@@@@@ Set up before running sub operators in BFSIterator')
            try:
                op_name = "BFSIterator"
                op_id = self.props['id']
                parent_node_id = self.props.get('parent_node_id', 0)
                label = self.props['label']

                print("@@@@@@@@@@ task url:", gvar.task_url)

                if gvar.task_zipcode_url is not None:
                    print("@@@@@@@@@@ input zipcode:", gvar.task_zipcode_url)

                src_url = urlparse(gvar.task_url).netloc
                print("@@@@@@@@@@ src_url:", src_url)


                if 'amazon.de' in src_url:
                    print(src_url)
                    # def interceptor_load(request):
                    #   request.method = 'POST'
                    #   request.headers['user-agent'] = str(random.randrange(1,99999999)) + str(''.join(random.choices(string.ascii_uppercase + string.digits, k=15))) + str(random.randrange(1,99999999))
                    # gvar.web_mgr.get_cur_driver_().request_interceptor = interceptor_load
                    # gvar.web_mgr.load(gvar.task_url)
                    # time.sleep(1)
                    # if gvar.task_url != gvar.web_mgr.get_current_url():
                    #   time.sleep(5)

                    # site_zipcode = gvar.web_mgr.get_value_by_selenium('//*[@id="glow-ingress-line2"]', "alltext")
                    # zipcode = gvar.graph_mgr.get_zipcode(src_url, gvar.task_zipcode_url)
                    # print('zipcode = ', site_zipcode)
                    # if site_zipcode is None:
                    #   while True:
                    #     time.sleep(5)
                    #     def interceptor_loop(request):
                    #       request.method = 'POST'
                    #       gvar.web_mgr.get_cur_driver_().request_interceptor = interceptor_loop
                    #     site_zipcode = gvar.web_mgr.get_value_by_selenium('//*[@id="glow-ingress-line2"]', "alltext")
                    #     print('zipcode = ', site_zipcode)
                    #     if site_zipcode is not None: break;
                    # if zipcode not in site_zipcode:
                    #   url = "http://www.amazon.de/gp/glow/get-address-selections.html?deviceType=desktop&pageType=Gateway"

                    #   def interceptor_de(request):
                    #     request.method = 'POST'
                    #     request.headers['user-agent'] = str(random.randrange(1,99999999)) + str(''.join(random.choices(string.ascii_uppercase + string.digits, k=15))) + str(random.randrange(1,99999999))
                    #   gvar.web_mgr.get_cur_driver_().request_interceptor = interceptor_de
                    #   gvar.web_mgr.load(url)
                    #   time.sleep(1)
                    #   self.check_captcha(url, gvar)

                    #   token = gvar.web_mgr.get_html().split('CSRF_TOKEN : "')[1].split('", IDs')[0]
                    #   print(token)

                    #   url = 'http://www.amazon.de/gp/delivery/ajax/address-change.html?locationType=LOCATION_INPUT&zipCode={}&storeContext=office-products&deviceType=web&pageType=Detail&actionSource=glow&almBrandId=undefined'.format(zipcode)
                    #   def interceptor_de2(request):
                    #     del request.headers['anti-csrftoken-a2z']
                    #     request.headers['user-agent'] = str(random.randrange(1,99999999)) + str(''.join(random.choices(string.ascii_uppercase + string.digits, k=15))) + str(random.randrange(1,99999999))
                    #     request.headers['anti-csrftoken-a2z'] = token
                    #   gvar.web_mgr.get_cur_driver_().request_interceptor = interceptor_de2
                    #
                    #   gvar.web_mgr.load(url)
                    #   print(gvar.web_mgr.get_html())

                    #   self.check_captcha(url, gvar)
                    #   time.sleep(2)
                    #   gvar.web_mgr.load(gvar.task_url)
                    #   time.sleep(2)
                    #   if gvar.task_url != gvar.web_mgr.get_current_url():
                    #     time.sleep(5)
                    #   site_zipcode = gvar.web_mgr.get_value_by_selenium('//*[@id="glow-ingress-line2"]', "alltext")
                    #   print('(After apply) zipcode = ', site_zipcode)
                    #   def interceptor_de3(request):
                    #     del request.headers['anti-csrftoken-a2z']
                    #     request.method = 'GET'
                    #   gvar.web_mgr.get_cur_driver_().request_interceptor = interceptor_de3
                elif 'amazon.com' in src_url:
                    print('@@@@@@@@ Current chrome country = {}, input = {}'.format(
                        gvar.web_mgr.get_cur_driver_zipcode_country(), src_url))
                    if 'amazon.com' not in gvar.web_mgr.get_cur_driver_zipcode_country():
                        print('@@@@@@@@ Change chrome amazon country zipcode')
                        gvar.web_mgr.set_cur_driver_zipcode_boolean()
                        gvar.web_mgr.change_cur_driver_zipcode_country(src_url)

                    if gvar.web_mgr.get_cur_driver_is_zipcode_reset() == True:
                        print('@@@@@@@@ Change zipcode (zipcode is reset)')
                        # Get token for zipcode
                        url = "http://www.amazon.com/gp/glow/get-address-selections.html?deviceType=desktop&pageType=Gateway"

                        def interceptor(request):
                            request.method = 'POST'

                        gvar.web_mgr.get_cur_driver_().request_interceptor = interceptor
                        while True:
                            try:
                                gvar.web_mgr.load(url)
                                time.sleep(1)
                                token = gvar.web_mgr.get_html().split(
                                    'CSRF_TOKEN : "')[1].split('", IDs')[0]
                                print("@@@@@@@@@@ Token {} ".format(token))
                                break
                            except:
                                sleep_time = 1000 + int(random.randrange(1, 60))
                                print(
                                    "@@@@@@@@@@ Retry get token {}s ".format(sleep_time))
                                time.sleep(sleep_time)
                                pass

                        # Check zicpode
                        gvar.web_mgr.load(gvar.task_url)
                        time.sleep(3)
                        site_zipcode = gvar.web_mgr.get_value_by_selenium(
                            '//*[@id="glow-ingress-line2"]', "alltext")
                        zipcode = gvar.graph_mgr.get_zipcode(
                            src_url, gvar.task_zipcode_url)
                        print('@@@@@@@@@@ zipcode = ', site_zipcode)
                        if site_zipcode is None:
                            self.check_captcha(gvar.task_url, gvar)
                            site_zipcode = gvar.web_mgr.get_value_by_selenium(
                                '//*[@id="glow-ingress-line2"]', "alltext")
                            print('@@@@@@@@@@ after captcha zipcode = ', site_zipcode)
                        if zipcode not in site_zipcode:
                            url = 'http://www.amazon.com/gp/delivery/ajax/address-change.html?locationType=LOCATION_INPUT&zipCode={}&storeContext=office-products&deviceType=web&pageType=Detail&actionSource=glow&almBrandId=undefined'.format(
                                zipcode)

                            def interceptor2(request):
                                del request.headers['anti-csrftoken-a2z']
                                request.headers['anti-csrftoken-a2z'] = token
                            gvar.web_mgr.get_cur_driver_().request_interceptor = interceptor2
                            #max_cnt = 10
                            #cnt = 0
                            while True:
                                gvar.web_mgr.load(url)
                                time.sleep(1)
                                if '"isValidAddress":1' in gvar.web_mgr.get_html():
                                    print(
                                        '@@@@@@@@@@@ change is zipcode reset as false')
                                    gvar.web_mgr.reset_cur_driver_zipcode_boolean()
                                    break
                                else:
                                    #cnt = cnt + 1
                                    sleep_time = 1000 + \
                                        int(random.randrange(1, 60))
                                    print(
                                        "@@@@@@@@@@ Retry get token {}s ".format(sleep_time))
                                    time.sleep(sleep_time)
                                    # if cnt < max_cnt:
                                    #  print('re-request change-address api. {} retry'.format(str(cnt)))
                                    # else:
                                    #  break;
                            # gvar.web_mgr.load(url)
                            # time.sleep(2)
                            gvar.web_mgr.load(gvar.task_url)
                            time.sleep(2)
                            if gvar.task_url != gvar.web_mgr.get_current_url():
                                time.sleep(5)
                            site_zipcode = gvar.web_mgr.get_value_by_selenium(
                                '//*[@id="glow-ingress-line2"]', "alltext")
                            print(
                                '@@@@@@@@@@ (After apply, before check captcha) zipcode = ', site_zipcode)
                            self.check_captcha(url, gvar)
                            print(
                                '@@@@@@@@@@ (After apply, before check captcha) zipcode = ', site_zipcode)

                            def interceptor3(request):
                                del request.headers['anti-csrftoken-a2z']
                                request.method = 'GET'
                            gvar.web_mgr.get_cur_driver_().request_interceptor = interceptor3
                        ####################################
                    else:
                        gvar.web_mgr.load(gvar.task_url)
                        time.sleep(2)
                        if gvar.task_url != gvar.web_mgr.get_current_url():
                            time.sleep(5)
                        self.check_captcha(gvar.task_url, gvar)
                        site_zipcode = gvar.web_mgr.get_value_by_selenium(
                            '//*[@id="glow-ingress-line2"]', "alltext")
                        print('@@@@@@@@@@ Current zipcode = ', site_zipcode)

                elif 'amazon.co.uk' in src_url:
                    print('@@@@@@@@ Current chrome country = {}, input = {}'.format(
                        gvar.web_mgr.get_cur_driver_zipcode_country(), src_url))
                    if 'amazon.co.uk' not in gvar.web_mgr.get_cur_driver_zipcode_country():
                        print('@@@@@@@@ Change chrome amazon country zipcode')
                        gvar.web_mgr.set_cur_driver_zipcode_boolean()
                        gvar.web_mgr.change_cur_driver_zipcode_country(src_url)

                    if gvar.web_mgr.get_cur_driver_is_zipcode_reset() == True:
                        print('@@@@@@@@ Change zipcode (zipcode is reset)')
                        # Get token for zipcode
                        url = "http://www.amazon.co.uk/gp/glow/get-address-selections.html?deviceType=desktop&pageType=Gateway"

                        def interceptor(request):
                            request.method = 'POST'
                        gvar.web_mgr.get_cur_driver_().request_interceptor = interceptor
                        while True:
                            try:
                                gvar.web_mgr.load(url)
                                time.sleep(1)
                                token = gvar.web_mgr.get_html().split(
                                    'CSRF_TOKEN : "')[1].split('", IDs')[0]
                                print("@@@@@@@@@@ Token {} ".format(token))
                                break
                            except:
                                sleep_time = 1000 + int(random.randrange(1, 60))
                                print(
                                    "@@@@@@@@@@ Retry get token {}s ".format(sleep_time))
                                time.sleep(sleep_time)
                                pass

                        # Check zicpode
                        def interceptor5(request):
                            request.method = 'GET'
                        gvar.web_mgr.get_cur_driver_().request_interceptor = interceptor5
                        print('@@@@@@@@ task_url: {}'.format(gvar.task_url))
                        gvar.web_mgr.load(gvar.task_url)
                        time.sleep(3)
                        site_zipcode = gvar.web_mgr.get_value_by_selenium(
                            '//*[@id="glow-ingress-line2"]', "alltext")
                        zipcode = gvar.graph_mgr.get_zipcode(
                            src_url, gvar.task_zipcode_url)
                        print('@@@@@@@@@@ zipcode = ', site_zipcode)
                        if site_zipcode is None:
                            self.check_captcha(gvar.task_url, gvar)
                            site_zipcode = gvar.web_mgr.get_value_by_selenium(
                                '//*[@id="glow-ingress-line2"]', "alltext")
                            print('@@@@@@@@@@ after captcha zipcode = ', site_zipcode)
                        if zipcode not in site_zipcode:
                            url = 'http://www.amazon.co.uk/gp/delivery/ajax/address-change.html?locationType=LOCATION_INPUT&zipCode={}&storeContext=office-products&deviceType=web&pageType=Detail&actionSource=glow&almBrandId=undefined'.format(
                                zipcode)

                            def interceptor2(request):
                                del request.headers['anti-csrftoken-a2z']
                                request.headers['anti-csrftoken-a2z'] = token
                            gvar.web_mgr.get_cur_driver_().request_interceptor = interceptor2
                            #max_cnt = 10
                            #cnt = 0
                            while True:
                                gvar.web_mgr.load(url)
                                time.sleep(1)
                                if '"isValidAddress":1' in gvar.web_mgr.get_html():
                                    print(
                                        '@@@@@@@@@@@ change is zipcode reset as false')
                                    gvar.web_mgr.reset_cur_driver_zipcode_boolean()
                                    break
                                else:
                                    #cnt = cnt + 1
                                    sleep_time = 1000 + \
                                        int(random.randrange(1, 60))
                                    print(
                                        "@@@@@@@@@@ Retry get token {}s ".format(sleep_time))
                                    time.sleep(sleep_time)
                                    # if cnt < max_cnt:
                                    #  print('re-request change-address api. {} retry'.format(str(cnt)))
                                    # else:
                                    #  break;
                            # gvar.web_mgr.load(url)
                            # time.sleep(2)
                            gvar.web_mgr.load(gvar.task_url)
                            time.sleep(2)
                            if gvar.task_url != gvar.web_mgr.get_current_url():
                                time.sleep(5)
                            site_zipcode = gvar.web_mgr.get_value_by_selenium(
                                '//*[@id="glow-ingress-line2"]', "alltext")
                            print(
                                '@@@@@@@@@@ (After apply, before check captcha) zipcode = ', site_zipcode)
                            self.check_captcha(url, gvar)
                            print(
                                '@@@@@@@@@@ (After apply, before check captcha) zipcode = ', site_zipcode)

                            def interceptor3(request):
                                del request.headers['anti-csrftoken-a2z']
                                request.method = 'GET'
                            gvar.web_mgr.get_cur_driver_().request_interceptor = interceptor3
                        ####################################
                    else:
                        gvar.web_mgr.load(gvar.task_url)
                        time.sleep(2)
                        if gvar.task_url != gvar.web_mgr.get_current_url():
                            time.sleep(5)
                        self.check_captcha(gvar.task_url, gvar)
                        site_zipcode = gvar.web_mgr.get_value_by_selenium(
                            '//*[@id="glow-ingress-line2"]', "alltext")
                        print('@@@@@@@@@@ Current zipcode = ', site_zipcode)

                else:
                    gvar.web_mgr.load(gvar.task_url)
                    if 'rakuten' in src_url:
                        self.check_captcha_rakuten(gvar)
                #######################################

                gvar.web_mgr.build_lxml_tree()
                time.sleep(5)
                # check invalid page
                if 'amazon' in src_url:
                    print("@@@@@@@@@@ Check invalid page (amazon)")
                    invalid_page_xpath = "//img[@alt='Dogs of Amazon'] | //span[contains(@id,'priceblock_') and contains(text(),'-')]"
                    is_invalid_page = gvar.web_mgr.get_elements_by_lxml_(
                        invalid_page_xpath)
                    if len(is_invalid_page) != 0:
                        print("@@@@@@ Invalid page")
                        return

                elif 'jomashop' in src_url:
                    print("@@@@@@@@@@ Check invalid page (jomashop)")
                    invalid_page_xpath = "//div[@class='image-404'] | //div[@class='product-buttons']//span[contains(text(),'OUT OF STOCK')] | //div[contains(text(),'Sold Out')] | //span[contains(text(),'Ships In')] | //span[contains(text(),'Contact us for')] | //*[text()='Unable to fetch data'] | //span[contains(text(),'Ships in')] "
                    is_invalid_page = gvar.web_mgr.get_elements_by_lxml_(
                        invalid_page_xpath)
                    if len(is_invalid_page) != 0:
                        print("@@@@@@ Invalid page")
                        return

                elif 'zalando' in src_url:
                    print("@@@@@@@@@@ Check invalid page (zalando)")
                    invalid_page_xpath = "//h2[contains(text(),'Out of stock')] | //h1[contains(text(),'find this page')]"
                    is_invalid_page = gvar.web_mgr.get_elements_by_lxml_(
                        invalid_page_xpath)
                    if len(is_invalid_page) != 0:
                        print("@@@@@@ Invalid page")
                        return

                if 'query' in self.props:
                    print(
                        "@@@@@@@@@@ Check invalid page or failure using input xpath in BFSIterator")
                    is_invalid_input = gvar.web_mgr.get_elements_by_lxml_(
                        self.props['query'])
                    if len(is_invalid_input) == 0:
                        if 'is_detail' in self.props:
                            print("@@@@@@@@@@ Not Detail page, set as a failure")
                            raise
                        else:
                            print("@@@@@@@@@@ Detail page, set as a invalid page")
                            node_id = gvar.graph_mgr.create_node(
                                gvar.task_id, parent_node_id, label)
                            gvar.stack_nodes.append(node_id)
                            gvar.stack_indices.append(0)
                            gvar.graph_mgr.insert_node_property(
                                gvar.stack_nodes[-1], 'url', gvar.task_url)
                            return
                #######################################

                node_id = gvar.graph_mgr.create_node(
                    gvar.task_id, parent_node_id, label)
                gvar.stack_nodes.append(node_id)
                gvar.stack_indices.append(0)
                gvar.graph_mgr.insert_node_property(
                    gvar.stack_nodes[-1], 'url', gvar.task_url)

                if 'btn_query' in self.props and int(self.props['page_id']) != 1:
                    res = gvar.web_mgr.get_value_by_lxml_strong(
                        self.props['btn_query'], 'alltext')
                    print('@@@@@@@@@@ btn cur :', res)
                    print(self.props['page_id'])

                    if str(self.props['page_id']) not in res:
                        print('@@@@@@@@@@ page number in button != page number in url')
                        raise
                gvar.graph_mgr.insert_node_property(
                    gvar.stack_nodes[-1], 'html', gvar.web_mgr.get_html())
                break;
            except Exception as e:
                if e.__class__.__name__ == 'WebDriverException' or e.__class__.__name__ == 'TimeoutException:':
                    print('Chrome Error. Restart chrome')
                    print(str(traceback.format_exc()))
                    gvar.web_mgr.restart(5)
                else:
                    fname = '/home/pse/PSE-engine/htmls/%s.html' % str(gvar.task_id)
                    gvar.web_mgr.store_page_source(fname)
                    print("error html:", fname)
                    err_msg = '================================== URL ==================================\n'
                    err_msg += ' ' + str(gvar.task_url) + '\n\n'
                    err_msg += '=============================== Opeartor ==================================\n'
                    err_msg += ' BFSIterator \n\n'
                    err_msg += '================================ STACK TRACE ============================== \n' + \
                        str(traceback.format_exc())
                    gvar.graph_mgr.log_err_msg_of_task(gvar.task_id, err_msg)
                    raise OperatorError(e, self.props['id'])

        try:
            for op in self.operators:
                op_name = op.props['name']
                op.run(gvar)

            op_time = time.time() - op_start
            gvar.profiling_info[op_id] = {'op_time': op_time}

        except Exception as e:
            fname = '/home/pse/PSE-engine/htmls/%s.html' % str(gvar.task_id)
            gvar.web_mgr.store_page_source(fname)
            print("error html:", fname)
            err_msg = '================================== URL ==================================\n'
            err_msg += ' ' + str(gvar.task_url) + '\n\n'
            err_msg += '=============================== Opeartor ==================================\n'
            err_msg += ' ' + str(op_name) + '\n\n'
            err_msg += '================================ STACK TRACE ============================== \n' + \
                str(traceback.format_exc())
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
                query = self.set_query(
                    query, gvar.stack_indices, self.props['indices'])

            essential = self.props.get("essential", False)
            if type(essential) != type(True):
                essential = eval(essential)
            if essential:
                elements = gvar.web_mgr.get_elements_by_selenium_strong_(query)
            else:
                elements = gvar.web_mgr.get_elements_by_selenium_(query)

            num_elements = len(elements)
            print(num_elements, query)
            if num_elements == 0 and int(self.props.get('self', 0)) == 1:
                num_elements = 1

            for i in range(num_elements):
                print(i, "-th loop#############################################")
                node_id = gvar.graph_mgr.create_node(
                    gvar.task_id, parent_node_id, label)
                gvar.stack_nodes.append(node_id)
                gvar.stack_indices.append(i)
                for op in self.operators:
                    op.run(gvar)
                gvar.stack_nodes.pop()
                gvar.stack_indices.pop()
            op_time = time.time() - op_start
            gvar.profiling_info[op_id] = {'op_time': op_time}
            return
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
            gvar.web_mgr.end_keys_to_elements_strong(query, number)
            time.sleep(int(column.get('delay', 0)))

            op_time = time.time() - op_start
            gvar.profiling_info[op_id] = {'op_time': op_time}
            return
        except Exception as e:
            raise OperatorError(e, self.props['id'])
            return
        return


class WaitOperator(BaseOperator):
    def run(self, gvar):
        try:
            op_id = self.props['id']
            print("Do Wait {} secs".format(self.props.get('wait', 0)))
            time.sleep(int(self.props.get('wait', 0)))
            return
        except Exception as e:
            raise OperatorError(e, self.props['id'])
            return
        return


class ScrollOperator(BaseOperator):
    def run(self, gvar):
        try:
            op_id = self.props['id']
            print("Do Scroll")
            op_start = time.time()
            gvar.web_mgr.scroll_to_bottom()

            op_time = time.time() - op_start
            gvar.profiling_info[op_id] = {'op_time': op_time}
            return
        except Exception as e:
            fname = '/home/pse/PSE-engine/htmls/%s.html' % str(gvar.task_id)
            raise OperatorError(e, self.props['id'])
            return
        return


class HoverOperator(BaseOperator):
    def run(self, gvar):
        try:
            op_id = self.props['id']
            print("Do Hover")
            op_start = time.time()
            xpath = self.props['query']
            gvar.web_mgr.move_to_elements(xpath)

            op_time = time.time() - op_start
            gvar.profiling_info[op_id] = {'op_time': op_time}
            return
        except Exception as e:
            fname = '/home/pse/PSE-engine/htmls/%s.html' % str(gvar.task_id)
            raise OperatorError(e, self.props['id'])
            return
        return


class LoginOperator(BaseOperator):
    def run(self, gvar):
        try:
            op_id = self.props['id']
            op_start = time.time()
            print("before login")
            print(gvar.web_mgr.get_current_url())
            print("Do Login")
            gvar.web_mgr.login_by_xpath(self.props["user_id"], self.props["pwd"],
                                        self.props["user_id_query"], self.props["pwd_query"], self.props["click_query"])
            time.sleep(int(self.props.get('delay', 10)))
            op_time = time.time() - op_start
            print("after login")
            print(gvar.web_mgr.get_current_url())
            fname = '/home/pse/PSE-engine/htmls/test.html'
            gvar.web_mgr.store_page_source(fname)
            gvar.profiling_info[op_id] = {'op_time': op_time}
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
            gvar.profiling_info[op_id] = {'op_time': op_time}
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
                    query = self.set_query(
                        query, gvar.stack_indices, column['indices'])
                essential = column.get("essential", False)
                repeat = column.get("repeat", False)
                if type(essential) != type(True):
                    essential = eval(essential)
                if type(repeat) != type(True):
                    repeat = eval(repeat)
                if repeat:
                    gvar.web_mgr.click_elements_repeat(
                        query, time_sleep, gvar.task_url)
                else:
                    if essential:
                        gvar.web_mgr.click_elements_strong(query)
                    else:
                        gvar.web_mgr.click_elements(query)
                time.sleep(int(column.get('delay', 5)))
            op_time = time.time() - op_start
            gvar.profiling_info[op_id] = {'op_time': op_time}
            return
        except Exception as e:
            raise OperatorError(e, self.props['id'])
            return
        return


class MoveCursorOperator(BaseOperator):

    def run(self, gvar):
        try:
            op_id = self.props['id']
            op_start = time.time()
            print("Do MoveCursor")
            for column in self.props["queries"]:
                query = column["query"]
                if 'indices' in column:
                    query = self.set_query(
                        query, gvar.stack_indices, column['indices'])
                essential = column.get("essential", False)
                if type(essential) != type(True):
                    essential = eval(essential)
                if essential:
                    gvar.web_mgr.move_to_elements_strong(query)
                else:
                    gvar.web_mgr.move_to_elements(query)
            op_time = time.time() - op_start
            gvar.profiling_info[op_id] = {'op_time': op_time}
        except Exception as e:
            raise OperatorError(e, self.props['id'])


class Expander(BaseOperator):

    def run_0(self, gvar):
        op_start = time.time()
        op_id = self.props['id']
        gvar.results[op_id] = [
            (gvar.task_id, gvar.stack_nodes[-1], [gvar.web_mgr.get_current_url()])]
        op_time = time.time() - op_start
        gvar.profiling_info[op_id] = {'op_time': op_time}

    def run_1(self, gvar):

        op_start = time.time()

        op_id = self.props['id']
        query = self.props['query']
        if 'indices' in self.props:
            query = self.set_query(
                query, gvar.stack_indices, self.props['indices'])
        attr = self.props["attr"]

        site = self.props.get("prefix", None)
        attr_delimiter = self.props.get("attr_delimiter", None)
        attr_idx = self.props.get("attr_idx", None)
        suffix = self.props.get("suffix", "")
        self_url = self.props.get('matchSelf', False)
        if type(self_url) != type(True):
            self_url = eval(self_url)
        no_matching_then_self = self.props.get('noMatchSelf', False)
        if type(no_matching_then_self) != type(True):
            no_matching_then_self = eval(no_matching_then_self)
        cur_url = gvar.web_mgr.get_current_url()

        xpaths_time = time.time()
        result = gvar.web_mgr.get_values_by_selenium(query, attr)
        #result = gvar.web_mgr.get_values_by_lxml(query, attr)
        xpaths_time = time.time() - xpaths_time

        # if url_query is not None:
        #  for idx, res in enumerate(result):
        #    result[idx] = int(result[idx])
        #  if len(result) == 0:
        #    if no_matching_then_self == 1: result = [gvar.web_mgr.get_current_url()]
        #  else:
        #    for idx, res in enumerate(result):
        #      result[idx] = cur_url.split('?')[0] + (url_query % int(result[idx]))
        # else:
        if attr_delimiter is not None:
            for idx, res in enumerate(result):
                result[idx] = result[idx].split(attr_delimiter)[
                    attr_idx] + str(suffix)
            if len(result) == 0:
                self_url = 1
                if no_matching_then_self == 1:
                    result = [gvar.web_mgr.get_current_url()]
            else:
                self_url = 0

        if site is not None:
            if len(result) == 0:
                if no_matching_then_self == 1:
                    result = [gvar.web_mgr.get_current_url()]
                # else:
                #  essential = self.props.get("essential", False)
                #  if type(essential) != type(True): essential = eval(essential)
                #  if essential: raise
            else:
                for idx, res in enumerate(result):
                    result[idx] = str(site) + str(res)
                if self_url == 1:
                    result.append(gvar.web_mgr.get_current_url())
        else:
            if len(result) == 0:
                if no_matching_then_self == 1:
                    result = [gvar.web_mgr.get_current_url()]
                # else:
                #  essential = self.props.get("essential", False)
                #  if type(essential) != type(True): essential = eval(essential)
                #  if essential: raise
            else:
                if self_url == 1:
                    result.append(gvar.web_mgr.get_current_url())

        gvar.results[op_id] = [(gvar.task_id, gvar.stack_nodes[-1], result)]
        op_time = time.time() - op_start
        gvar.profiling_info[op_id] = {
            'op_time': op_time,
            'xpaths_time': xpaths_time,
            'num_elements':  len(result)
        }
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
                    if attr == 'url':
                        result[key] = str(
                            gvar.web_mgr.get_current_url()).strip()
                else:
                    if 'indices' in pair:
                        print(xpath)
                        print(gvar.stack_indices)
                        print(pair['indices'])
                        xpath = self.set_query(
                            xpath, gvar.stack_indices, pair['indices'])
                    essential = pair.get('essential', False)
                    if type(essential) != type(True):
                        essential = eval(essential)
                    if attr == 'outerHTML':
                        if essential:
                            result[key] = gvar.web_mgr.get_subtree_with_style_strong(
                                xpath)
                        else:
                            result[key] = gvar.web_mgr.get_subtree_with_style(
                                xpath)
                        continue
                    if attr == 'innerHTML':
                        if essential:
                            result[key] = gvar.web_mgr.get_subtree_no_parent_with_style_strong(
                                xpath)
                        else:
                            result[key] = gvar.web_mgr.get_subtree_no_parent_with_style(
                                xpath)
                        continue
                    if essential:
                        result[key] = gvar.web_mgr.get_value_by_lxml_strong(
                            xpath, attr)
                    else:
                        result[key] = gvar.web_mgr.get_value_by_lxml(
                            xpath, attr)
            xpaths_time = time.time() - xpaths_time
            db_time = time.time()
            for key, value in result.items():
                gvar.graph_mgr.insert_node_property(
                    gvar.stack_nodes[-1], key, value)
            db_time = time.time() - db_time

            op_time = time.time() - op_time
            gvar.profiling_info[op_id] = {
                'op_time': op_time,
                'build_time': build_time,
                'xpaths_num': len(pairs),
                'xpaths_time': xpaths_time,
                'db_num': len(result),
                'db_time': db_time
            }
            return
        except Exception as e:
            raise OperatorError(e, self.props['id'])



class ValuesScrapperNew(BaseOperator):

    def before(self, gvar):
        while True:
            result = {}
            op_time = time.time()
            print('Do ValuesScrapper')
            op_id = self.props['id']
            pairs = self.props['queries']
            xpaths_time = ''
            build_time = ''
            try:

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
                        if attr == 'url':
                            result[key] = str(
                                gvar.web_mgr.get_current_url()).strip()
                    else:
                        if 'indices' in pair:
                            print(xpath)
                            print(gvar.stack_indices)
                            print(pair['indices'])
                            xpath = self.set_query(
                                xpath, gvar.stack_indices, pair['indices'])
                        essential = pair.get('essential', False)
                        if type(essential) != type(True):
                            essential = eval(essential)
                        if attr == 'outerHTML':
                            if essential:
                                result[key] = gvar.web_mgr.get_subtree_with_style_strong(
                                    xpath)
                            else:
                                result[key] = gvar.web_mgr.get_subtree_with_style(
                                    xpath)
                            continue
                        if attr == 'innerHTML':
                            if essential:
                                result[key] = gvar.web_mgr.get_subtree_no_parent_with_style_strong(
                                    xpath)
                            else:
                                result[key] = gvar.web_mgr.get_subtree_no_parent_with_style(
                                    xpath)
                            continue
                        if essential:
                            result[key] = gvar.web_mgr.get_value_by_lxml_strong(
                                xpath, attr)
                        else:
                            result[key] = gvar.web_mgr.get_value_by_lxml(
                                xpath, attr)
                xpaths_time = time.time() - xpaths_time
            except Exception as e:
                if e.__class__.__name__ == 'WebDriverException' or e.__class__.__name__ == 'TimeoutException:':
                    print('Chrome Error. Restart chrome')
                    print(str(traceback.format_exc()))
                    gvar.web_mgr.restart(5)
                else:
                    raise OperatorError(e, self.props['id'])
            
            try:
                db_time = time.time()
                for key, value in result.items():
                    gvar.graph_mgr.insert_node_property(
                        gvar.stack_nodes[-1], key, value)
                db_time = time.time() - db_time

                op_time = time.time() - op_time
                gvar.profiling_info[op_id] = {
                    'op_time': op_time,
                    'build_time': build_time,
                    'xpaths_num': len(pairs),
                    'xpaths_time': xpaths_time,
                    'db_num': len(result),
                    'db_time': db_time
                }
                break;
            except Exception as e:
                raise OperatorError(e, self.props['id'])
        return

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
                    xpath = self.set_query(
                        xpath, gvar.stack_indices, query['indices'])
                attr = query['attr']
                essential = query.get('essential', False)
                if type(essential) != type(True):
                    essential = eval(essential)
                if essential:
                    result[key] = gvar.web_mgr.get_values_by_lxml_strong(
                        xpath, attr)
                else:
                    result[key] = gvar.web_mgr.get_values_by_lxml(xpath, attr)

            xpaths_time = time.time() - xpaths_time

            db_time = time.time()
            for key, value in result.items():
                gvar.graph_mgr.insert_node_property(
                    gvar.stack_nodes[-1], key, value)
            db_time = time.time() - db_time

            op_time = time.time() - op_time
            gvar.profiling_info[op_id] = {
                'op_time': op_time,
                'build_time': build_time,
                'xpaths_time': xpaths_time,
                'db_time': db_time,
                'num_results': len(result)
            }
            return
        except Exception as e:
            raise OperatorError(e, self.props['id'])
            return
        return


class ListsScrapperNew(BaseOperator):

    def run(self, gvar):
        while True:
            result = {}
            op_time = time.time()
            print('Do ListsScrapper')
            op_id = self.props['id']
            queries = self.props['queries']
            xpaths_time = ''
            build_time = ''
            try:

                build_time = time.time()
                gvar.web_mgr.build_lxml_tree()
                build_time = time.time() - build_time

                xpaths_time = time.time()

                for query in queries:
                    key = query['key']
                    xpath = query['query']
                    if 'indices' in query:
                        xpath = self.set_query(
                            xpath, gvar.stack_indices, query['indices'])
                    attr = query['attr']
                    essential = query.get('essential', False)
                    if type(essential) != type(True):
                        essential = eval(essential)
                    if essential:
                        result[key] = gvar.web_mgr.get_values_by_lxml_strong(
                            xpath, attr)
                    else:
                        result[key] = gvar.web_mgr.get_values_by_lxml(xpath, attr)

                xpaths_time = time.time() - xpaths_time
            except Exception as e:
                if e.__class__.__name__ == 'WebDriverException' or e.__class__.__name__ == 'TimeoutException:':
                    print('Chrome Error. Restart chrome')
                    print(str(traceback.format_exc()))
                    gvar.web_mgr.restart(5)
                else:
                    raise OperatorError(e, self.props['id'])
            try: 
                db_time = time.time()
                for key, value in result.items():
                    gvar.graph_mgr.insert_node_property(
                        gvar.stack_nodes[-1], key, value)
                db_time = time.time() - db_time

                op_time = time.time() - op_time
                gvar.profiling_info[op_id] = {
                    'op_time': op_time,
                    'build_time': build_time,
                    'xpaths_time': xpaths_time,
                    'db_time': db_time,
                    'num_results': len(result)
                }
                break;
            except Exception as e:
                raise OperatorError(e, self.props['id'])
        return


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
                    rows_query = self.set_query(
                        rows_query, gvar.stack_indices, query['rows_indices'].strip())
                key_query = query['key_query']
                if 'key_indices' in query:
                    key_query = self.set_query(
                        key_query, gvar.stack_indices, query['key_indices'].strip())
                key_attr = query['key_attr']
                value_query = query['value_query']
                if 'value_indices' in query:
                    value_query = self.set_query(
                        value_query, gvar.stack_indices, query['value_indices'].strip())
                value_attr = query['value_attr']

                essential = query.get('essential', False)
                if type(essential) != type(True):
                    essential = eval(essential)

                if essential:
                    result[key] = gvar.web_mgr.get_key_values_by_lxml_strong(
                        rows_query, key_query, key_attr, value_query, value_attr)
                else:
                    result[key] = gvar.web_mgr.get_key_values_by_lxml(
                        rows_query, key_query, key_attr, value_query, value_attr)
                title_query = query['title_query']
                result[key]['dictionary_title0'] = gvar.web_mgr.get_value_by_lxml(
                    title_query, 'alltext')

            xpaths_time = time.time() - xpaths_time
 
            db_time = time.time()
            for key, value in result.items():
                gvar.graph_mgr.insert_node_property(
                    gvar.stack_nodes[-1], key, value)
            db_time = time.time() - db_time

            op_time = time.time() - op_time
            gvar.profiling_info[op_id] = {
                'op_time': op_time,
                'build_time': build_time,
                'xpaths_time': xpaths_time,
                'db_time': db_time,
                'num_results': len(result)
            }
            return
        except Exception as e:
            raise OperatorError(e, self.props['id'])
        return

class DictsScrapperNew(BaseOperator):

    def run(self, gvar):
        while True:
            result = {}
            op_time = time.time()
            print('Do dictionary scrapper')
            op_id = self.props['id']
            queries = self.props['queries']
            xpaths_time = ''
            build_time = ''
            try:

                build_time = time.time()
                gvar.web_mgr.build_lxml_tree()
                build_time = time.time() - build_time

                result = {}

                xpaths_time = time.time()

                for query in queries:
                    key = query['key']
                    rows_query = query['rows_query']
                    if 'rows_indices' in query:
                        rows_query = self.set_query(
                            rows_query, gvar.stack_indices, query['rows_indices'].strip())
                    key_query = query['key_query']
                    if 'key_indices' in query:
                        key_query = self.set_query(
                            key_query, gvar.stack_indices, query['key_indices'].strip())
                    key_attr = query['key_attr']
                    value_query = query['value_query']
                    if 'value_indices' in query:
                        value_query = self.set_query(
                            value_query, gvar.stack_indices, query['value_indices'].strip())
                    value_attr = query['value_attr']

                    essential = query.get('essential', False)
                    if type(essential) != type(True):
                        essential = eval(essential)

                    if essential:
                        result[key] = gvar.web_mgr.get_key_values_by_lxml_strong(
                            rows_query, key_query, key_attr, value_query, value_attr)
                    else:
                        result[key] = gvar.web_mgr.get_key_values_by_lxml(
                            rows_query, key_query, key_attr, value_query, value_attr)
                    title_query = query['title_query']
                    result[key]['dictionary_title0'] = gvar.web_mgr.get_value_by_lxml(
                        title_query, 'alltext')

                xpaths_time = time.time() - xpaths_time
            except Exception as e:
                if e.__class__.__name__ == 'WebDriverException' or e.__class__.__name__ == 'TimeoutException:':
                    print('Chrome Error. Restart chrome')
                    print(str(traceback.format_exc()))
                    gvar.web_mgr.restart(5)
                else:
                    raise OperatorError(e, self.props['id'])
            
            try:
                db_time = time.time()
                for key, value in result.items():
                    gvar.graph_mgr.insert_node_property(
                        gvar.stack_nodes[-1], key, value)
                db_time = time.time() - db_time

                op_time = time.time() - op_time
                gvar.profiling_info[op_id] = {
                    'op_time': op_time,
                    'build_time': build_time,
                    'xpaths_time': xpaths_time,
                    'db_time': db_time,
                    'num_results': len(result)
                }
                break;
            except Exception as e:
                raise OperatorError(e, self.props['id'])
        return


class OptionListScrapper(BaseOperator):

    def run(self, gvar):
        try:
            op_start = time.time()
            print('Do OptionListScrapper')
            op_id = self.props['id']
            parent_node_id = gvar.stack_nodes[-1]

            option_name_query = self.props['option_name_query']
            option_dropdown_query = self.props['option_dropdown_query']
            option_value_query = self.props['option_value_query']

            build_time = time.time()
            gvar.web_mgr.build_lxml_tree()
            build_time = time.time() - build_time

            xpaths_time = time.time()

            option_names = gvar.web_mgr.get_values_by_lxml(
                option_name_query, 'alltext')
            option_values = gvar.web_mgr.get_option_values_by_lxml(
                option_dropdown_query, option_value_query, 'alltext')

            result = {}
            for idx, option_name in enumerate(option_names):
                try:
                    result[option_name] = option_values[idx]
                except:
                    pass

            xpaths_time = time.time() - xpaths_time

            db_time = time.time()
            # for key, value in result.items():
            print(result)
            gvar.graph_mgr.insert_node_property(
                gvar.stack_nodes[-1], 'option_list', result)
            db_time = time.time() - db_time

            op_time = time.time() - op_start
            gvar.profiling_info[op_id] = {
                'op_time': op_time,
                'build_time': build_time,
                'xpaths_time': xpaths_time,
                'db_time': db_time,
                'num_results': len(result)
            }
            return
        except Exception as e:
            raise OperatorError(e, self.props['id'])
            return
        return



class OptionListScrapperNew(BaseOperator):

    def run(self, gvar):
        while True:
            op_start = time.time()
            print('Do OptionListScrapper')
            op_id = self.props['id']
            parent_node_id = gvar.stack_nodes[-1]
            xpaths_time = ''
            build_time = ''
            result = {}
            try:

                option_name_query = self.props['option_name_query']
                option_dropdown_query = self.props['option_dropdown_query']
                option_value_query = self.props['option_value_query']

                build_time = time.time()
                gvar.web_mgr.build_lxml_tree()
                build_time = time.time() - build_time

                xpaths_time = time.time()

                option_names = gvar.web_mgr.get_values_by_lxml(
                    option_name_query, 'alltext')
                option_values = gvar.web_mgr.get_option_values_by_lxml(
                    option_dropdown_query, option_value_query, 'alltext')

                for idx, option_name in enumerate(option_names):
                    try:
                        result[option_name] = option_values[idx]
                    except:
                        pass

                xpaths_time = time.time() - xpaths_time
            except Exception as e:
                if e.__class__.__name__ == 'WebDriverException' or e.__class__.__name__ == 'TimeoutException:':
                    print('Chrome Error. Restart chrome')
                    print(str(traceback.format_exc()))
                    gvar.web_mgr.restart(5)
                else:
                    raise OperatorError(e, self.props['id'])

            try:
                db_time = time.time()
                # for key, value in result.items():
                print(result)
                gvar.graph_mgr.insert_node_property(
                    gvar.stack_nodes[-1], 'option_list', result)
                db_time = time.time() - db_time

                op_time = time.time() - op_start
                gvar.profiling_info[op_id] = {
                    'op_time': op_time,
                    'build_time': build_time,
                    'xpaths_time': xpaths_time,
                    'db_time': db_time,
                    'num_results': len(result)
                }
                break;
            except Exception as e:
                raise OperatorError(e, self.props['id'])
        return

class OptionMatrixScrapper(BaseOperator):

    def run(self, gvar):
        try:
            op_start = time.time()
            print('Do OptionListScrapper')
            op_id = self.props['id']
            parent_node_id = gvar.stack_nodes[-1]

            option_name_query = self.props['option_name_query']
            option_col_query = self.props['option_x_value_query']
            option_row_query = self.props['option_y_value_query']
            option_combination_value_query = self.props['option_matrix_row_wise_value_query']

            build_time = time.time()
            gvar.web_mgr.build_lxml_tree()
            build_time = time.time() - build_time
            xpaths_time = time.time()

            option_names = gvar.web_mgr.get_values_by_lxml(
                option_name_query, 'alltext')
            option_col_value = gvar.web_mgr.get_values_by_lxml(
                option_col_query, 'alltext')
            option_row_value = gvar.web_mgr.get_values_by_lxml(
                option_row_query, 'alltext')
            option_combination_value = gvar.web_mgr.get_values_by_lxml(
                option_combination_value_query, 'alltext')

            result = {}
            for idx, option_name in enumerate(option_names):
                if idx == 0:
                    result[option_name] = option_col_value
                    result['option_matrix_col_name'] = [option_name]
                elif idx == 1:
                    result[option_name] = option_row_value
                    result['option_matrix_row_name'] = [option_name]

            if len(result) >= 1:
                result['option_maxtrix_value'] = option_combination_value

            xpaths_time = time.time() - xpaths_time

            db_time = time.time()

            print(result)
            gvar.graph_mgr.insert_node_property(
                gvar.stack_nodes[-1], 'option_matrix', result)
            db_time = time.time() - db_time

            op_time = time.time() - op_start
            result_len = 0
            if len(result) >= 1:
                result_len = len(result) - 1

            gvar.profiling_info[op_id] = {
                'op_time': op_time,
                'build_time': build_time,
                'xpaths_time': xpaths_time,
                'db_time': db_time,
                'num_results': result_len
            }
            return
        except Exception as e:
            raise OperatorError(e, self.props['id'])
            return
        return



class OptionMatrixScrapperNew(BaseOperator):

    def run(self, gvar):
        while True:
            op_start = time.time()
            print('Do OptionListScrapper')
            op_id = self.props['id']
            parent_node_id = gvar.stack_nodes[-1]
            xpaths_time = ''
            build_time = ''
            result = {}
            try:

                option_name_query = self.props['option_name_query']
                option_x_query = self.props['option_x_value_query']
                option_y_query = self.props['option_y_value_query']
                option_combination_value_query = self.props['option_matrix_row_wise_value_query']

                build_time = time.time()
                gvar.web_mgr.build_lxml_tree()
                build_time = time.time() - build_time
                xpaths_time = time.time()

                option_names = gvar.web_mgr.get_values_by_lxml(
                    option_name_query, 'alltext')
                option_x_value = gvar.web_mgr.get_values_by_lxml(
                    option_x_query, 'alltext')
                option_y_value = gvar.web_mgr.get_values_by_lxml(
                    option_y_query, 'alltext')
                option_combination_value = gvar.web_mgr.get_values_by_lxml(
                    option_combination_value_query, 'alltext')

                for idx, option_name in enumerate(option_names):
                    if idx == 0:
                        result[option_name] = option_x_value
                    elif idx == 1:
                        result[option_name] = option_y_value

                if len(result) >= 1:
                    result['option_maxtrix_value'] = option_combination_value

                xpaths_time = time.time() - xpaths_time

            except Exception as e:
                if e.__class__.__name__ == 'WebDriverException' or e.__class__.__name__ == 'TimeoutException:':
                    print('Chrome Error. Restart chrome')
                    print(str(traceback.format_exc()))
                    gvar.web_mgr.restart(5)
                else:
                    raise OperatorError(e, self.props['id'])


            try:
                db_time = time.time()

                print(result)
                gvar.graph_mgr.insert_node_property(
                    gvar.stack_nodes[-1], 'option_matrix', result)
                db_time = time.time() - db_time

                op_time = time.time() - op_start
                result_len = 0
                if len(result) >= 1:
                    result_len = len(result) - 1

                gvar.profiling_info[op_id] = {
                    'op_time': op_time,
                    'build_time': build_time,
                    'xpaths_time': xpaths_time,
                    'db_time': db_time,
                    'num_results': result_len
                }
                break;
            except Exception as e:
                raise OperatorError(e, self.props['id'])
        return


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
    'OptionListScrapper': OptionListScrapper,
    'OptionMatrixScrapper': OptionMatrixScrapper,
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
    gvar.graph_mgr.connect(
        "host=141.223.197.36 port=5434 user=smlee password=smlee dbname=pse")
    gvar.web_mgr = WebManager()
    gvar.task_id = 0
    gvar.exec_id = 0
    gvar.task_url = "https://www.amazon.com/Sensodyne-Pronamel-Whitening-Strengthening-Toothpaste/dp/B0762LYFKP?pf_rd_p=9dbbfba7-e756-51ca-b790-09e9b92beee1&pf_rd_r=EG4J8ZAJZNB9B3HBQ9G1&pd_rd_wg=W8hx6&ref_=pd_gw_ri&pd_rd_w=kynj4&pd_rd_r=6365323e-7c16-4273-a2c5-5d85b04565f5"
    gvar.task_zipcode_url = "https://www.amazon.com/gp/delivery/ajax/address-change.html?locationType=LOCATION_INPUT&zipCode=94024&storeContext=offce-products&deviceType=web&pageType=detail&actionSource=glow"
    bfs_iterator = BFSIterator()
    bfs_iterator.props = {'id': 1, 'query': "//span[@id='productTitle']"}
    bfs_iterator.run(gvar)
