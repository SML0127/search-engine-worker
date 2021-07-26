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

from functools import partial
print_flushed = partial(print, flush=True)

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
        print_flushed(__class__.__name__)

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


selenium_chrome_erros = ['StaleElementReferenceException', 'WebDriverException', 'TimeoutException']
class BFSIterator(BaseOperator):

    def check_captcha(self, url, gvar):
        try:
            print_flushed("@@@@@@@@ Check captcha (amazon)")
            chaptcha_xpath = '//input[@id=\'captchacharacters\']'  # for amazon
            check_chaptcha = gvar.web_mgr.get_elements_by_selenium_(
                chaptcha_xpath)
            cnt = 0
            max_cnt = 4
            while(len(check_chaptcha) != 0):
                cnt = cnt + 1
                print_flushed('Captcha check cnt: ', cnt)
                link = gvar.web_mgr.get_value_by_selenium(
                    '//form[@action="/errors/validateCaptcha"]//img', 'src')
                print_flushed('Captcha image link = {}'.format(link))
                captcha = AmazonCaptcha.fromlink(link)
                solution = captcha.solve()
                print_flushed('String in image = {}'.format(solution))
                gvar.web_mgr.send_keys_to_elements(
                    '//input[@id="captchacharacters"]', solution)
                gvar.web_mgr.click_elements('//button')
                time.sleep(3)
                gvar.web_mgr.load(url)
                check_chaptcha = gvar.web_mgr.get_elements_by_selenium_(chaptcha_xpath)
                if cnt >= max_cnt:
                    raise
        except:
            print_flushed(str(traceback.format_exc()))
            raise

    def check_captcha_rakuten(self, gvar):
        try:
            print_flushed("@@@@@@@@ Check is blocked (rakuten)")
            chaptcha_xpath = '//body[contains(text(),\'Reference\')]'
            print_flushed("Taksid: {}".format(gvar.task_id))
            fname = '/home/pse/PSE-engine/htmls/%s.html' % str(gvar.task_id)
            gvar.web_mgr.store_page_source(fname)
            if gvar.web_mgr.get_html() == '<html><head></head><body></body></html>':
                print_flushed(gvar.web_mgr.get_html())
            check_chaptcha = gvar.web_mgr.get_elements_by_selenium_(
                chaptcha_xpath)
            sleep_time = 10
            while(len(check_chaptcha) != 0 or gvar.web_mgr.get_html() == '<html><head></head><body></body></html>'):
                print_flushed('@@@@@ Restart chrome')
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
        err_cnt = 0
        err_op_name = "BFSIterator"
        while True:
            print_flushed('@@@@@ Set up before running sub operators in BFSIterator')
            try:
                op_name = "BFSIterator"
                op_id = self.props['id']
                parent_node_id = self.props.get('parent_node_id', 0)
                label = self.props['label']

                print_flushed("@@@@@@@@@@ task url:", gvar.task_url)

                if gvar.task_zipcode_url is not None:
                    print_flushed("@@@@@@@@@@ input zipcode:", gvar.task_zipcode_url)

                src_url = urlparse(gvar.task_url).netloc
                print_flushed("@@@@@@@@@@ src_url:", src_url)


                if 'amazon.de' in src_url:
                    print_flushed(src_url)
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
                    # print_flushed('zipcode = ', site_zipcode)
                    # if site_zipcode is None:
                    #   while True:
                    #     time.sleep(5)
                    #     def interceptor_loop(request):
                    #       request.method = 'POST'
                    #       gvar.web_mgr.get_cur_driver_().request_interceptor = interceptor_loop
                    #     site_zipcode = gvar.web_mgr.get_value_by_selenium('//*[@id="glow-ingress-line2"]', "alltext")
                    #     print_flushed('zipcode = ', site_zipcode)
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
                    #   print_flushed(token)

                    #   url = 'http://www.amazon.de/gp/delivery/ajax/address-change.html?locationType=LOCATION_INPUT&zipCode={}&storeContext=office-products&deviceType=web&pageType=Detail&actionSource=glow&almBrandId=undefined'.format(zipcode)
                    #   def interceptor_de2(request):
                    #     del request.headers['anti-csrftoken-a2z']
                    #     request.headers['user-agent'] = str(random.randrange(1,99999999)) + str(''.join(random.choices(string.ascii_uppercase + string.digits, k=15))) + str(random.randrange(1,99999999))
                    #     request.headers['anti-csrftoken-a2z'] = token
                    #   gvar.web_mgr.get_cur_driver_().request_interceptor = interceptor_de2
                    #
                    #   gvar.web_mgr.load(url)
                    #   print_flushed(gvar.web_mgr.get_html())

                    #   self.check_captcha(url, gvar)
                    #   time.sleep(2)
                    #   gvar.web_mgr.load(gvar.task_url)
                    #   time.sleep(2)
                    #   if gvar.task_url != gvar.web_mgr.get_current_url():
                    #     time.sleep(5)
                    #   site_zipcode = gvar.web_mgr.get_value_by_selenium('//*[@id="glow-ingress-line2"]', "alltext")
                    #   print_flushed('(After apply) zipcode = ', site_zipcode)
                    #   def interceptor_de3(request):
                    #     del request.headers['anti-csrftoken-a2z']
                    #     request.method = 'GET'
                    #   gvar.web_mgr.get_cur_driver_().request_interceptor = interceptor_de3
                #elif 'amazon.com' in src_url:
                elif 'amazon.com' in src_url:
                    gvar.web_mgr.load(gvar.task_url)
                    self.check_captcha(gvar.task_url, gvar)
                elif 'amazon.co.uk' in src_url:
                    gvar.web_mgr.load(gvar.task_url)
                    self.check_captcha(gvar.task_url, gvar)
                elif 'DISABLE-amazon.com' in src_url:
                    print_flushed('@@@@@@@@ Current chrome country = {}, input = {}'.format(
                        gvar.web_mgr.get_cur_driver_zipcode_country(), src_url))
                    if 'amazon.com' not in gvar.web_mgr.get_cur_driver_zipcode_country():
                        print_flushed('@@@@@@@@ Change chrome amazon country zipcode')
                        gvar.web_mgr.set_cur_driver_zipcode_boolean()
                        gvar.web_mgr.change_cur_driver_zipcode_country(src_url)

                    if gvar.web_mgr.get_cur_driver_is_zipcode_reset() == True:
                        print_flushed('@@@@@@@@ Change zipcode (zipcode is reset)')
                        # Get token for zipcode
                        url = "http://www.amazon.com/gp/glow/get-address-selections.html?deviceType=desktop&pageType=Gateway"

                        def interceptor(request):
                            request.method = 'POST'

                        gvar.web_mgr.get_cur_driver_().request_interceptor = interceptor
                        while True:
                            try:
                                gvar.web_mgr.load(url)
                                time.sleep(1)
                                print(gvar.web_mgr.get_html())
                                token = gvar.web_mgr.get_html().split(
                                    'CSRF_TOKEN : "')[1].split('", IDs')[0]
                                print_flushed("@@@@@@@@@@ Token {} ".format(token))
                                break
                            except:
                                sleep_time = 1000 + int(random.randrange(1, 60))
                                print_flushed(
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
                        print_flushed('@@@@@@@@@@ zipcode = ', site_zipcode)
                        if site_zipcode is None:
                            self.check_captcha(gvar.task_url, gvar)
                            site_zipcode = gvar.web_mgr.get_value_by_selenium(
                                '//*[@id="glow-ingress-line2"]', "alltext")
                            print_flushed('@@@@@@@@@@ after captcha zipcode = ', site_zipcode)
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
                                    print_flushed(
                                        '@@@@@@@@@@@ change is zipcode reset as false')
                                    gvar.web_mgr.reset_cur_driver_zipcode_boolean()
                                    break
                                else:
                                    #cnt = cnt + 1
                                    sleep_time = 1000 + \
                                        int(random.randrange(1, 60))
                                    print_flushed(
                                        "@@@@@@@@@@ Retry get token {}s ".format(sleep_time))
                                    time.sleep(sleep_time)
                                    # if cnt < max_cnt:
                                    #  print_flushed('re-request change-address api. {} retry'.format(str(cnt)))
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
                            print_flushed(
                                '@@@@@@@@@@ (After apply, before check captcha) zipcode = ', site_zipcode)
                            self.check_captcha(url, gvar)
                            print_flushed(
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
                        print_flushed('@@@@@@@@@@ Current zipcode = ', site_zipcode)

                #elif 'amazon.co.uk' in src_url:
                elif 'DISABLE-amazon.co.uk' in src_url:
                    print_flushed('@@@@@@@@ Current chrome country = {}, input = {}'.format(
                        gvar.web_mgr.get_cur_driver_zipcode_country(), src_url))
                    if 'amazon.co.uk' not in gvar.web_mgr.get_cur_driver_zipcode_country():
                        print_flushed('@@@@@@@@ Change chrome amazon country zipcode')
                        gvar.web_mgr.set_cur_driver_zipcode_boolean()
                        gvar.web_mgr.change_cur_driver_zipcode_country(src_url)

                    if gvar.web_mgr.get_cur_driver_is_zipcode_reset() == True:
                        print_flushed('@@@@@@@@ Change zipcode (zipcode is reset)')
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
                                print_flushed("@@@@@@@@@@ Token {} ".format(token))
                                break
                            except:
                                sleep_time = 1000 + int(random.randrange(1, 60))
                                print_flushed(
                                    "@@@@@@@@@@ Retry get token {}s ".format(sleep_time))
                                time.sleep(sleep_time)
                                pass

                        # Check zicpode
                        def interceptor5(request):
                            request.method = 'GET'
                        gvar.web_mgr.get_cur_driver_().request_interceptor = interceptor5
                        print_flushed('@@@@@@@@ task_url: {}'.format(gvar.task_url))
                        gvar.web_mgr.load(gvar.task_url)
                        time.sleep(3)
                        site_zipcode = gvar.web_mgr.get_value_by_selenium(
                            '//*[@id="glow-ingress-line2"]', "alltext")
                        zipcode = gvar.graph_mgr.get_zipcode(
                            src_url, gvar.task_zipcode_url)
                        print_flushed('@@@@@@@@@@ zipcode = ', site_zipcode)
                        if site_zipcode is None:
                            self.check_captcha(gvar.task_url, gvar)
                            site_zipcode = gvar.web_mgr.get_value_by_selenium(
                                '//*[@id="glow-ingress-line2"]', "alltext")
                            print_flushed('@@@@@@@@@@ after captcha zipcode = ', site_zipcode)
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
                                    print_flushed(
                                        '@@@@@@@@@@@ change is zipcode reset as false')
                                    gvar.web_mgr.reset_cur_driver_zipcode_boolean()
                                    break
                                else:
                                    #cnt = cnt + 1
                                    sleep_time = 1000 + \
                                        int(random.randrange(1, 60))
                                    print_flushed(
                                        "@@@@@@@@@@ Retry get token {}s ".format(sleep_time))
                                    time.sleep(sleep_time)
                                    # if cnt < max_cnt:
                                    #  print_flushed('re-request change-address api. {} retry'.format(str(cnt)))
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
                            print_flushed(
                                '@@@@@@@@@@ (After apply, before check captcha) zipcode = ', site_zipcode)
                            self.check_captcha(url, gvar)
                            print_flushed(
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
                        print_flushed('@@@@@@@@@@ Current zipcode = ', site_zipcode)

                else:
                    gvar.web_mgr.load(gvar.task_url)
                    if 'rakuten' in src_url:
                        self.check_captcha_rakuten(gvar)
                #######################################

                gvar.web_mgr.build_lxml_tree()
                time.sleep(5)
                # check invalid page
                if 'amazon' in src_url:
                    print_flushed("@@@@@@@@@@ Check invalid page (amazon)")
                    invalid_page_xpath = "//img[@alt='Dogs of Amazon'] | //span[contains(@id,'priceblock_') and contains(text(),'-')]"
                    is_invalid_page = gvar.web_mgr.get_elements_by_lxml_(
                        invalid_page_xpath)
                    if len(is_invalid_page) != 0:
                        print_flushed("@@@@@@ Invalid page")
                        # smlee
                        node_id = gvar.graph_mgr.create_node(gvar.task_id, parent_node_id, label)
                        gvar.stack_nodes.append(node_id)
                        gvar.stack_indices.append(0)
                        gvar.graph_mgr.insert_node_property(gvar.stack_nodes[-1], 'url', gvar.task_url)
                        #gvar.profiling_info[op_id] = {'invalid': True}
                        gvar.profiling_info['invalid'] = True
                        return

                elif 'jomashop' in src_url:
                    wrong_to_rendering_xpath = "//div[@id='react-top-error-boundary'] | //*[contains(text(),'Unable to fetch data')] | //*[contains(text(),'Something went wrong')] | //div[@classname='splash-screen'] | //*[contains(text(),'Data Fetch Error')]"          
                    render_cnt = 0 
                    max_render_cnt = 5
                    while True:
                        print_flushed("@@@@@@@@@@ Check Wrong to rendering page (jomashop)")
                        wrong_to_rendering_page = gvar.web_mgr.get_elements_by_lxml_(wrong_to_rendering_xpath)
                        if len(wrong_to_rendering_page) != 0:
                            render_cnt = render_cnt + 1
                            if render_cnt >= max_render_cnt:
                                break
                            else:
                                gvar.web_mgr.load(gvar.task_url)
                                time.sleep(5)
                                gvar.web_mgr.build_lxml_tree()
                        else:
                            break;

                    print_flushed("@@@@@@@@@@ Check invalid page (jomashop)")
                    #invalid_page_xpath = "//div[@class='image-404'] | //div[@class='product-buttons']//span[contains(text(),'OUT OF STOCK')] | //div[contains(text(),'Sold Out')] | //span[contains(text(),'Ships In')] | //span[contains(text(),'Contact us for')] | //span[contains(text(),'Ships in')] "
                    invalid_page_xpath = "//div[@class='product-buttons']//span[contains(text(),'OUT OF STOCK')] | //div[contains(text(),'Sold Out')] | //span[contains(text(),'Ships In')] | //span[contains(text(),'Contact us for')] | //span[contains(text(),'Ships in')] "
                    invalid_page_xpath = "//div[@class='image-404'] | //*[text()='Unable to fetch data']"
                    is_invalid_page = gvar.web_mgr.get_elements_by_lxml_(
                        invalid_page_xpath)
                    if len(is_invalid_page) != 0:
                        print_flushed("@@@@@@ Invalid page")
                        # smlee
                        node_id = gvar.graph_mgr.create_node(gvar.task_id, parent_node_id, label)
                        gvar.stack_nodes.append(node_id)
                        gvar.stack_indices.append(0)
                        gvar.graph_mgr.insert_node_property(gvar.stack_nodes[-1], 'url', gvar.task_url)
                        #gvar.profiling_info[op_id] = {'invalid': True}
                        gvar.profiling_info['invalid'] = True
                        return

                elif 'zalando' in src_url:
                    print_flushed("@@@@@@@@@@ Check invalid page (zalando)")
                    invalid_page_xpath = "//h2[contains(text(),'Out of stock')] | //h1[contains(text(),'find this page')]"
                    is_invalid_page = gvar.web_mgr.get_elements_by_lxml_(
                        invalid_page_xpath)
                    if len(is_invalid_page) != 0:
                        print_flushed("@@@@@@ Invalid page")
                        # smlee
                        node_id = gvar.graph_mgr.create_node(gvar.task_id, parent_node_id, label)
                        gvar.stack_nodes.append(node_id)
                        gvar.stack_indices.append(0)
                        gvar.graph_mgr.insert_node_property(gvar.stack_nodes[-1], 'url', gvar.task_url)
                        #gvar.profiling_info[op_id] = {'invalid': True}
                        gvar.profiling_info['invalid'] = True
                        return

                if 'query' in self.props:
                    print_flushed(
                        "@@@@@@@@@@ Check invalid page or failure using input xpath in BFSIterator")
                    is_invalid_input = gvar.web_mgr.get_elements_by_lxml_(
                        self.props['query'])
                    if len(is_invalid_input) == 0:
                        if 'is_detail' in self.props:
                            print_flushed("@@@@@@@@@@ Not Detail page, set as a failure")
                            gvar.profiling_info['check_xpath_error'] = True
                            raise CheckXpathError
                        else:
                            print_flushed("@@@@@@@@@@ Detail page, set as a invalid page")
                            node_id = gvar.graph_mgr.create_node(
                                gvar.task_id, parent_node_id, label)
                            gvar.stack_nodes.append(node_id)
                            gvar.stack_indices.append(0)
                            gvar.graph_mgr.insert_node_property(
                                gvar.stack_nodes[-1], 'url', gvar.task_url)
                            return CheckXpathError
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
                    print_flushed('@@@@@@@@@@ btn cur :', res)
                    print_flushed(self.props['page_id'])

                    if str(self.props['page_id']) not in res:
                        print_flushed('@@@@@@@@@@ page number in button != page number in url')
                        gvar.profiling_info['btn_num_error'] = True
                        raise BtnNumError 
                gvar.graph_mgr.insert_node_property(
                    gvar.stack_nodes[-1], 'html', gvar.web_mgr.get_html())

                for op in self.operators:
                    op_name = op.props['name']
                    err_op_name = op_name
                    op.run(gvar)

                op_time = time.time() - op_start
                gvar.profiling_info[op_id] = {'op_time': op_time}
                break;

            except Exception as e:
                print_flushed(e.__class__.__name__)
                if e.__class__.__name__ == 'BtnNumError':
                    raise
                elif e.__class__.__name__ == 'NoneDetailPageError':
                    err_msg = '================================== URL ==================================\n'
                    err_msg += ' ' + str(gvar.task_url) + '\n\n'
                    err_msg += '================================ Opeartor ==================================\n'
                    err_msg += 'Summary page pagination \n\n'
                    err_msg += '================================ Reason ============================== \n'
                    err_msg += 'There is no detail page\n\n'
                    err_msg += '================================ STACK TRACE ============================== \n' + \
                        str(traceback.format_exc())
                    gvar.graph_mgr.log_err_msg_of_task(gvar.task_id, err_msg)
                    raise

                err_cnt = err_cnt + 1
                if err_cnt >= 5:
                    if e.__class__.__name__ == 'CheckXpathError':
                        fname = '/home/pse/PSE-engine/htmls/%s.html' % str(gvar.task_id)
                        gvar.web_mgr.store_page_source(fname)
                        print_flushed("error html:", fname)
                        err_msg = '================================== URL ==================================\n'
                        err_msg += ' ' + str(gvar.task_url) + '\n\n'
                        err_msg += '================================ Opeartor ==================================\n'
                        err_msg += err_op_name + ' \n\n'
                        err_msg += '================================ STACK TRACE ============================== \n' + \
                            str(traceback.format_exc())
                        gvar.graph_mgr.log_err_msg_of_task(gvar.task_id, err_msg)
                        raise
                    else:
                        fname = '/home/pse/PSE-engine/htmls/%s.html' % str(gvar.task_id)
                        gvar.web_mgr.store_page_source(fname)
                        print_flushed("error html:", fname)
                        err_msg = '================================== URL ==================================\n'
                        err_msg += ' ' + str(gvar.task_url) + '\n\n'
                        err_msg += '================================ Opeartor ==================================\n'
                        err_msg += err_op_name + ' \n\n'
                        err_msg += '================================ STACK TRACE ============================== \n' + \
                            str(traceback.format_exc())
                        gvar.graph_mgr.log_err_msg_of_task(gvar.task_id, err_msg)
                        raise OperatorError(e, self.props['id'])

                else:
                    gvar.web_mgr.restart(5)
                    print_flushed('err_cnt : ', err_cnt)



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
            print_flushed(num_elements, query)
            if num_elements == 0 and int(self.props.get('self', 0)) == 1:
                num_elements = 1

            for i in range(num_elements):
                print_flushed(i, "-th loop#############################################")
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
            print_flushed("Do SendPhoneKeys")

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
            print_flushed("Do Wait {} secs".format(self.props.get('wait', 0)))
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
             print_flushed("Do Scroll")
             op_start = time.time()
             gvar.web_mgr.scroll_to_bottom()

             op_time = time.time() - op_start
             gvar.profiling_info[op_id] = {'op_time': op_time}
             return
         except Exception as e:
             if e.__class__.__name__ in selenium_chrome_erros:
             #if e.__class__.__name__ == 'WebDriverException' or e.__class__.__name__ == 'TimeoutException':
                 print_flushed('Chrome Error in ScrollOperator')
                 raise e
             else:
                 fname = '/home/pse/PSE-engine/htmls/%s.html' % str(gvar.task_id)
                 raise OperatorError(e, self.props['id'])
         return


class HoverOperator(BaseOperator):
    def run(self, gvar):
        try:
            op_id = self.props['id']
            print_flushed("Do Hover")
            op_start = time.time()
            xpath = self.props['query']
            gvar.web_mgr.move_to_elements(xpath)

            op_time = time.time() - op_start
            gvar.profiling_info[op_id] = {'op_time': op_time}
            return
        except Exception as e:
            if e.__class__.__name__ in selenium_chrome_erros:
            #if e.__class__.__name__ == 'WebDriverException' or e.__class__.__name__ == 'TimeoutException':
                print_flushed('Chrome Error in HoverOperator')
                raise e
            else:
                fname = '/home/pse/PSE-engine/htmls/%s.html' % str(gvar.task_id)
                raise OperatorError(e, self.props['id'])

        return


class LoginOperator(BaseOperator):
    def run(self, gvar):
        try:
            op_id = self.props['id']
            op_start = time.time()
            print_flushed("before login")
            print_flushed(gvar.web_mgr.get_current_url())
            print_flushed("Do Login")
            gvar.web_mgr.login_by_xpath(self.props["user_id"], self.props["pwd"],
                                        self.props["user_id_query"], self.props["pwd_query"], self.props["click_query"])
            time.sleep(int(self.props.get('delay', 10)))
            op_time = time.time() - op_start
            print_flushed("after login")
            print_flushed(gvar.web_mgr.get_current_url())
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
            print_flushed("Do Input (SendKeys)")
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
            print_flushed("Do Click")
            for column in self.props["queries"]:
                query = column["query"]
                check_query = column.get("check_query",'').strip()
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
                        query, check_query, time_sleep, gvar.task_url)
                else:
                    if essential:
                        gvar.web_mgr.click_elements_strong(query, check_query)
                    else:
                        gvar.web_mgr.click_elements(query, check_query)
                time.sleep(int(column.get('delay', 5)))
            op_time = time.time() - op_start
            gvar.profiling_info[op_id] = {'op_time': op_time}
            return
        except Exception as e:
            if e.__class__.__name__ in selenium_chrome_erros:
            #if e.__class__.__name__ == 'WebDriverException' or e.__class__.__name__ == 'TimeoutException' or e.__class__.__name__ ==  'StaleElementReferenceException':
                print_flushed('Chrome Error in ClickOperator')
                raise e
            else:
                fname = '/home/pse/PSE-engine/htmls/%s.html' % str(gvar.task_id)
                raise OperatorError(e, self.props['id'])

        return


class MoveCursorOperator(BaseOperator):

    def run(self, gvar):
        try:
            op_id = self.props['id']
            op_start = time.time()
            print_flushed("Do MoveCursor")
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
            if e.__class__.__name__ in selenium_chrome_erros:
            #if e.__class__.__name__ == 'WebDriverException' or e.__class__.__name__ == 'TimeoutException':
                print_flushed('Chrome Error in MoveCursorOperator')
                raise e
            else:
                fname = '/home/pse/PSE-engine/htmls/%s.html' % str(gvar.task_id)
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
        if len(result) == 0:
            raise NoneDetailPageError
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
            if e.__class__.__name__ in selenium_chrome_erros:
            #if e.__class__.__name__ == 'WebDriverException' or e.__class__.__name__ == 'TimeoutException':
                print_flushed('Chrome Error in ExpanderOperator')
                raise e
            elif e.__class__.__name__ =='NoneDetailPageError':
                raise e
            else:
                raise OperatorError(e, self.props['id'])


class ValuesScrapper(BaseOperator):

    def before(self, gvar):
        result = {}
        op_time = time.time()
        print_flushed('Do ValuesScrapper')
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
                print_flushed(pair)

                if xpath == '':
                    if attr == 'url':
                        result[key] = str(
                            gvar.web_mgr.get_current_url()).strip()
                else:
                    if 'indices' in pair:
                        print_flushed(xpath)
                        print_flushed(gvar.stack_indices)
                        print_flushed(pair['indices'])
                        xpath = self.set_query(
                            xpath, gvar.stack_indices, pair['indices'])
                    essential = pair.get('essential', False)
                    if type(essential) != type(True):
                        essential = eval(essential)
                    if attr == 'Default Value(constant)':
                        result[key] = xpath
                    else:
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
            if e.__class__.__name__ in selenium_chrome_erros:
            #if e.__class__.__name__ == 'WebDriverException' or e.__class__.__name__ == 'TimeoutException':
                print_flushed('Chrome Error in ValuesScrapper')
                raise e
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
        except Exception as e:
            raise OperatorError(e, self.props['id'])
        return


class ListsScrapper(BaseOperator):

    def run(self, gvar):
        result = {}
        op_time = time.time()
        print_flushed('Do ListsScrapper')
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
            if e.__class__.__name__ in selenium_chrome_erros:
            #if e.__class__.__name__ == 'WebDriverException' or e.__class__.__name__ == 'TimeoutException':
                print_flushed('Chrome Error in ListsScrapper')
                raise e
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
        except Exception as e:
            raise OperatorError(e, self.props['id'])
        return



class DictsScrapper(BaseOperator):

    def run(self, gvar):
        result = {}
        op_time = time.time()
        print_flushed('Do dictionary scrapper')
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
            if e.__class__.__name__ in selenium_chrome_erros:
            #if e.__class__.__name__ == 'WebDriverException' or e.__class__.__name__ == 'TimeoutException':
                print_flushed('Chrome Error in DictionariesScrapper')
                raise e
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
        except Exception as e:
            raise OperatorError(e, self.props['id'])
        return



class OptionListScrapper(BaseOperator):

    def run(self, gvar):
        op_start = time.time()
        print_flushed('Do OptionListScrapper')
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
            if e.__class__.__name__ in selenium_chrome_erros:
            #if e.__class__.__name__ == 'WebDriverException' or e.__class__.__name__ == 'TimeoutException':
                print_flushed('Chrome Error in OptionListScrapper')
                raise e
            else:
                raise OperatorError(e, self.props['id'])

        try:
            db_time = time.time()
            # for key, value in result.items():
            print_flushed(result)
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
        except Exception as e:
            raise OperatorError(e, self.props['id'])
        return


class OptionMatrixScrapper(BaseOperator):

    def run(self, gvar):
        op_start = time.time()
        print_flushed('Do OptionMatrixScrapper')
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
                    result['option_matrix_col_name'] = [option_name]
                elif idx == 1:
                    result[option_name] = option_y_value
                    result['option_matrix_row_name'] = [option_name]

            if len(result) >= 1:
                result['option_maxtrix_value'] = option_combination_value

            xpaths_time = time.time() - xpaths_time

        except Exception as e:
            if e.__class__.__name__ in selenium_chrome_erros:
            #if e.__class__.__name__ == 'WebDriverException' or e.__class__.__name__ == 'TimeoutException':
                print_flushed('Chrome Error in OptionMatrixScrapper')
                raise e
            else:
                raise OperatorError(e, self.props['id'])

        try:
            db_time = time.time()

            print_flushed(result)
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
