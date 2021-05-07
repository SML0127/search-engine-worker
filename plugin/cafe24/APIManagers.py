import sys
import os
import json
import base64
import requests
import cfscrape
import pathlib
import time
import urllib.request
from furl import furl
from selenium import webdriver
from PIL import Image
from io import StringIO
from io import BytesIO
from managers.graph_manager import GraphManager
from managers.settings_manager import *


class Cafe24Manager:

    def __init__(self, args):
        self.mall_id = args['mall_id']
        self.user_id = args['user_id']
        self.user_pwd = args['user_pwd']
        #self.client_id = args['client_id']
        #self.client_secret = args['client_secret']
        self.redirect_uri = args['redirect_uri']
        self.scope = args['scope']
        self.setting_manager = SettingsManager()
        self.setting_manager.setting(
            "/home/pse/PSE-engine/settings-worker.yaml")
        settings = self.setting_manager.get_settings()
        self.graph_manager = GraphManager()
        self.graph_manager.init(settings)
        client = self.graph_manager.get_client(self.mall_id, args['job_id'])
        self.client_id = ""
        self.client_secret = ""
        if client is not None:
            self.client_id = client[0]
            self.client_secret = client[1]
        else:
            while client is None:
                client = self.graph_manager.get_client(
                    self.mall_id, args['job_id'])
                if client is not None:
                    self.client_id = client[0]
                    self.client_secret = client[1]
                    break
                print('Waiting for available client id, secret .....')
                time.sleep(10)
            #self.graph_manager.connect("dbname='pse' user='pse' host='127.0.0.1' port='5432' password='pse'")
        self.brands = {}
        self.manufacturers = {}
        print(client)
        self.connected = True

    def close(self):
        # smlee
        if self.connected == True:
            print('Close APIManager, so return client id, secret')
            self.graph_manager.return_client(
                self.client_id, self.client_secret)
            self.graph_manager.disconnect()
            self.connected = False

    def get_auth_code(self):
        option = webdriver.ChromeOptions()
        option.add_argument('--headless')
        option.add_argument('--disable-gpu')
        option.add_argument('--no-sandbox')
        option.add_argument('--disable-dev-shm-usage')
        option.add_argument('--user-agent={}'.format(
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/61.0.3163.100 Safari/537.36"))
        driver = webdriver.Chrome(
            './web_drivers/chromedriver', chrome_options=option)
        url = 'https://{}.cafe24api.com/api/v2/oauth/authorize?response_type=code&client_id={}&state=test&redirect_uri={}&scope={}'.format(
            self.mall_id, self.client_id, self.redirect_uri, self.scope)
        print(url)

        # driver.get(url)
        #cur_url = driver.current_url
        #login_url = 'https://ec'
        #agreement_url = 'https://{}'.format(self.mall_id)

        cnt = 0
        max_try = 3
        while cnt < max_try:
            cnt = cnt + 1
            print("=============== Try get auth code {} time ==============".format(cnt))
            try:
                driver.get(url)
                cur_url = driver.current_url
                login_url = 'https://ec'
                agreement_url = 'https://{}'.format(self.mall_id)
                print(cur_url)
                if (cur_url[0:len(login_url)] == login_url):
                    print('try to log in')
                    inputElement = driver.find_element_by_id("mall_id")
                    inputElement.send_keys(self.user_id)
                    print('user_id:', self.user_id)
                    inputElement = driver.find_element_by_id("userpasswd")
                    inputElement.send_keys(self.user_pwd)
                    time.sleep(3)
                    print('user_pwd:', self.user_pwd)
                    inputElement = driver.find_element_by_class_name('mButton')
                    inputElement.click()
                    time.sleep(5)
                    cur_url = driver.current_url

                # Check is change password page
                if (cur_url == 'https://user.cafe24.com/comLogin/?action=comForce&req=hosting'):
                    print('@@@@@@@@@@@ Please change password')
                    inputElement = driver.find_element_by_class_name('btnEm')
                    inputElement.click()
                    time.sleep(5)
                    cur_url = driver.current_url

                if (cur_url[0:len(agreement_url)] == agreement_url):
                    print('try to agree')
                    time.sleep(1)
                    print(cur_url)
                    if driver.current_url[0:len('google')] != 'google':
                        inputElement = driver.find_element_by_class_name(
                            'btnSubmit')
                        inputElement.click()
                        time.sleep(5)
                        try:
                            cur_url = driver.current_url
                        except:
                            pass

                if (cur_url == 'https://user.cafe24.com/comLogin/?action=comAuth&req=hosting'):
                    page_source = driver.execute_script(
                        "return document.body.innerHTML")
                cur_url = driver.current_url
                self.auth_code = furl(cur_url).args['code']
                break
            except:
                if cnt < max_try:
                    print('There is no auth code')
                    print('URL: ', driver.current_url)
                    pass
                else:
                    driver.quit()
                    raise

        driver.quit()

    def do_post(self, url, data, headers):
        cnt = 0
        max_try = 3
        while cnt < max_try:
            print("=============== Try do post {} time ==============".format(cnt))
            try:
                response = requests.request(
                    "POST", url, data=data, headers=headers)
                response = json.loads(response.text)
                print(response)
                while 'error' in response and type(response['error']) == type({}) and response['error'].get('code', 0) == 429:
                    response = requests.request(
                        "POST", url, data=data, headers=headers)
                    response = json.loads(response.text)
                    print(response)
                if 'error' in response:
                    print(response)
                return response
            except:
                if cnt < max_try:
                    cnt = cnt + 1
                    pass
                else:
                    raise

    def do_put(self, url, data, headers):
        cnt = 0
        max_try = 3
        while cnt < max_try:
            print("=============== Try do put {} time ==============".format(cnt))
            try:
                response = requests.request(
                    "PUT", url, data=data, headers=headers)
                print(response)
                response = json.loads(response.text)
                while 'error' in response and response['error']['code'] == 429:
                    response = requests.request(
                        "PUT", url, data=data, headers=headers)
                    print(response)
                    response = json.loads(response.text)
                return response
            except:
                if cnt < max_try:
                    cnt = cnt + 1
                    pass
                else:
                    raise

    def do_get(self, url, headers):
        cnt = 0
        max_try = 3
        while cnt < max_try:
            print("=============== Try do get {} time ==============".format(cnt))
            try:
                response = requests.request("GET", url, headers=headers)
                print(response)
                response = json.loads(response.text)
                while 'error' in response and response['error']['code'] == 429:
                    response = requests.request("GET", url, headers=headers)
                    response = json.loads(response.text)
                return response
            except:
                if cnt < max_try:
                    cnt = cnt + 1
                    pass
                else:
                    raise

    def get_token(self):
        auth = (self.client_id + ':' + self.client_secret).encode('ascii')
        auth = 'Basic ' + str(base64.b64encode(auth))[2:-1]
        url = 'https://{}.cafe24api.com/api/v2/oauth/token'.format(
            self.mall_id)

        headers = {
            'Authorization': auth,
            'Content-Type': "application/x-www-form-urlencoded",
        }

        data = {
            'grant_type': 'authorization_code',
            'code': self.auth_code,
            'redirect_uri': self.redirect_uri
        }

        response = self.do_post(url, data, headers)
        # print(response)
        self.token = response['access_token']
        self.refresh_token = response['refresh_token']
        #print('get_token', self.token, self.refresh_token)

    def refresh(self):
        cnt = 0
        max_try = 3
        while cnt < max_try:
            print("=============== Try refresh token {} time ==============".format(cnt))
            time.sleep(10)
            try:
                auth = (self.client_id + ':' +
                        self.client_secret).encode('ascii')
                auth = 'Basic ' + str(base64.b64encode(auth))[2:-1]
                url = 'https://{}.cafe24api.com/api/v2/oauth/token'.format(
                    self.mall_id)

                headers = {
                    'Authorization': auth,
                    'Content-Type': "application/x-www-form-urlencoded"
                }

                data = {
                    'grant_type': 'refresh_token',
                    'refresh_token': self.refresh_token
                }

                response = self.do_post(url, data, headers)

                self.token = response['access_token']
                self.refresh_token = response['refresh_token']
                break
                #print('refresh token', self.token, self.refresh_token)
            except:
                if cnt < max_try:
                    cnt = cnt + 1
                    pass
                else:
                    raise

    def upload_image(self, image):
        url = "https://{}.cafe24api.com/api/v2/admin/products/images".format(
            self.mall_id)
        headers = {
            'Content-Type': "application/json",
            'Authorization': "Bearer {}".format(self.token),
            'Accept-Encoding': "gzip, deflate",
            'Connection': "keep-alive",
        }
        data = {
            "requests": [{
                "image": image
            }]
        }
        # print(url)
        # print(headers)
        # print(data)
        response = self.do_post(url, json.dumps(data), headers)

        try:
            image_path = response['images'][0]['path']
            print(response['images'])
            print(image_path)
        except Exception as e:
            print(e)
            print(response)
            raise e

        if 'cafe24.com' in image_path:
            print(
                image_path[len('http://{}.cafe24.com'.format(self.mall_id)):])
            return image_path[len('http://{}.cafe24.com'.format(self.mall_id)):]
        else:
            print(image_path[len('http://{}.shop'.format(self.mall_id)):])
            return image_path[len('http://{}.shop'.format(self.mall_id)):]

    def upload_image_from_file(self, fpath):
        imgFile = open(fpath, 'rb')
        image = imgFile.read()
        return self.upload_image(image)

    def upload_image_from_link(self, link):
        cnt = 0
        max_try = 3
        while cnt < max_try:
            print(
                "=============== Try download & upload detail image {} time ==============".format(cnt))
            try:
                image = self.get_image_from_link(link)
                #print("upload_image_from_link: ", image)
                return self.upload_image(image)
            except:
                if cnt < max_try:
                    cnt = cnt + 1
                    pass
                else:
                    raise

    def get_image_from_link(self, link):
        if link[:12] == 'https://cdn2':
            scraper = cfscrape.create_scraper()
            r = scraper.get(link)
            u = r.content
            im = Image.open(BytesIO(u))

        elif link[:len("data:image/webp;base64,")] == "data:image/webp;base64,":
            im = link[len("data:image/webp;base64,"):]
            dimage = BytesIO()
            im = Image.open(BytesIO(base64.b64decode(im))).convert(
                "RGB").save(dimage, "JPEG")
            u = dimage.getvalue()
        elif link[:len("data:image/jpeg;base64,")] == "data:image/jpeg;base64,":
            im = link[len("data:image/jpeg;base64,"):]
            dimage = BytesIO()
            im = Image.open(BytesIO(base64.b64decode(im))).convert(
                "RGB").save(dimage, "JPEG")
            u = dimage.getvalue()
        else:
            headers = {
                'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Ubuntu Chromium/79.0.3945.36 Chrome/79.0.3945.36 Safari/537.36',
                "Accept-Encoding": "gzip, deflate, br",
                "Accept": "*/*"
            }
            u = requests.request("GET", link, headers=headers)

            u = u.content

        u = str(base64.b64encode(u))
        return u[2:-1]

    def delete_image(self, tpid):
        url = "https://{}.cafe24api.com/api/v2/admin/products/{}/images".format(
            self.mall_id, tpid)
        headers = {
            'Content-Type': "application/json",
            'Authorization': "Bearer {}".format(self.token),
            'Accept-Encoding': "gzip, deflate",
            'Connection': "keep-alive",
        }

        response = requests.request("DELETE", url, headers=headers)
        response = json.loads(response.text)
        return response

    def delete_product(self, tpid):
        url = "https://{}.cafe24api.com/api/v2/admin/products/{}".format(
            self.mall_id, tpid)
        headers = {
            'Content-Type': "application/json",
            'Authorization': "Bearer {}".format(self.token),
            'Accept-Encoding': "gzip, deflate",
            'Connection': "keep-alive",
        }

        response = requests.request("DELETE", url, headers=headers)
        response = json.loads(response.text)
        return response

    def delete_option(self, tpid):
        url = "https://{}.cafe24api.com/api/v2/admin/products/{}/options".format(
            self.mall_id, tpid)
        headers = {
            'Content-Type': "application/json",
            'Authorization': "Bearer {}".format(self.token),
            'Accept-Encoding': "gzip, deflate",
            'Connection': "keep-alive",
        }
        response = requests.request("DELETE", url, headers=headers)

        return response

    def get_option(self, tpid):
        url = "https://{}.cafe24api.com/api/v2/admin/products/{}/options".format(
            self.mall_id, tpid)
        headers = {
            'Content-Type': "application/json",
            'Authorization': "Bearer {}".format(self.token),
            'Accept-Encoding': "gzip, deflate",
            'Connection': "keep-alive",
        }

        response = self.do_get(url, headers)
        return response

    def get_product(self, tpid):
        url = "https://{}.cafe24api.com/api/v2/admin/products/{}".format(
            self.mall_id, tpid)
        headers = {
            'Content-Type': "application/json",
            'Authorization': "Bearer {}".format(self.token),
            'Accept-Encoding': "gzip, deflate",
            'Connection': "keep-alive",
        }

        response = self.do_get(url, headers)
        return response

    def update_product(self, args, tpid):
        url = "https://{}.cafe24api.com/api/v2/admin/products/{}".format(
            self.mall_id, tpid)
        headers = {
            'Content-Type': "application/json",
            'Authorization': "Bearer {}".format(self.token),
            'Accept-Encoding': "gzip, deflate",
            'Connection': "keep-alive",
        }
        data = {
            'shop_no': 1,
            'request': args
        }
        response = self.do_put(url, json.dumps(data), headers)
        return response

    def create_product(self, args):
        url = "https://{}.cafe24api.com/api/v2/admin/products".format(
            self.mall_id)
        headers = {
            'Content-Type': "application/json",
            'Authorization': "Bearer {}".format(self.token),
            'Accept-Encoding': "gzip, deflate",
            'Connection': "keep-alive",
        }
        data = {
            'shop_no': 1,
            'request': args
        }
        response = self.do_post(url, json.dumps(data), headers)
        return response

    def create_brand(self, args):
        cnt = 0
        max_try = 3
        while cnt < max_try:
            print(
                "=============== Try create brand code {} time ==============".format(cnt))
            try:
                url = "https://{}.cafe24api.com/api/v2/admin/brands".format(
                    self.mall_id)
                headers = {
                    'Authorization': "Bearer {}".format(self.token),
                    'Content-Type': "application/json",
                    'Accept-Encoding': "gzip, deflate",
                    'Connection': "keep-alive",
                }
                data = {
                    'shop_no': 1,
                    'request': args
                }
                response = self.do_post(url, json.dumps(data), headers)
                if 'brand' not in response:
                    print(response)
                brand = response['brand']
                self.brands[brand['brand_name']] = brand['brand_code']
                return brand['brand_code']
            except:
                if cnt < max_try:
                    cnt = cnt + 1
                    pass
                else:
                    raise

    def create_manufacturer(self, args):
        cnt = 0
        max_try = 3
        while cnt < max_try:
            print(
                "=============== Try create manufacturer code {} time ==============".format(cnt))
            try:
                url = "https://{}.cafe24api.com/api/v2/admin/manufacturers".format(
                    self.mall_id)
                headers = {
                    'Authorization': "Bearer {}".format(self.token),
                    'Content-Type': "application/json",
                    'Accept-Encoding': "gzip, deflate",
                    'Connection': "keep-alive",
                }
                data = {
                    'shop_no': 1,
                    'request': args
                }
                response = self.do_post(url, json.dumps(data), headers)
                if 'manufacturer' not in response:
                    print(response)
                manufacturer = response['manufacturer']
                self.manufacturers[manufacturer['manufacturer_name']
                                   ] = manufacturer['manufacturer_code']
                return manufacturer['manufacturer_code']
            except:
                if cnt < max_try:
                    cnt = cnt + 1
                    pass
                else:
                    raise

    def list_brands(self):
        url = "https://{}.cafe24api.com/api/v2/admin/brands".format(
            self.mall_id)
        headers = {
            'Authorization': "Bearer {}".format(self.token),
            'Content-Type': "application/json",
            'Accept-Encoding': "gzip, deflate",
            'Connection': "keep-alive",
        }
        response = self.do_get(url, headers)
        for brand in response['brands']:
            self.brands[brand['brand_name']] = brand['brand_code']
        return response['brands']

    def create_category(self, args):
        url = "https://{}.cafe24api.com/api/v2/admin/categories".format(
            self.mall_id)
        headers = {
            'Authorization': "Bearer {}".format(self.token),
            'Content-Type': "application/json",
            'Accept-Encoding': "gzip, deflate",
            'Connection': "keep-alive",
        }
        data = {
            'shop_no': 1,
            'request': args
        }
        response = self.do_post(url, json.dumps(data), headers)
        # print(response)
        return response['category']['category_no']

    def list_categories(self):
        url = "https://{}.cafe24api.com/api/v2/admin/categories".format(
            self.mall_id)
        headers = {
            'Authorization': "Bearer {}".format(self.token),
            'Content-Type': "application/json",
            'Accept-Encoding': "gzip, deflate",
            'Connection': "keep-alive",
        }

        response = self.do_get(url, headers)
        return response['categories']

    def create_additional_images(self, product_no, links):
        if len(links) == 0:
            return None

        additional_image = []
        for link in links:
            additional_image.append(self.get_image_from_link(link))

        url = "https://{}.cafe24api.com/api/v2/admin/products/{}/additionalimages".format(
            self.mall_id, product_no)
        headers = {
            'Authorization': "Bearer {}".format(self.token),
            'Content-Type': "application/json",
            'Accept-Encoding': "gzip, deflate",
            'Connection': "keep-alive",
        }

        data = {
            'shop_no': 1,
            'request': {
                "additional_image": additional_image
            }
        }
        response = self.do_post(url, json.dumps(data), headers)
        # print(response)
        return response

    def update_additional_images(self, product_no, links):
        if len(links) == 0:
            return None
        cnt = 0
        max_try = 3
        while cnt < max_try:
            print(
                "=============== Try download & upload additional images {} time ==============".format(cnt))
            try:
                additional_image = []
                for link in links:
                    additional_image.append(self.get_image_from_link(link))

                url = "https://{}.cafe24api.com/api/v2/admin/products/{}/additionalimages".format(
                    self.mall_id, product_no)
                headers = {
                    'Authorization': "Bearer {}".format(self.token),
                    'Content-Type': "application/json",
                    'Accept-Encoding': "gzip, deflate",
                    'Connection': "keep-alive",
                }

                data = {
                    'shop_no': 1,
                    'request': {
                        "additional_image": additional_image
                    }
                }
                response = self.do_put(url, json.dumps(data), headers)
                # print(response)
                return response
            except:
                if cnt < max_try:
                    cnt = cnt + 1
                    pass
                else:
                    raise

    def create_option(self, product_no, option):
        url = "https://{}.cafe24api.com/api/v2/admin/products/{}/options".format(
            self.mall_id, product_no)
        headers = {
            'Authorization': "Bearer {}".format(self.token),
            'Content-Type': "application/json",
            'Accept-Encoding': "gzip, deflate",
            'Connection': "keep-alive",
        }
        data = {
            'shop_no': 1,
            'request': {
                "has_option": "T",
                "option_type": "F",
                "option_list_type": "S",
                "options": option,
                "option_display_type": "P",
            }
        }
        response = self.do_post(url, json.dumps(data), headers)
        # print(response)
        return response

    def update_option(self, product_no, option):
        url = "https://{}.cafe24api.com/api/v2/admin/products/{}/options".format(
            self.mall_id, product_no)
        headers = {
            'Authorization': "Bearer {}".format(self.token),
            'Content-Type': "application/json",
            'Accept-Encoding': "gzip, deflate",
            'Connection': "keep-alive",
        }
        data = {
            'shop_no': 1,
            'request': {
                "has_option": "T",
                "option_type": "F",
                "option_list_type": "S",
                "options": [{'option_name': 'size'}],
                "option_display_type": "P",
            }
        }
        response = self.do_post(url, json.dumps(data), headers)
        # print(response)
        return response

    def list_variants(self, product_no):
        url = "https://{}.cafe24api.com/api/v2/admin/products/{}/variants".format(
            self.mall_id, product_no)
        headers = {
            'Authorization': "Bearer {}".format(self.token),
            'Content-Type': "application/json",
            'Accept-Encoding': "gzip, deflate",
            'Connection': "keep-alive",
        }
        response = self.do_get(url, headers)
        # print(response)
        return response

    # Used for insert stock(quantity) information of no option product
    def update_variant_inventory(self, product_no, variant_code, quantity):
        url = "https://{}.cafe24api.com/api/v2/admin/products/{}/variants/{}/inventories".format(
            self.mall_id, product_no, variant_code)
        headers = {
            'Authorization': "Bearer {}".format(self.token),
            'Content-Type': "application/json",
            'Accept-Encoding': "gzip, deflate",
            'Connection': "keep-alive",
        }
        data = {
            "shop_no": 1,
            "requests": {
                "use_inventory": "T" if quantity > 0 else "F",
                "important_inventory": "A",
                "inventory_control_type": "B",
                "display_soldout": "T",
                "quantity": quantity,
                "safety_inventory": 1,
            }
        }

        response = self.do_put(url, json.dumps(data), headers)
        return response

    def update_variant(self, product_no, variant_code, quantity):
        url = "https://{}.cafe24api.com/api/v2/admin/products/{}/variants".format(
            self.mall_id, product_no)
        headers = {
            'Authorization': "Bearer {}".format(self.token),
            'Content-Type': "application/json",
            'Accept-Encoding': "gzip, deflate",
            'Connection': "keep-alive",
        }
        data = {
            "shop_no": 1,
            "requests": [{
                "variant_code": variant_code,
                "display": "T" if quantity > 0 else "F",
                "selling": "T" if quantity > 0 else "F",
                "quantity": str(quantity),
                "safety_inventory": 1,
                "display_soldout": "T",
                "use_inventory": "T" if quantity > 0 else "F",
                "important_inventory": "A",
                "inventory_control_type": "B",
            }]
        }
        print(data)
        response = self.do_put(url, json.dumps(data), headers)
        print(response)
        return response

    def create_memo(self, product_no, memo):
        url = "https://{}.cafe24api.com/api/v2/admin/products/{}/memos".format(
            self.mall_id, product_no)
        headers = {
            'Authorization': "Bearer {}".format(self.token),
            'Content-Type': "application/json",
            'Accept-Encoding': "gzip, deflate",
            'Connection': "keep-alive",
        }
        data = {
            "request": {
                "author_id": self.mall_id,
                "memo": memo
            }
        }
        response = self.do_post(url, json.dumps(data), headers)
        return response

    def upload_new_product(self, product, profiling_info, job_id):
        try:
            product['display'] = "T"
            product['selling'] = "T"
            if 'brand_code' in product:
                tmp_time = time.time()
                if product['brand_code'] == '':
                    product['brand_code'] = 'B0000000'
                elif product['brand_code'] in self.brands:
                    product['brand_code'] = self.brands[product['brand_code']]
                else:
                    product['brand_code'] = self.create_brand(
                        {'brand_name': product['brand_code'], 'use_brand': 'T', 'search_keyword': product['brand_code']})
                profiling_info['brand'] = profiling_info.get(
                    'brand', 0) + (time.time() - tmp_time)
            if 'detail_image' in product:
                tmp_time = time.time()
                product['detail_image'] = self.upload_image_from_link(
                    product['detail_image'])
                profiling_info['detail_image'] = profiling_info.get(
                    'detail_image', 0) + (time.time() - tmp_time)

            if 'manufacturer_code' in product:
                tmp_time = time.time()
                if product['manufacturer_code'] == '':
                    product['manufacturer_code'] = 'M0000000'
                elif product['manufacturer_code'] in self.manufacturers:
                    product['manufacturer_code'] = self.manufacturers[product['manufacturer_code']]
                else:
                    product['manufacturer_code'] = self.create_manufacturer(
                        {'manufacturer_name': product['manufacturer_code'], 'use_manufacturer': 'T', 'president_name': 'Test user'})
                profiling_info['manufacturer'] = profiling_info.get(
                    'manufacturer', 0) + (time.time() - tmp_time)

            additional_image = []
            if 'additional_image' in product:
                additional_image = product['additional_image']
                del product['additional_image']

            if 'variants' in product:
                variants = product.get('variants', [])
                del product['variants']

            num_combination = 1
            num_variant = 0
            if product['has_option'] == 'T' and len(variants) > 0:
                option_names = product['option_names']
                del product['option_names']
                options = {}
                for variant in variants:
                    for key, value in variant.items():
                        if key in option_names:
                            values = options.get(key, [])
                            if value not in values:
                                values.append(value)
                                num_variant = num_variant + 1
                            options[key] = values
                        num_combination = num_combination * num_variant 
                result = []
                if num_combination < 1000:
                    for option_name, values in options.items():
                        result.append({'name': option_name, 'value': values[0]})
                    product['options'] = result
                else:
                    print("Do not upload product option")
                    print("# of option combination: {}  (>= 1000)".format(num_combination))
                    product['has_option'] = 'F'


            tmp_time = time.time()
            product_result = self.create_product(product)
            profiling_info['create_product'] = profiling_info.get(
                'create_product', 0) + time.time() - tmp_time

            if 'product' not in product_result:
                print(product_result['error'])
                raise

            tpid = product_result['product']['product_no']
            # print(product_result['product'])
            # upload new product and then store target site product it to my site

            # update quantity, inventory..
            if 'memo' in product:
                tmp_time = time.time()
                self.create_memo(tpid, product['memo'])
                profiling_info['memo'] = profiling_info.get(
                    'memo', 0) + time.time() - tmp_time

            if product['has_option'] == 'T' and len(variants) > 0:
                for cafe24_variant in self.list_variants(tpid)['variants']:
                    cafe24_code = cafe24_variant['variant_code']
                    cafe24_options = cafe24_variant['options']
                    for variant in variants:
                        stat = True
                        for cafe24_option in cafe24_options:
                            # if cafe24_option['value'] != variant.get(cafe24_option['name'], None):
                            #  stat = False
                            #  break
                            # if stat:
                            self.update_variant(tpid, cafe24_code, 999)
                            #self.update_variant(tpid, cafe24_code, variant['stock'])
                            #print(self.update_variant(tpid, cafe24_code, 999))

            # elif len(variants) == 0:
            #  #for cafe24_variant in self.list_variants(tpid)['variants']:
            #  cafe24_variant = self.list_variants(tpid)['variants'][0]
            #  variant_code = cafe24_variant['variant_code']

            if len(additional_image) > 0:
                self.update_additional_images(tpid, additional_image)

            profiling_info['successful_node'] = profiling_info.get(
                'successful_node', 0) + 1
            self.graph_manager.update_tpid_into_mapping_table(
                job_id, tpid, product['mpid'], product['targetsite_url'])
        except:
            profiling_info['failed_node'] = profiling_info.get(
                'failed_node', 0) + 1
            raise

    # https://developers.cafe24.com/docs/en/api/admin/#update-a-product

    def update_exist_product(self, product, profiling_info, job_id, tpid):
        try:
            print('---------update product--------')
            product['product_no'] = tpid
            product['image_upload_type'] = "A"
            product['display'] = "T"
            product['selling'] = "T"
            product.pop('brand_code')
            has_option = product['has_option']
            product.pop('has_option')

            if 'manufacturer_code' in product:
                tmp_time = time.time()
                if product['manufacturer_code'] == '':
                    product['manufacturer_code'] = 'M0000000'
                elif product['manufacturer_code'] in self.manufacturers:
                    product['manufacturer_code'] = self.manufacturers[product['manufacturer_code']]
                else:
                    product['manufacturer_code'] = self.create_manufacturer(
                        {'manufacturer_name': product['manufacturer_code'], 'use_manufacturer': 'T', 'president_name': 'Test user'})
                profiling_info['manufacturer'] = profiling_info.get(
                    'manufacturer', 0) + (time.time() - tmp_time)

            # delete image from product in target site
            print(self.delete_image(tpid))

            # add detail image
            if 'detail_image' in product:
                tmp_time = time.time()
                product['detail_image'] = self.upload_image_from_link(
                    product['detail_image'])
                profiling_info['detail_image'] = profiling_info.get(
                    'detail_image', 0) + (time.time() - tmp_time)

            # delete additional image from product dictionary
            additional_image = []
            if 'additional_image' in product:
                additional_image = product['additional_image']
                del product['additional_image']

            tmp_time = time.time()
            product_result = self.update_product(product, tpid)
            profiling_info['update_product'] = profiling_info.get(
                'update_product', 0) + time.time() - tmp_time
            if 'product' not in product_result:
                print(product_result['error'])
                raise

            # update variants (option)
            #option = []
            # if 'variants' in product:
            #  variants = product.get('variants', [])
            #  del product['variants']
            # if has_option == 'T' and len(variants) > 0:
            #  option_names = product['option_names']
            #  del product['option_names']
            #  options = {}
            #  for variant in variants:
            #    for key, value in variant.items():
            #      if key in option_names:
            #        values = options.get(key, [])
            #        if value not in values: values.append(value)
            #        options[key] = values
            #  for option_name, values in options.items():
            #    option_value = []
            #    for val in values[0]:
            #      option_value.append({'option_text': val })
            #    option.append({'option_name': option_name, 'option_value': option_value})

            print(self.delete_option(tpid))

            if 'variants' in product:
                variants = product.get('variants', [])
                del product['variants']

            num_combination = 1
            num_variant = 0
            if has_option == 'T' and len(variants) > 0:
                option_names = product['option_names']
                del product['option_names']
                options = {}
                for variant in variants:
                    for key, value in variant.items():
                        if key in option_names:
                            values = options.get(key, [])
                            if value not in values:
                                values.append(value)
                                num_variant = num_variant + 1
                            options[key] = values
                        num_combination = num_combination * num_variant 
                result = []
                if num_combination < 1000:
                    for option_name, values in options.items():
                        result.append({'name': option_name, 'value': values[0]})
                    product['options'] = result
                else:
                    print("Do not update product option")
                    print("# of option combination: {}  (>= 1000)".format(num_combination))
                    has_option = 'F'



            if has_option == 'T' and len(variants) > 0:
                for cafe24_variant in self.list_variants(tpid)['variants']:
                    cafe24_code = cafe24_variant['variant_code']
                    cafe24_options = cafe24_variant['options']
                    for variant in variants:
                        stat = True
                        for cafe24_option in cafe24_options:
                            # if cafe24_option['value'] != variant.get(cafe24_option['name'], None):
                            #  stat = False
                            #  break
                            # if stat:
                            self.update_variant(tpid, cafe24_code, 999)
                            #self.update_variant(tpid, cafe24_code, variant['stock'])
                            #print(self.update_variant(tpid, cafe24_code, 999))

            # elif len(variants) == 0:
            #  #for cafe24_variant in self.list_variants(tpid)['variants']:
            #  cafe24_variant = self.list_variants(tpid)['variants'][0]
            #  variant_code = cafe24_variant['variant_code']

                # 0 -> N
            #  prd = self.get_option(tpid)
            #  if 'option' not in prd:
            #    print(prd['error'])
            #    raise
            #  if prd['option']['has_option'] == 'F':
            #    self.create_option(tpid, option)
            #  # need implement
            #  else:
            #    # Change option name or option value
            #    self.delete_option(tpid)
            #    self.create_option(tpid, option)

            #  #[{'test option': ['option val1', 'option val2']},{}] -> variants

            #  for cafe24_variant in self.list_variants(tpid)['variants']:
            #    variant_code = cafe24_variant['variant_code']
            #    cafe24_options = cafe24_variant['options']
            #    #[{'test option': ['option val1', 'option val2']},{}] -> variants
            #    #[{'name': 'test option', 'value': 'option val1'}] -> cafe24_options
            #    #[{'name': 'test option', 'value': 'option val2'}]
            #
            #    for variant in variants:
            #      stat = True
            #      for cafe24_option in cafe24_options:
            #        for val in variant.get(cafe24_option['name'], None):
            #          if cafe24_option['value'] == val:
            #            print(self.update_variant(tpid, variant_code, 999))

            # else:
            #  cafe24_variant = self.list_variants(tpid)['variants'][0]
            #  variant_code = cafe24_variant['variant_code']
            #  print(self.update_variant_inventory(tpid, variant_code, int(product['stock'])))
                # for cafe24_variant in self.list_variants(tpid)['variants']:
                #  variant_code = cafe24_variant['variant_code']
                #  cafe24_options = cafe24_variant['options']
                #  print(cafe24_options)
                #  for cafe24_option in cafe24_options:
                #    self.update_variant_inventory(tpid, variant_code, product['stock'])

            if len(additional_image) > 0:
                self.update_additional_images(tpid, additional_image)
                #self.create_additional_images(tpid, additional_image)

            profiling_info['successful_node'] = profiling_info.get(
                'successful_node', 0) + 1
        except:
            profiling_info['failed_node'] = profiling_info.get(
                'failed_node', 0) + 1
            raise

    # https://developers.cafe24.com/docs/en/api/admin/#update-a-product

    def hide_exist_product_no_profiling(self, tpid):  # delete
        try:
            print('---------hide product--------')
            product = {}
            product['product_no'] = tpid
            product['display'] = "F"
            product['selling'] = "F"

            tmp_time = time.time()
            product_result = self.update_product(product, tpid)
            print(product_result)
            if 'error' in product_result:
                print('Do not delete tpid = {} from tpid mapping table'.format(tpid))
        except:
            raise



    def hide_exist_product(self, profiling_info, job_id, tpid):  # delete
        try:
            print('---------hide product--------')
            product = {}
            product['product_no'] = tpid
            product['display'] = "F"
            product['selling'] = "F"

            tmp_time = time.time()
            product_result = self.update_product(product, tpid)
            print(product_result)
            profiling_info['update_product'] = profiling_info.get(
                'update_product', 0) + time.time() - tmp_time
            profiling_info['successful_node'] = profiling_info.get(
                'successful_node', 0) + 1
            if 'error' not in product_result:
                self.graph_manager.delete_from_tpid_mapping_table(tpid)
            else:
                print('Do not delete tpid = {} from tpid mapping table'.format(tpid))
        except:
            profiling_info['failed_node'] = profiling_info.get(
                'failed_node', 0) + 1
            raise


if __name__ == '__main__':

    args = {}
    args['mall_id'] = 'mallmalljmjm'
    args['user_id'] = 'mallmalljmjm'
    args['user_pwd'] = 'Dlwjdgns2'
    args['client_id'] = 'lmnl9eLRBye5aZvfSU4tXE'
    args['client_secret'] = 'nKAquRGpPVsgo6GZkeniLA'
    args['redirect_uri'] = 'https://www.google.com'
    args['scope'] = 'mall.write_product mall.read_product mall.read_category mall.write_category mall.read_collection mall.write_collection'

    cafe24manager = Cafe24Manager(args)
    cafe24manager.get_auth_code()
    cafe24manager.get_token()
    cafe24manager.refresh()
    # print(cafe24manager.list_brands())
    # print(cafe24manager.list_categories())
    #image_path = cafe24manager.upload_image_from_link('https://www.google.com/images/branding/googlelogo/1x/googlelogo_color_272x92dp.png')

    args = {
        'display': 'T',
        'add_category_no': [{
            'category_no': 46,
            'recommend': 'F',
            'new': 'T'
        }],
        'product_name': 'lsm',
        'supply_price': '1000.00',
        'price': '1000.00',
        # 'detail_image': image_path,
        'description': '<h1> abc </h1>'
    }

    # cafe24manager.create_product(args)

    cafe24manager.update_variant(897, 'P0000BIN000F', 10)
