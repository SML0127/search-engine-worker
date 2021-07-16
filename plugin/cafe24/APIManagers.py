import sys
import traceback
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
from functools import partial
from util.pse_timeout import *
print_flushed = partial(print, flush=True)

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
                print_flushed('Waiting for available client id, secret .....')
                time.sleep(10)
            #self.graph_manager.connect("dbname='pse' user='pse' host='127.0.0.1' port='5432' password='pse'")
        self.brands = {}
        self.manufacturers = {}
        print_flushed(client)
        self.connected = True

    def close(self):
        # smlee
        if self.connected == True:
            print_flushed('Close APIManager, so return client id, secret')
            self.graph_manager.return_client(
                self.client_id, self.client_secret)
            self.graph_manager.disconnect()
            self.connected = False

    def get_auth_code(self, log_mt_history_id):
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
        print_flushed(url)
        driver.delete_all_cookies()
        cnt = 0
        max_try = 5
        while cnt < max_try:
            cnt = cnt + 1

            print_flushed("=============== Try get auth code {} time ==============".format(cnt))
            try:
                max_cnt2 = 10
                while True:
                    cnt2 = 0
                    try:
                        driver.get(url)
                        break
                    except Exception as e:
                        if e.__class__.__name__ == 'WebDriverException':
                            cnt2 = cnt2 + 1
                            driver.delete_all_cookies()
                            time.sleep(2)
                            if cnt2 >= max_cnt2:
                                raise
                            else:
                                pass 
                        else:
                            raise
                cur_url = driver.current_url
                login_url = 'https://ec'
                agreement_url = 'https://{}'.format(self.mall_id)
                print_flushed(cur_url)
                if (cur_url[0:len(login_url)] == login_url):
                    print_flushed('try to log in')
                    inputElement = driver.find_element_by_id("mall_id")
                    inputElement.send_keys(self.user_id)
                    print_flushed('user_id:', self.user_id)
                    inputElement = driver.find_element_by_id("userpasswd")
                    inputElement.send_keys(self.user_pwd)
                    time.sleep(3)
                    print_flushed('user_pwd:', self.user_pwd)
                    inputElement = driver.find_element_by_class_name('mButton')
                    inputElement.click()
                    time.sleep(5)
                    cur_url = driver.current_url

                #self.graph_manager.log_err_msg_of_upload(log_mpid, err_msg, log_mt_history_id )
                #There is no auth code
                #URL:  https://eclogin.cafe24.com/Shop/?mode=ipblock
                if (cur_url == 'https://eclogin.cafe24.com/Shop/?mode=ipblock'):
                    print_flushed("IP block: ", cur_url)
                    cnt = 100
                    self.graph_manager.log_err_msg_of_upload(-1, "IP Block\n", log_mt_history_id )
                    raise 

                # Check is change password page
                if (cur_url == 'https://user.cafe24.com/comLogin/?action=comForce&req=hosting'):
                    print_flushed('@@@@@@@@@@@ Please change password')
                    self.graph_manager.log_err_msg_of_upload(-1, "!!!!!!!!! Change password", log_mt_history_id )
                    inputElement = driver.find_element_by_class_name('btnEm')
                    inputElement.click()
                    time.sleep(5)
                    cur_url = driver.current_url

                if (cur_url[0:len(agreement_url)] == agreement_url):
                    print_flushed('try to agree')
                    time.sleep(1)
                    print_flushed(cur_url)
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
                    print_flushed('There is no auth code')
                    print_flushed('URL: ', driver.current_url)
                    pass
                else:
                    driver.quit()
                    raise

        driver.quit()

    def do_post(self, url, data, headers):
        cnt = 0
        max_try = 3
        while cnt < max_try:
            print_flushed("=============== Try do post {} time ==============".format(cnt))
            try:
                response = requests.request(
                    "POST", url, data=data, headers=headers)
                response = json.loads(response.text)
                print_flushed(response)
                while 'error' in response and type(response['error']) == type({}) and response['error'].get('code', 0) == 429:
                    response = requests.request(
                        "POST", url, data=data, headers=headers)
                    response = json.loads(response.text)
                    print_flushed(response)
                if 'error' in response:
                    print_flushed(response)
                return response
            except:
                if cnt < max_try:
                    cnt = cnt + 1
                    pass
                else:
                    raise

    def do_delete(self, url, headers):
        cnt = 0
        max_try = 3
        while cnt < max_try:
            print_flushed("=============== Try do delete {} time ==============".format(cnt))
            try:
                response = requests.request(
                    "DELETE", url, headers=headers)
                response = json.loads(response.text)
                print_flushed(response)
                while 'error' in response and response['error']['code'] == 429:
                    response = requests.request(
                        "DELETE", url, headers=headers)
                    response = json.loads(response.text)
                    print_flushed(response)
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
            print_flushed("=============== Try do put {} time ==============".format(cnt))
            try:
                response = requests.request(
                    "PUT", url, data=data, headers=headers)
                response = json.loads(response.text)
                print_flushed(response)
                while 'error' in response and response['error']['code'] == 429:
                    response = requests.request(
                        "PUT", url, data=data, headers=headers)
                    response = json.loads(response.text)
                    print_flushed(response)
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
            print_flushed("=============== Try do get {} time ==============".format(cnt))
            try:
                response = requests.request("GET", url, headers=headers)
                print_flushed(response)
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
        # print_flushed(response)
        self.token = response['access_token']
        self.refresh_token = response['refresh_token']
        #print_flushed('get_token', self.token, self.refresh_token)

    def refresh(self):
        cnt = 0
        max_try = 3
        while cnt < max_try:
            print_flushed("=============== Try refresh token {} time ==============".format(cnt))
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
                #print_flushed('refresh token', self.token, self.refresh_token)
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
        # print_flushed(url)
        # print_flushed(headers)
        # print_flushed(data)
        response = self.do_post(url, json.dumps(data), headers)

        try:
            image_path = response['images'][0]['path']
            print_flushed(response['images'])
            print_flushed(image_path)
        except Exception as e:
            print_flushed(e)
            print_flushed(response)
            raise e

        if 'cafe24.com' in image_path:
            print_flushed(
                image_path[len('http://{}.cafe24.com'.format(self.mall_id)):])
            return image_path[len('http://{}.cafe24.com'.format(self.mall_id)):]
        else:
            print_flushed(image_path[len('http://{}.shop'.format(self.mall_id)):])
            return image_path[len('http://{}.shop'.format(self.mall_id)):]

    def upload_image_from_file(self, fpath):
        imgFile = open(fpath, 'rb')
        image = imgFile.read()
        return self.upload_image(image)

    def upload_image_from_link(self, link):
        cnt = 0
        max_try = 3
        image = ""
        while cnt < max_try:
            print_flushed(
                "=============== Try download & upload detail image {} time ==============".format(cnt))
            try:
                result = ""
                inner_cnt = 0
                max_inner_cnt = 5
                while True:
                    try:
                      scraper = cfscrape.create_scraper()
                      image = self.get_image_from_link(link, scraper)
                      break;
                    except:
                      if inner_cnt >= max_inner_cnt:
                          print_flushed('Fail to download image from link: ', link)
                          print_flushed(str(traceback.format_exc()))
                          break;
                      else:
                          inner_cnt = inner_cnt + 1
                          pass
                #print_flushed("upload_image_from_link: ", image)
                return self.upload_image(image)
            except:
                if cnt < max_try:
                    cnt = cnt + 1
                    pass
                else:
                    raise

    @pse_timeout(30)
    def get_image_from_link(self, link, scraper):
        u = ""
        if link[:12] == 'https://cdn2':
            r = scraper.get(link)
            u = r.content
            #print_flushed(u)
            dimage = BytesIO()
            im = Image.open(BytesIO(u)).convert("RGB").save(dimage, "JPEG")
            u = dimage.getvalue()
            #im = Image.open(BytesIO(u))

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
            dimage = BytesIO()
            im = Image.open(BytesIO(u)).convert("RGB").save(dimage, "JPEG")
            u = dimage.getvalue()

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
        if 'option_matrix' in args: 
            del args['option_matrix']
        data = {
            'shop_no': 1,
            'request': args
        }
        response = self.do_put(url, json.dumps(data), headers)
        return response

    def create_product(self, args):
        url = "https://{}.cafe24api.com/api/v2/admin/products".format(self.mall_id)
        headers = {
            'Content-Type': "application/json",
            'Authorization': "Bearer {}".format(self.token),
            'Accept-Encoding': "gzip, deflate",
            'Connection': "keep-alive",
        }
        if 'option_matrix' in args: 
            del args['option_matrix']
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
            print_flushed(
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
                    print_flushed(response)
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
            print_flushed(
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
                    print_flushed(response)
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
        # print_flushed(response)
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
            result = ""
            inner_cnt = 0
            max_inner_cnt = 5
            while True:
                try:
                  scraper = cfscrape.create_scraper()
                  result = self.get_image_from_link(link, scraper)
                  additional_image.append(result)
                  break;
                except:
                  if inner_cnt >= max_inner_cnt:
                      print_flushed('Fail to download image from link: ', link)
                      print_flushed(str(traceback.format_exc()))
                      break;
                  else:
                      inner_cnt = inner_cnt + 1
                      pass


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
        # print_flushed(response)
        return response

    def update_additional_images(self, product_no, links):
        if len(links) == 0:
            return None
        if len(links) > 20:
            links = links[0:20]
        cnt = 0
        max_try = 3
        while cnt < max_try:
            print_flushed(
                "=============== Try download & upload additional images {} time ==============".format(cnt))
            try:
                additional_image = []
                for link in links:
                    result = ""
                    inner_cnt = 0
                    max_inner_cnt = 5
                    while True:
                        try:
                          scraper = cfscrape.create_scraper()
                          result = self.get_image_from_link(link, scraper)
                          additional_image.append(result)
                          break;
                        except:
                          if inner_cnt >= max_inner_cnt:
                              print_flushed('Fail to download image from link: ', link)
                              print_flushed(str(traceback.format_exc()))
                              break;
                          else:
                              inner_cnt = inner_cnt + 1
                              pass


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
                # print_flushed(response)
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
                "option_type": "T",
                "option_list_type": "S",
                "options": option,
                "option_display_type": "P",
            }
        }
        response = self.do_post(url, json.dumps(data), headers)
        # print_flushed(response)
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
                "option_type": "T",
                "option_list_type": "S",
                "options": [{'option_name': 'size'}],
                "option_display_type": "P",
            }
        }
        response = self.do_post(url, json.dumps(data), headers)
        # print_flushed(response)
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
        # print_flushed(response)
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

    def delete_variant(self, product_no, variant_code):
        url = "https://{}.cafe24api.com/api/v2/admin/products/{}/variants/{}".format(
            self.mall_id, product_no, variant_code)
        headers = {
            'Authorization': "Bearer {}".format(self.token),
            'Content-Type': "application/json",
            'Accept-Encoding': "gzip, deflate",
            'Connection': "keep-alive",
        }
        response = self.do_delete(url, headers)
        print_flushed(response)
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
        print_flushed(data)
        response = self.do_put(url, json.dumps(data), headers)
        print_flushed(response)
        return response

    def update_variant_additional_price(self, product_no, variant_code, quantity, additional_amount):
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
                "additional_amount": additional_amount,
            }]
        }
        response = self.do_put(url, json.dumps(data), headers)
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

    def upload_new_product(self, product, profiling_info, job_id, log_mt_history_id):
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
            
            #variants = [{'option_name1': ['v1', 'v2']}, {'option_name2': ['v3', 'v4']}]
            num_combination = 1
            num_variant = 0
            options = {}
            if product['has_option'] == 'T' and len(variants) > 0:
                option_names = product['option_names']
                del product['option_names']
                #print_flushed(variants)
                #variant = {'option_name1': [{value: 'v1', stock: ??}, {value: 'v2', stock: ??}]}
                for variant in variants:
                    for key, value in variant.items():
                        if key in option_names:
                            values = options.get(key, [])
                            if value not in values:
                                print_flushed(value)
                                for op_v in value:
                                    print_flushed(op_v)
                                    op_v['value'] = op_v['value'].replace('"','').replace("'","").replace(',', ' ').replace(';', ' ').replace('#', '').replace('$', '').replace('%', '').replace('\\', '')
                                values.append(value)
                                num_variant = len(value)
                            #print_flushed('-----------------------')
                            #print_flushed(values)
                            options[key] = values 
                        num_combination = num_combination * num_variant 
                result = []
                if num_combination < 1000:
                    for option_name, values in options.items():
                        option_value = []
                        for value in values[0]:
                           option_value.append(value['value'])
                        #result.append({'name': option_name, 'value': values[0]})
                        result.append({'name': option_name, 'value': option_value})
                    product['options'] = result
                else:
                    print_flushed("Do not upload product option")
                    print_flushed("# of option combination: {}  (>= 1000)".format(num_combination))
                    err_msg = "Do not upload product option \n# of option combination: {}  (>= 1000)".format(num_combination)
                    try:
                        self.graph_manager.log_err_msg_of_upload(product['mpid'], err_msg, log_mt_history_id )
                    except:
                        pass
                    product['has_option'] = 'F'

            option_matrix = product.get('option_matrix','')
            matrix_row_name = product.get('matrix_row_name','')
            matrix_col_name = product.get('matrix_col_name','')
            if option_matrix != '':
               del product['option_matrix']
               del product['matrix_row_name']
               del product['matrix_col_name']

            tmp_time = time.time()
            upload_product = product
            print_flushed(upload_product)
            product_result = self.create_product(upload_product)
            profiling_info['create_product'] = profiling_info.get(
                'create_product', 0) + time.time() - tmp_time

            if 'product' not in product_result:
                print_flushed(product_result['error'])
                raise Exception(product_result['error']['message'])

            # upload new product and then store target site product it to my site
            tpid = product_result['product']['product_no']
            self.graph_manager.update_tpid_into_mapping_table(
                job_id, tpid, product['mpid'], product['targetsite_url'])

            # update quantity, inventory..
            if 'memo' in product:
                tmp_time = time.time()
                self.create_memo(tpid, product['memo'])
                profiling_info['memo'] = profiling_info.get(
                    'memo', 0) + time.time() - tmp_time
            if product['has_option'] == 'T' and len(variants) > 0:
                print_flushed(options)
                print_flushed(option_matrix)
                print_flushed(matrix_row_name)
                print_flushed(matrix_col_name)
                print_flushed('-------------------------------------------------------------------------')
                print_flushed(self.list_variants(tpid)['variants'])
                for cafe24_variant in self.list_variants(tpid)['variants']:
                    cafe24_code = cafe24_variant['variant_code']
                    cafe24_options = cafe24_variant['options']
                    print_flushed(cafe24_variant)
                    stock = 999
                    additional_amount = 0
                    row_value = ""
                    col_value = ""
                    # one variant [{name:color, value:blue}, {name:size, value:small}]
                    if cafe24_options is None:
                        print_flushed('cafe24_options is None')
                    if cafe24_options is not None:
                        for cafe24_option in cafe24_options:
                            # {name:color, value:blue}
                            #print_flushed(cafe24_option)
                            if matrix_row_name == '':
                                for option in options[cafe24_option['name']][0]:
                                    if option['value'] == cafe24_option['value']:
                                        if stock > option['stock']:
                                            stock = option['stock']  
                                        additional_amount = additional_amount + option['additional_amount']  
                            else:
                                # cafe24_option[{},{}]
                                for option in options[cafe24_option['name']][0]:
                                    if cafe24_option['name'] == matrix_row_name:
                                        row_value = cafe24_option['value']
                                    elif cafe24_option['name'] == matrix_col_name:
                                        col_value = cafe24_option['value']
                                    else:
                                        if option['value'] == cafe24_option['value']:
                                            if stock > option['stock']:
                                                stock = option['stock']  
                                            additional_amount = additional_amount + option['additional_amount']  
                                    if row_value != "" and col_value != "":
                                        #print_flushed(row_value, col_value)
                                        #print_flushed(option_matrix)
                                        if stock > option_matrix[row_value, col_value]['stock']:
                                            stock = option_matrix[row_value, col_value]['stock']  
                                        additional_amount = additional_amount + option_matrix[row_value, col_value]['additional_amount']  
                                        row_value = ""
                                        col_value = ""
                        response = self.update_variant_additional_price(tpid, cafe24_code, stock, additional_amount)
                        if 'error' in response:
                            print_flushed("Product creation was successful, but option variant update failed")
                            err_msg = "Product creation was successful, but option variant update failed\n\n"
                            err_msg += '================================ Error Message ================================ \n'
                            err_msg += response['error']['message'] + '\n\n'
                            try:
                                self.graph_manager.log_err_msg_of_upload(product['mpid'], err_msg, log_mt_history_id )
                            except:
                                pass


            # elif len(variants) == 0:
            #  #for cafe24_variant in self.list_variants(tpid)['variants']:
            #  cafe24_variant = self.list_variants(tpid)['variants'][0]
            #  variant_code = cafe24_variant['variant_code']

            if len(additional_image) > 0:
                response = self.update_additional_images(tpid, additional_image)
                if 'error' in response:
                    print_flushed("Product creation was successful, but additional image update failed")
                    err_msg = "Product creation was successful, but additional image update failed\n\n"
                    err_msg += '================================ Error Message ================================ \n'
                    err_msg += response['error']['message'] + '\n\n'
                    try:
                        self.graph_manager.log_err_msg_of_upload(product['mpid'], err_msg, log_mt_history_id )
                    except:
                        pass

            profiling_info['successful_node'] = profiling_info.get(
                'successful_node', 0) + 1
        except:
            print_flushed(str(traceback.format_exc()))
            print_flushed('-------------------------------------------------------------------------------------------')
            print_flushed(product)
            print_flushed('-------------------------------------------------------------------------------------------')
            profiling_info['failed_node'] = profiling_info.get(
                'failed_node', 0) + 1
            raise

    # https://developers.cafe24.com/docs/en/api/admin/#update-a-product

    def update_exist_product(self, product, profiling_info, job_id, tpid, log_mt_history_id):
        try:
            print_flushed('---------update product--------')
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
            print_flushed(self.delete_image(tpid))

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
            option_matrix = product.get('option_matrix','')
            product_result = self.update_product(product, tpid)
            product['option_matrix'] = option_matrix
            profiling_info['update_product'] = profiling_info.get(
                'update_product', 0) + time.time() - tmp_time
            if 'product' not in product_result:
                print_flushed(product_result['error'])
                raise

            print_flushed(self.delete_option(tpid))

            if 'variants' in product:
                variants = product.get('variants', [])
                del product['variants']

            num_combination = 1
            num_variant = 0
            print_flushed(num_combination)
            options = {}
            create_option_array = []
            if has_option == 'T' and len(variants) > 0:
                option_names = product['option_names']
                del product['option_names']
                #print_flushed(variants)
                for variant in variants:
                    for key, value in variant.items():
                        if key in option_names:
                            values = options.get(key, [])
                            create_option_values = []
                            if value not in values:
                                #print_flushed(value)
                                values2 = []
                                for op_v in value:
                                    op_v['value'] = op_v['value'].replace('"','').replace("'","").replace(',', ' ').replace(';', ' ').replace('#', '').replace('$', '').replace('%', '').replace('\\', '')
                                    create_option_values.append({'option_text':op_v['value']})
                                values.append(value)
                                num_variant = num_variant + 1
                            #print_flushed('-----------------------')
                            #print_flushed(values)
                            options[key] = values 
                            create_option_array.append({'option_name': key, 'option_value' : create_option_values})
                        num_combination = num_combination * num_variant 
                        print_flushed(num_combination)
                result = []
                if num_combination < 1000:
                    for option_name, values in options.items():
                        option_value = []
                        for value in values[0]:
                           option_value.append(value['value'])
                        #result.append({'name': option_name, 'value': values[0]})
                        result.append({'name': option_name, 'value': option_value})
                    product['options'] = result

                else:
                    print_flushed("Do not upload product option")
                    print_flushed("# of option combination: {}  (>= 1000)".format(num_combination))
                    err_msg = "Do not upload product option \n# of option combination: {}  (>= 1000)".format(num_combination)
                    try:
                        self.graph_manager.log_err_msg_of_upload(product['mpid'], err_msg, log_mt_history_id )
                    except:
                        pass
                    has_option = 'F'

            print_flushed('--------------------------------------------------------')
            option_matrix = product.get('option_matrix','')
            matrix_row_name = product.get('matrix_row_name','')
            matrix_col_name = product.get('matrix_col_name','')
            if option_matrix != '':
               del product['option_matrix']
               del product['matrix_row_name']
               del product['matrix_col_name']


            if has_option == 'T' and len(variants) > 0:
                print_flushed(options)
                print_flushed(option_matrix)
                print_flushed(matrix_row_name)
                print_flushed(matrix_col_name)
                print_flushed('-------------------------------------------------------------------------')
                uploaded_variants = self.list_variants(tpid)['variants']
                if uploaded_variants[0]['options'] is None:
                    print_flushed('cafe24_options is None')
                    #print_flushed(self.delete_variant(tpid,uploaded_variants[0]['variant_code']))
                    print_flushed(self.create_option(tpid, create_option_array))
                for cafe24_variant in self.list_variants(tpid)['variants']:
                    cafe24_code = cafe24_variant['variant_code']
                    cafe24_options = cafe24_variant['options']
                    stock = 999
                    additional_amount = 0
                    row_value = ""
                    col_value = ""
                    # one variant [{name:color, value:blue}, {name:size, value:small}]
                    if cafe24_options is None:
                        print_flushed('cafe24_options is None')
                    if cafe24_options is not None:                    
                        for cafe24_option in cafe24_options:
                            # {name:color, value:blue}
                            #print_flushed(cafe24_option)
                            if matrix_row_name == '':
                                for option in options[cafe24_option['name']][0]:
                                    if option['value'] == cafe24_option['value']:
                                        if stock > option['stock']:
                                            stock = option['stock']  
                                        additional_amount = additional_amount + option['additional_amount']  
                            else:
                                # cafe24_option[{},{}]
                                for option in options[cafe24_option['name']][0]:
                                    if cafe24_option['name'] == matrix_row_name:
                                        row_value = cafe24_option['value']
                                    elif cafe24_option['name'] == matrix_col_name:
                                        col_value = cafe24_option['value']
                                    else:
                                        if option['value'] == cafe24_option['value']:
                                            if stock > option['stock']:
                                                stock = option['stock']  
                                            additional_amount = additional_amount + option['additional_amount']  
                                    if row_value != "" and col_value != "":
                                        #print_flushed(row_value, col_value)
                                        print_flushed(option_matrix)
                                        if stock > option_matrix[row_value, col_value]['stock']:
                                            stock = option_matrix[row_value, col_value]['stock']  
                                        additional_amount = additional_amount + option_matrix[row_value, col_value]['additional_amount']  
                                        row_value = ""
                                        col_value = ""
                        response = self.update_variant_additional_price(tpid, cafe24_code, stock, additional_amount)
                        if 'error' in response:
                            print_flushed("Product creation was successful, but option variant update failed")
                            err_msg = "Product creation was successful, but option variant update failed\n\n"
                            err_msg += '================================ Error Message ================================ \n'
                            err_msg += response['error']['message'] + '\n\n'
                            try:
                                self.graph_manager.log_err_msg_of_upload(product['mpid'], err_msg, log_mt_history_id )
                            except:
                                pass


            if len(additional_image) > 0:
                response = self.update_additional_images(tpid, additional_image)
                if 'error' in response:
                    print_flushed("Product update was successful, but additional image update failed")
                    err_msg = "Product update was successful, but additional image update failed\n\n"
                    err_msg += '================================ Error Message ================================ \n'
                    err_msg += response['error']['message'] + '\n\n'
                    try:
                        self.graph_manager.log_err_msg_of_upload(product['mpid'], err_msg, log_mt_history_id )
                    except:
                        pass
                #self.create_additional_images(tpid, additional_image)

            profiling_info['successful_node'] = profiling_info.get(
                'successful_node', 0) + 1
        except:
            print_flushed(str(traceback.format_exc()))
            print_flushed('-------------------------------------------------------------------------------------------')
            print_flushed(product)
            print_flushed('-------------------------------------------------------------------------------------------')
            profiling_info['failed_node'] = profiling_info.get(
                'failed_node', 0) + 1
           
            raise

    # https://developers.cafe24.com/docs/en/api/admin/#update-a-product

    def hide_exist_product_no_profiling(self, tpid):  # delete
        try:
            print_flushed('---------hide product--------')
            product = {}
            product['product_no'] = tpid
            product['display'] = "F"
            product['selling'] = "F"

            tmp_time = time.time()
            product_result = self.update_product(product, tpid)
            print_flushed(product_result)
            if 'error' in product_result:
                print_flushed('Do not delete tpid = {} from tpid mapping table'.format(tpid))
        except:
            raise



    def hide_exist_product(self, profiling_info, job_id, tpid):  # delete
        try:
            print_flushed('---------hide product--------')
            product = {}
            product['product_no'] = tpid
            product['display'] = "F"
            product['selling'] = "F"

            tmp_time = time.time()
            product_result = self.update_product(product, tpid)
            print_flushed(product_result)
            profiling_info['update_product'] = profiling_info.get(
                'update_product', 0) + time.time() - tmp_time
            profiling_info['successful_node'] = profiling_info.get(
                'successful_node', 0) + 1
            if 'error' not in product_result:
                self.graph_manager.delete_from_tpid_mapping_table(tpid)
            else:
                print_flushed('Do not delete tpid = {} from tpid mapping table'.format(tpid))
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
    # print_flushed(cafe24manager.list_brands())
    # print_flushed(cafe24manager.list_categories())
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
