from lxml import html
import time
from lxml import etree

from seleniumwire import webdriver
#from seleniumwire import webdriver
from selenium.common.exceptions import *
import traceback
from selenium.webdriver.common.proxy import Proxy, ProxyType
from selenium.webdriver.support.ui import WebDriverWait 
from random_user_agent.user_agent import UserAgent
from random_user_agent.params import SoftwareName, OperatingSystem
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait 
from selenium.common.exceptions import TimeoutException, WebDriverException, InvalidSessionIdException
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.common.exceptions import TimeoutException, WebDriverException, InvalidSessionIdException
import time
import requests
import http

from functools import partial
print_flushed = partial(print, flush=True)

software_names = [SoftwareName.CHROME.value]
operating_systems = [OperatingSystem.WINDOWS.value, OperatingSystem.LINUX.value]
user_agent_rotator = UserAgent(software_names=software_names, operating_systems=operating_systems, limit=100)



class NoElementFoundError(Exception):

  def __init__(self, error):
    self.error = error

  def __str__(self):
    return str("NoElementFoundError of xpath: ") + str(self.error) + "\n"


class WebMgrErr(Exception):
  
  def __init__(self, error):
    self.error = error

  def __str__(self):
    return str("WebMgrErr: ") + "\n" + str(self.error)

selenium_chrome_erros = ['StaleElementReferenceException', 'WebDriverException', 'TimeoutException']
class WebManager():

  def __init__(self):
      self.drivers = []
      self.drivers_is_zipcode_reset = []
      self.drivers_last_amazon_country = []
      self.settings = {}

  def init(self, settings):
    try:
      self.settings = settings
      num_driver = settings.get('num_driver', 1)
      option = Options()
      #option = webdriver.ChromeOptions()
      
      option.add_argument('--headless')
      option.add_argument('--disable-gpu')
      option.add_argument('--window-size=2048,1536')
      option.add_argument('--start-maximized')
      option.add_argument('--no-proxy-server')
      option.add_argument('--no-sandbox')
      option.add_argument('--blink-settings=imagesEnabled=false')
      option.add_argument('--lang=en_US')
      option.add_argument('--disable-dev-shm-usage')
      option.add_argument('--disable-blink-features=AutomationControlled')
      option.add_argument('--disable-infobars')
      #option.add_argument('--disable-automation')
      #option.add_argument('--disable-extensions')
      #prefs = {"profile.managed_default_content_settings.images": 2}
      prefs = {"profile.managed_default_content_settings.images":2,
               "profile.default_content_setting_values.notifications":2,
               "profile.managed_default_content_settings.stylesheets":2,
               "profile.managed_default_content_settings.cookies":2,
               "profile.managed_default_content_settings.javascript":2,
               "profile.managed_default_content_settings.plugins":2,
               "profile.managed_default_content_settings.popups":1,
               "profile.managed_default_content_settings.geolocation":2,
               "profile.managed_default_content_settings.media_stream":2,
      }
      option.add_experimental_option("prefs", prefs)

      driver_path = self.settings.get('chromedriver_path', './web_drivers/chromedriver')
      
      self.javascripts = {
        'style': './managers/SerializeWithStyles.js'
      }

      for javascript in self.javascripts.keys():
        f = open('./managers/SerializeWithStyles.js')
        code = f.read()
        f.close()
        self.javascripts[javascript] = code

      for i in range(num_driver):
        user_agent = self.settings['chromedriver_user_agent']
        #user_agent = user_agent_rotator.get_random_user_agent()
        option.add_argument('--user-agent={}'.format(user_agent))
        
        #driver = webdriver.Chrome(driver_path, chrome_options = option)
        driver = webdriver.Chrome(driver_path, options = option)
        driver.implicitly_wait(5)
        driver.set_page_load_timeout(120)
        driver.get('about:blank')
        driver.execute_script("Object.defineProperty(navigator, 'plugins', {get: function() {return[1, 2, 3, 4, 5];},});")
        self.drivers.append(driver)
        self.drivers_is_zipcode_reset.append(True)
        self.drivers_last_amazon_country.append("")
      self.driver_idx = 0
    except Exception as e:
      if e.__class__.__name__ in selenium_chrome_erros:
      #if e.__class__.__name__ == 'WebDriverException' or e.__class__.__name__ == 'TimeoutException':
        raise e; 
      else:
        raise WebMgrErr(e)

  def restart(self,sleep_time):
    try:
      print_flushed('---start restart func--')
      for driver in self.drivers:
        driver.quit()
      self.drivers = []
      self.drivers_is_zipcode_reset = []
      self.drivers_last_amazon_country = []
      time.sleep(sleep_time)
      num_driver = self.settings.get('num_driver', 1)
      option = Options()
      #option = webdriver.ChromeOptions()
      option.add_argument('--headless')
      option.add_argument('--disable-gpu')
      option.add_argument('--window-size=2048,1536')
      option.add_argument('--start-maximized')
      option.add_argument('--no-proxy-server')
      option.add_argument('--no-sandbox')
      option.add_argument('--blink-settings=imagesEnabled=false')
      option.add_argument('--lang=en_US')
      option.add_argument('--disable-dev-shm-usage')
      option.add_argument('--disable-blink-features=AutomationControlled')
      option.add_argument('--disable-infobars')
      #option.add_argument('--disable-automation')
      #option.add_argument('--disable-extensions')
      #prefs = {"profile.managed_default_content_settings.images": 2}
      prefs = {"profile.managed_default_content_settings.images":2,
               "profile.default_content_setting_values.notifications":2,
               "profile.managed_default_content_settings.stylesheets":2,
               "profile.managed_default_content_settings.cookies":2,
               "profile.managed_default_content_settings.javascript":2,
               "profile.managed_default_content_settings.plugins":2,
               "profile.managed_default_content_settings.popups":1,
               "profile.managed_default_content_settings.geolocation":2,
               "profile.managed_default_content_settings.media_stream":2,
      }
      option.add_experimental_option("prefs", prefs)



      #print_flushed(self.settings)
      driver_path = self.settings.get('chromedriver_path', './web_drivers/chromedriver')
      
      self.javascripts = {
        'style': './managers/SerializeWithStyles.js'
      }

      for javascript in self.javascripts.keys():
        f = open('./managers/SerializeWithStyles.js')
        code = f.read()
        f.close()
        self.javascripts[javascript] = code

      for i in range(num_driver):
        user_agent = self.settings['chromedriver_user_agent']
        #user_agent = user_agent_rotator.get_random_user_agent()
        option.add_argument('--user-agent={}'.format(user_agent))
        driver = webdriver.Chrome(driver_path, options = option)
        #driver = webdriver.Chrome(driver_path, chrome_options = option)
        driver.implicitly_wait(5)
        driver.set_page_load_timeout(120)
        driver.get('about:blank')
        driver.execute_script("Object.defineProperty(navigator, 'plugins', {get: function() {return[1, 2, 3, 4, 5];},});")
        self.drivers.append(driver)
        self.drivers_is_zipcode_reset.append(True)
        self.drivers_last_amazon_country.append("")
      self.driver_idx = 0
      print_flushed('---end restart func--')
    except Exception as e:
      if e.__class__.__name__ in selenium_chrome_erros:
      #if e.__class__.__name__ == 'WebDriverException' or e.__class__.__name__ == 'TimeoutException':
        raise e; 
      else:
        raise WebMgrErr(e)

  def close(self):
    try:
      print_flushed(self.drivers)
      for driver in self.drivers:
        driver.quit()
    except Exception as e:
      raise WebMgrErr(e)

 
  def get_cur_driver_(self):
    return self.drivers[self.driver_idx]

  def get_cur_driver_is_zipcode_reset(self):
    return self.drivers_is_zipcode_reset[self.driver_idx]

  def get_cur_driver_zipcode_country(self):
    return self.drivers_last_amazon_country[self.driver_idx]

  def change_cur_driver_zipcode_country(self, country):
    self.drivers_last_amazon_country[self.driver_idx] = country

  def reset_cur_driver_zipcode_boolean(self):
    self.drivers_is_zipcode_reset[self.driver_idx] = False

  def set_cur_driver_zipcode_boolean(self):
    self.drivers_is_zipcode_reset[self.driver_idx] = True


  def rotate_driver_(self):
    self.driver_idx += 1
    if self.driver_idx == len(self.drivers):
      self.driver_idx = 0


  def store_page_source(self, name):
    try:
      driver = self.get_cur_driver_()
      f = open(name, 'w')
      page_source = driver.page_source
      lxml_tree = html.fromstring(page_source)
      f.write(etree.tostring(lxml_tree, encoding='unicode', pretty_print=True))
      f.close()
    except Exception as e:
      if e.__class__.__name__ in selenium_chrome_erros:
      #if e.__class__.__name__ == 'WebDriverException' or e.__class__.__name__ == 'TimeoutException':
        raise e; 
      else:
        raise WebMgrErr(e)


  def get_html(self):
    try:
      driver = self.get_cur_driver_()
      page_source = driver.page_source 
      return page_source
    except Exception as e:
      if e.__class__.__name__ in selenium_chrome_erros:
      #if e.__class__.__name__ == 'WebDriverException' or e.__class__.__name__ == 'TimeoutException':
        raise e; 
      else:
        raise WebMgrErr(e)


  def build_lxml_tree(self):
    try:
      driver = self.get_cur_driver_()
      driver.execute_script("window.scrollTo(0, document.body.scrollHeight)")
      time.sleep(3)
      driver.execute_script("window.scrollTo(0, document.body.scrollHeight)")
      time.sleep(3)
      page_source = driver.page_source 
      self.lxml_tree = html.fromstring(page_source)
    except Exception as e:
      if e.__class__.__name__ in selenium_chrome_erros:
      #if e.__class__.__name__ == 'WebDriverException' or e.__class__.__name__ == 'TimeoutException':
        raise e; 
      else:
        raise WebMgrErr(e)


  def load(self, url):
    try:
      self.rotate_driver_()
      driver = self.get_cur_driver_()
      driver.get(url)
      WebDriverWait(driver, 60).until(lambda d: d.execute_script('return document.readyState') == 'complete')
      driver.execute_script("window.scrollTo(0, document.body.scrollHeight)")
      cnt = 0
      max_cnt = 3
      while (self.get_html() == '<html><head></head><body></body></html>'):
        print_flushed("Reload page (blank page)")
        driver.get(url)
        WebDriverWait(driver, 60).until(lambda d: d.execute_script('return document.readyState') == 'complete')
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight)")
        cnt = cnt + 1
        if cnt >= max_cnt:
          break;
        

    except Exception as e:
      if e.__class__.__name__ in selenium_chrome_erros:
      #if e.__class__.__name__ == 'WebDriverException' or e.__class__.__name__ == 'TimeoutException':
        raise e; 
      else:
        raise WebMgrErr(e)


  def get_current_url(self):
    try:
      driver = self.get_cur_driver_()
      return driver.current_url
    except Exception as e:
      if e.__class__.__name__ in selenium_chrome_erros:
      #if e.__class__.__name__ == 'WebDriverException' or e.__class__.__name__ == 'TimeoutException':
        raise e; 
      else:
        raise WebMgrErr(e)

    
  def wait_loading(self):
    try:
      driver = self.get_cur_driver_()
      WebDriverWait(driver, 10).until(lambda d: d.execute_script('return document.readyState') == 'complete')
    except Exception as e:
      if e.__class__.__name__ in selenium_chrome_erros:
      #if e.__class__.__name__ == 'WebDriverException' or e.__class__.__name__ == 'TimeoutException':
        raise e; 
      else:
        raise WebMgrErr(e)


  def execute_script(self, script):
    try:
      driver = self.get_cur_driver_()
      return driver.execute_script(script)
    except Exception as e:
      if e.__class__.__name__ in selenium_chrome_erros:
      #if e.__class__.__name__ == 'WebDriverException' or e.__class__.__name__ == 'TimeoutException':
        raise e; 
      else:
        raise WebMgrErr(e)
 
  def get_subtree_no_parent_with_style(self, xpath):
    try:
      driver = self.get_cur_driver_()
      elements = driver.find_elements_by_xpath(xpath)
      if len(elements) > 0:
        driver.execute_script(self.javascripts['style'])
        print_flushed(driver.execute_script('return arguments[0].innerHTML;', elements[0]))
        return driver.execute_script('return arguments[0].innerHTML;', elements[0])
      return ''
    except Exception as e:
      if e.__class__.__name__ in selenium_chrome_erros:
      #if e.__class__.__name__ == 'WebDriverException' or e.__class__.__name__ == 'TimeoutException':
        raise e; 
      else:
        raise WebMgrErr(e)
  

  def get_subtree_no_parent_with_style_strong(self, xpath):
    try:
      driver = self.get_cur_driver_()
      elements = driver.find_elements_by_xpath(xpath)
      if len(elements) == 0: raise NoElementFoundError(xpath)
      driver.execute_script(self.javascripts['style'])
      print_flushed(driver.execute_script('return arguments[0].innerHTML;', elements[0]))
      return driver.execute_script('return arguments[0].innerHTML;', elements[0])
    except Exception as e:
      if e.__class__.__name__ in selenium_chrome_erros:
      #if e.__class__.__name__ == 'WebDriverException' or e.__class__.__name__ == 'TimeoutException':
        raise e; 
      else:
        raise WebMgrErr(e)


 

  def get_subtree_with_style(self, xpath):
    try:
      driver = self.get_cur_driver_()
      elements = driver.find_elements_by_xpath(xpath)
      if len(elements) > 0:
        driver.execute_script(self.javascripts['style'])
        return driver.execute_script('return arguments[0].serializeWithStyles();', elements[0])
      return ''
    except Exception as e:
      if e.__class__.__name__ in selenium_chrome_erros:
      #if e.__class__.__name__ == 'WebDriverException' or e.__class__.__name__ == 'TimeoutException':
        raise e; 
      else:
        raise WebMgrErr(e)
  

  def get_subtree_with_style_strong(self, xpath):
    try:
      driver = self.get_cur_driver_()
      elements = driver.find_elements_by_xpath(xpath)
      if len(elements) == 0: raise NoElementFoundError(xpath)
      driver.execute_script(self.javascripts['style'])
      return driver.execute_script('return arguments[0].serializeWithStyles();', elements[0])
    except Exception as e:
      if e.__class__.__name__ in selenium_chrome_erros:
      #if e.__class__.__name__ == 'WebDriverException' or e.__class__.__name__ == 'TimeoutException':
        raise e; 
      else:
        raise WebMgrErr(e)


  def get_elements_by_selenium_(self, xpath):
    driver = self.get_cur_driver_()
    elements = driver.find_elements_by_xpath(xpath)
    print_flushed(len(elements), xpath)
    return elements


  def get_elements_by_selenium_strong_(self, xpath):
    #driver = self.get_cur_driver_()
    #element = WebDriverWait(driver, 60).until(EC.presence_of_element_located((By.XPATH, xpath)))
    elements = self.get_elements_by_selenium_(xpath)
    if len(elements) == 0:
      raise NoElementFoundError(xpath)
    return elements

  def get_elements_by_lxml_(self, xpath):
    #self.build_lxml_tree()
    elements = self.lxml_tree.xpath(xpath)
    print_flushed(len(elements), xpath)
    return elements

  def get_elements_by_lxml_strong_(self, xpath):
    elements = self.get_elements_by_lxml_(xpath)
    if len(elements) == 0:
      print_flushed("Re-build lxml tree and retry")
      self.build_lxml_tree()
      elements = self.get_elements_by_lxml_(xpath)
      if len(elements) == 0:
        raise NoElementFoundError(xpath)
    return elements


  def get_attribute_by_selenium_(self, element, attr):
    if attr == 'alltext':
      return element.text.strip()
    else:
      return element.get_attribute(attr)

  def get_attribute_by_selenium_strong_(self, element, attr):
    val = self.get_attribute_by_selenium_(element, attr)
    if val == None: raise NoElementFoundError(xpath)
    return val

  def get_attribute_by_lxml_(self, element, attr):
    if attr == 'alltext': val = '\n'.join(element.itertext()).strip()
    elif attr == 'text': val = element.text.strip()
    elif attr == 'innerHTML': val = etree.tostring(element, pretty_print=True)
    else: val = element.get(attr)
    if val != None: val = val.strip()
    print_flushed(val)
    return val

  def get_attribute_by_lxml_strong_(self, element, attr):
    val = self.get_attribute_by_lxml_(element, attr)
    if val == None: raise NoElementFoundError(xpath)
    return val

  def get_value_by_selenium(self, xpath, attr):
    try:
      elements = self.get_elements_by_selenium_(xpath)
      if len(elements) == 0: return None
      return self.get_attribute_by_selenium_(elements[0], attr)
    except Exception as e:
      if e.__class__.__name__ in selenium_chrome_erros:
      #if e.__class__.__name__ == 'WebDriverException' or e.__class__.__name__ == 'TimeoutException':
        raise e; 
      else:
        raise WebMgrErr(e)

  def get_value_by_selenium_strong(self, xpath, attr):
    try:
      elements = self.get_elements_by_selenium_strong_(xpath)
      return self.get_attribute_by_selenium_strong_(elements[0], attr)
    except Exception as e:
      if e.__class__.__name__ in selenium_chrome_erros:
      #if e.__class__.__name__ == 'WebDriverException' or e.__class__.__name__ == 'TimeoutException':
        raise e; 
      else:
        raise WebMgrErr(e)


  def login_by_xpath(self, user_id, pwd, xpath_user_id, xpath_pwd, click_xpath):
    try:
      driver = self.get_cur_driver_()
      #print_flushed(xpath_user_id)
      #elem = driver.find_element_by_xpath(xpath_user_id)
      elem1 = driver.find_element_by_id("fm-login-id")
      print_flushed(user_id)
      print_flushed(elem1)
      elem1.send_keys(user_id)
      
      #print_flushed(xpath_pwd)
      #elem = driver.find_element_by_xpath(xpath_pwd)
      elem2 = driver.find_element_by_id("fm-login-password")
      print_flushed(pwd)
      print_flushed(elem2)
      elem2.send_keys(pwd)
      print_flushed(click_xpath)
      #driver.find_element_by_xpath(click_xpath).click()
      
      inputElement = driver.find_element_by_class_name('fm-submit')
      inputElement.click()
      time.sleep(5)
      print_flushed(driver.current_url)
    except Exception as e:
      if e.__class__.__name__ in selenium_chrome_erros:
      #if e.__class__.__name__ == 'WebDriverException' or e.__class__.__name__ == 'TimeoutException':
        raise e; 
      else:
        raise WebMgrErr(e)


  def get_value_by_lxml(self, xpath, attr):
    try:
      elements = self.get_elements_by_lxml_(xpath)
      if len(elements) == 0: return None
      return self.get_attribute_by_lxml_(elements[0], attr)
    except Exception as e:
      if e.__class__.__name__ in selenium_chrome_erros:
      #if e.__class__.__name__ == 'WebDriverException' or e.__class__.__name__ == 'TimeoutException':
        raise e; 
      else:
        raise WebMgrErr(e)

  def get_value_by_lxml_strong(self, xpath, attr):
    try:
      elements = self.get_elements_by_lxml_strong_(xpath)
      return self.get_attribute_by_lxml_strong_(elements[0], attr)
    except Exception as e:
      if e.__class__.__name__ in selenium_chrome_erros:
      #if e.__class__.__name__ == 'WebDriverException' or e.__class__.__name__ == 'TimeoutException':
        raise e; 
      else:
        raise WebMgrErr(e)

  def get_values_by_selenium(self, xpath, attr):
    try:
      elements = self.get_elements_by_selenium_(xpath)
      if len(elements) == 0: return []
      result = []
      for element in elements:
        val = self.get_attribute_by_selenium_(element, attr)
        if val != None: result.append(val)
      return result
    except Exception as e:
      if e.__class__.__name__ in selenium_chrome_erros:
      #if e.__class__.__name__ == 'WebDriverException' or e.__class__.__name__ == 'TimeoutException':
        raise e; 
      else:
        raise WebMgrErr(e)

  def send_keys_to_elements(self, xpath, txt):
    try:
      elements = self.get_elements_by_selenium_(xpath)
      num_elements = len(elements)
      if num_elements == 0: return
      action = ActionChains(self.get_cur_driver_())
      for element in elements:
        action.send_keys_to_element(element, txt)
      action.perform()
    except Exception as e:
      elements = self.get_elements_by_selenium_(xpath)
      num_elements = len(elements)
      if num_elements == 0:
        if e.__class__.__name__ in selenium_chrome_erros:
        #if e.__class__.__name__ == 'WebDriverException' or e.__class__.__name__ == 'TimeoutException':
          raise e; 
        else:
          raise WebMgrErr(e)

  def send_keys_to_elements_strong(self, xpath, txt):
    try:
      elements = self.get_elements_by_selenium_strong_(xpath)
      action = ActionChains(self.get_cur_driver_())
      for element in elements:
        action.send_keys_to_element(element, txt)
      action.perform()
    except Exception as e:
      if e.__class__.__name__ in selenium_chrome_erros:
      #if e.__class__.__name__ == 'WebDriverException' or e.__class__.__name__ == 'TimeoutException':
        raise e; 
      else:
        raise WebMgrErr(e)


  def scroll_to_bottom(self):
    try:
      self.get_cur_driver_().execute_script("window.scrollTo(0, document.body.scrollHeight)")
      time.sleep(2)

    except Exception as e:
      if e.__class__.__name__ in selenium_chrome_erros:
      #if e.__class__.__name__ == 'WebDriverException' or e.__class__.__name__ == 'TimeoutException':
        raise e; 
      else:
        raise WebMgrErr(e)



  def click_elements_repeat(self, xpath, check_xpath, time_sleep, url):
    try:
      cnt = 0
      max_retry = 30 
      while True:
        try:
          if check_xpath != '':
            check_elements = self.get_elements_by_selenium_(check_xpath)
            num_check_elements = len(check_elements)
            if num_check_elements == 0:
              raise

          elements = self.get_elements_by_selenium_(xpath)
          num_elements = len(elements)
          if num_elements == 0: 
            break
          while True:
            try:
              element = WebDriverWait(self.get_cur_driver_(), 10).until(EC.element_to_be_clickable((By.XPATH, xpath)))  
              location = element.location
              height = self.get_cur_driver_().get_window_size()['height']
              if int(location['y']) < height:
                print("window.scrollTo(0, {})".format(int(location['y'])))
                self.get_cur_driver_().execute_script("window.scrollTo(0, {})".format(int(location['y']) ))
              else: 
                print("window.scrollTo(0, {})".format(int(location['y'] - height/5)))
                self.get_cur_driver_().execute_script("window.scrollTo(0, {})".format(int(location['y'] - height/5) ))
              #print("window.scrollTo(0, {})".format(int(location['y'])))
              #self.get_cur_driver_().execute_script("window.scrollTo(0, {})".format(int(location['y']) ))
              time.sleep(4)
              try:
                element.click()
                break
              except Exception as e:
                print_flushed("Click error: ", e.__class__.__name__)
                if e.__class__.__name__ == 'ElementClickInterceptedException':
                  break;
                elif e.__class__.__name__ == 'StaleElementReferenceException':
                  pass
                else:
                  print_flushed(str(traceback.format_exc()))
                  raise
            except Exception as e:
              raise
          time.sleep(time_sleep)
        except Exception as e:
          if cnt < max_retry:
            print_flushed("cnt: ", cnt)
            self.restart(5)
            self.load(url)
            cnt = cnt + 1
            print_flushed(str(traceback.format_exc()))
          else:
            raise e
    except Exception as e:
      if e.__class__.__name__ in selenium_chrome_erros:
        raise e; 
      else:
        raise WebMgrErr(e)


  def click_elements(self, xpath, check_xpath=''):
    try:
      elements = self.get_elements_by_selenium_(xpath)
      num_elements = len(elements)
      if check_xpath != '':
        check_elements = self.get_elements_by_selenium_(check_xpath)
        num_check_elements = len(check_elements)
        if num_check_elements == 0:
          raise

      if num_elements == 0: return
      element = WebDriverWait(self.get_cur_driver_(), 10).until(EC.element_to_be_clickable((By.XPATH, xpath)))  
      location = element.location
      height = self.get_cur_driver_().get_window_size()['height']
      if int(location['y']) < height:
        print("window.scrollTo(0, {})".format(int(location['y'])))
        self.get_cur_driver_().execute_script("window.scrollTo(0, {})".format(int(location['y']) ))
      else: 
        print("window.scrollTo(0, {})".format(int(location['y'] - height/5)))
        self.get_cur_driver_().execute_script("window.scrollTo(0, {})".format(int(location['y'] - height/5)))
      time.sleep(3)
      element.click()

    except Exception as e:
      print_flushed(str(traceback.format_exc()))
      elements = self.get_elements_by_selenium_(xpath)
      num_elements = len(elements)
      if num_elements == 0:
        if e.__class__.__name__ in selenium_chrome_erros:
          raise e; 
        else:
          raise WebMgrErr(e)

  def click_elements_strong(self, xpath, check_xpath=''):
    try:
      if check_xpath != '':
        check_elements = self.get_elements_by_selenium_(check_xpath)
        num_check_elements = len(check_elements)
        if num_check_elements == 0:
          raise
      elements = self.get_elements_by_selenium_strong_(xpath)
      action = ActionChains(self.get_cur_driver_())
      for element in elements:
        action.click(element)
      action.perform()
    except Exception as e:
      if e.__class__.__name__ in selenium_chrome_erros:
        raise e; 
      else:
        raise WebMgrErr(e)

  def move_to_elements(self, xpath):
    try:
      action = ActionChains(self.get_cur_driver_())
      elements = self.get_elements_by_selenium_(xpath)
      for element in elements:
        action.move_to_element(element)
      action.perform();
    except Exception as e:
      if e.__class__.__name__ in selenium_chrome_erros:
      #if e.__class__.__name__ == 'WebDriverException' or e.__class__.__name__ == 'TimeoutException':
        raise e; 
      else:
        raise WebMgrErr(e)

  def move_to_elements_strong(self, xpath):
    try:
      elements = self.get_elements_by_selenium_(xpath)
      if len(elements) == 0: raise NoElementFoundError(xpath)
      action = ActionChains(self.get_cur_driver_())
      for element in elements:
        action.move_to_element(element).perform();
    except Exception as e:
      if e.__class__.__name__ in selenium_chrome_erros:
      #if e.__class__.__name__ == 'WebDriverException' or e.__class__.__name__ == 'TimeoutException':
        raise e; 
      else:
        raise WebMgrErr(e)

  def get_values_by_lxml(self, xpath, attr):
    try:
      elements = self.get_elements_by_lxml_(xpath)
      if len(elements) == 0: return []
      result = []
      for element in elements:
        val = self.get_attribute_by_lxml_(element, attr)
        if val != None: result.append(val)
      return result
    except Exception as e:
      if e.__class__.__name__ in selenium_chrome_erros:
      #if e.__class__.__name__ == 'WebDriverException' or e.__class__.__name__ == 'TimeoutException':
        raise e; 
      else:
        raise WebMgrErr(e)

  def get_values_by_selenium_strong(self, xpath, attr):
    try:
      elements = self.get_elements_by_selenium_strong_(xpath)
      result = []
      for element in elements:
        val = self.get_attribute_by_selenium_(element, attr)
        if val != None: result.append(val)
      if len(result) == 0: raise NoElementFoundError(xpath)
      print_flushed(result)
      return result
    except Exception as e:
      if e.__class__.__name__ in selenium_chrome_erros:
      #if e.__class__.__name__ == 'WebDriverException' or e.__class__.__name__ == 'TimeoutException':
        raise e; 
      else:
        raise WebMgrErr(e)

  def get_values_by_lxml_strong(self, xpath, attr):
    try:
      elements = self.get_elements_by_lxml_strong_(xpath)
      result = []
      for element in elements:
        val = self.get_attribute_by_lxml_(element, attr)
        if val != None: result.append(val)
      if len(result) == 0: raise NoElementFoundError(xpath)
      return result
    except Exception as e:
      if e.__class__.__name__ in selenium_chrome_erros:
      #if e.__class__.__name__ == 'WebDriverException' or e.__class__.__name__ == 'TimeoutException':
        raise e; 
      else:
        raise WebMgrErr(e)
    
  def get_key_values_by_selenium(self, xpath, kxpath, kattr, vxpath, vattr):
    try:
      elements = self.get_elements_by_selenium_(xpath)
      if len(elements) == 0: return {}
      result = {}
      for element in elements:
        kelements = element.find_elements_by_xpath(kxpath)
        if len(kelements) == 0: continue
        key = self.get_attribute_by_selenium_(kelements[0], kattr)
        if key == None: continue
        velements = element.find_elements_by_xpath(vxpath)
        if len(velements) == 0: continue
        val = self.get_attribute_by_selenium_(velements[0], vattr)
        if val == None: continue
        result[key] = val
      return result
    except Exception as e:
      if e.__class__.__name__ in selenium_chrome_erros:
      #if e.__class__.__name__ == 'WebDriverException' or e.__class__.__name__ == 'TimeoutException':
        raise e; 
      else:
        raise WebMgrErr(e)

  def get_key_values_by_lxml(self, xpath, kxpath, kattr, vxpath, vattr):
    try:
      elements = self.get_elements_by_lxml_(xpath)
      if len(elements) == 0: return {}
      result = {}
      for element in elements:
        kelements = element.xpath(kxpath)
        if len(kelements) == 0: continue
        key = self.get_attribute_by_lxml_(kelements[0], kattr)
        if key == None: continue
        velements = element.xpath(vxpath)
        if len(velements) == 0: continue
        val = self.get_attribute_by_lxml_(velements[0], vattr)
        if val == None: continue
        result[key] = val
      return result
    except Exception as e:
      if e.__class__.__name__ in selenium_chrome_erros:
      #if e.__class__.__name__ == 'WebDriverException' or e.__class__.__name__ == 'TimeoutException':
        raise e; 
      else:
        raise WebMgrErr(e)


  def get_option_values_by_lxml(self, xpath, vxpath, vattr, essential):
    try:
      elements = self.get_elements_by_lxml_(xpath)
      if len(elements) == 0: return []
      result = []
      for element in elements:
        velements = element.xpath(vxpath)
        #print(etree.tostring(element))
        #print(velements)
        if len(velements) == 0: continue
        res_tmp = []
        for velement in velements:
          #print(etree.tostring(velement))
          if essential == True:
            val = self.get_attribute_by_lxml_strong_(velement, vattr)
            if val is not None:
              res_tmp.append(val)
          else:
            val = self.get_attribute_by_lxml_(velement, vattr)
            if val is not None:
              res_tmp.append(val)
        result.append(res_tmp)
      return result
    except Exception as e:
      if e.__class__.__name__ in selenium_chrome_erros:
      #if e.__class__.__name__ == 'WebDriverException' or e.__class__.__name__ == 'TimeoutException':
        raise e; 
      else:
        raise WebMgrErr(e)



  def get_key_values_by_selenium_strong(self, xpath, kxpath, kattr, vxpath, vattr):
    try:
      elements = self.get_elements_by_selenium_strong_(xpath)
      result = {}
      for element in elements:
        kelements = element.find_elements_by_xpath(kxpath)
        if len(kelements) == 0: continue
        key = self.get_attribute_by_lxml_(kelements[0], kattr)
        if key == None: continue
        velements = element.find_elements_by_xpath(vxpath)
        if len(velements) == 0: continue
        val = self.get_attribute_by_lxml_(velements[0], vattr)
        if val == None: continue
        result[key] = val
      if len(result) == 0: raise NoElementFoundError(xpath)
      return result
    except Exception as e:
      if e.__class__.__name__ in selenium_chrome_erros:
      #if e.__class__.__name__ == 'WebDriverException' or e.__class__.__name__ == 'TimeoutException':
        raise e; 
      else:
        raise WebMgrErr(e)

  def get_key_values_by_lxml_strong(self, xpath, kxpath, kattr, vxpath, vattr):
    try:
      elements = self.get_elements_by_lxml_strong_(xpath)
      result = {}
      for element in elements:
        kelements = element.xpath(kxpath)
        if len(kelements) == 0: continue
        key = self.get_attribute_by_lxml_(kelements[0], kattr)
        if key == None: continue
        velements = element.xpath(vxpath)
        if len(velements) == 0: continue
        val = self.get_attribute_by_lxml_(velements[0], vattr)
        if val == None: continue
        result[key] = val
      if len(result) == 0: raise NoElementFoundError(xpath)
      return result
    except Exception as e:
      if e.__class__.__name__ in selenium_chrome_erros:
      #if e.__class__.__name__ == 'WebDriverException' or e.__class__.__name__ == 'TimeoutException':
        raise e; 
      else:
        raise WebMgrErr(e)

  def change_zipcode_us(self):
    try:
      print_flushed('@@@@@@@@ Get ctrf token get-address-slections.html API')
      # get token for zipcode
      url = "https://www.amazon.com/gp/glow/get-address-selections.html?deviceType=desktop&pageType=Gateway"
      def interceptor(request):
        request.method = 'POST'
      self.get_cur_driver_().request_interceptor = interceptor 
      self.load(url)
      
      print_flushed(self.get_html())
      token = self.get_html().split('CSRF_TOKEN : "')[1].split('", IDs')[0]
      print_flushed("@@@@@@@@@@ Get ctrf token {} ".format(token))
      url = 'http://www.amazon.com/gp/delivery/ajax/address-change.html?locationType=LOCATION_INPUT&zipCode=94024&storeContext=office-products&deviceType=web&pageType=Detail&actionSource=glow&almBrandId=undefined'
      def interceptor2(request):
        del request.headers['anti-csrftoken-a2z']
        request.headers['anti-csrftoken-a2z'] = token 
      self.get_cur_driver_().request_interceptor = interceptor2

      print_flushed("@@@@@@@@@@ Change zipcode address-change.html API")
      self.load(url)
     
      is_valid = '"isValidAddress":1' in self.get_html()
      print_flushed("@@@@@@@ is return valid address? {}".format(is_valid))

      def interceptor3(request):
        del request.headers['anti-csrftoken-a2z']
        request.method = 'GET'
      #site_zipcode = gvar.web_mgr.get_value_by_selenium('//*[@id="glow-ingress-line2"]', "alltext")
      self.get_cur_driver_().request_interceptor = interceptor3 

      ###########################################################
    except Exception as e:
      if e.__class__.__name__ in selenium_chrome_erros:
      #if e.__class__.__name__ == 'WebDriverException' or e.__class__.__name__ == 'TimeoutException':
        raise e; 
      else:
        raise WebMgrErr(e)

  def change_zipcode_uk(self):
    try:
      print_flushed('@@@@@@@@ Get ctrf token get-address-slections.html API')
      # get token for zipcode
      url = "https://www.amazon.co.uk/gp/glow/get-address-selections.html?deviceType=desktop&pageType=Gateway"
      def interceptor(request):
        request.method = 'POST'
      self.get_cur_driver_().request_interceptor = interceptor 
      self.load(url)
      
      print_flushed(self.get_html())
      token = self.get_html().split('CSRF_TOKEN : "')[1].split('", IDs')[0]
      print_flushed("@@@@@@@@@@ Get ctrf token {} ".format(token))
      url = 'http://www.amazon.co.uk/gp/delivery/ajax/address-change.html?locationType=LOCATION_INPUT&zipCode=TW13 6DH&storeContext=office-products&deviceType=web&pageType=Detail&actionSource=glow&almBrandId=undefined'
      def interceptor2(request):
        del request.headers['anti-csrftoken-a2z']
        request.headers['anti-csrftoken-a2z'] = token 
      self.get_cur_driver_().request_interceptor = interceptor2

      print_flushed("@@@@@@@@@@ Change zipcode address-change.html API")
      self.load(url)
     
      is_valid = '"isValidAddress":1' in self.get_html()
      print_flushed("@@@@@@@ is return valid address? {}".format(is_valid))

      def interceptor3(request):
        del request.headers['anti-csrftoken-a2z']
        request.method = 'GET'
      #site_zipcode = gvar.web_mgr.get_value_by_selenium('//*[@id="glow-ingress-line2"]', "alltext")
      self.get_cur_driver_().request_interceptor = interceptor3 

      ###########################################################
    except Exception as e:
      if e.__class__.__name__ in selenium_chrome_erros:
      #if e.__class__.__name__ == 'WebDriverException' or e.__class__.__name__ == 'TimeoutException':
        raise e; 
      else:
        raise WebMgrErr(e)

  def change_zipcode_de(self):
    try:
      print_flushed('@@@@@@@@ Get ctrf token get-address-slections.html API')
      # get token for zipcode
      url = "https://www.amazon.co.de/gp/glow/get-address-selections.html?deviceType=desktop&pageType=Gateway"
      def interceptor(request):
        request.method = 'POST'
      self.get_cur_driver_().request_interceptor = interceptor 
      self.load(url)
      
      print_flushed(self.get_html())
      token = self.get_html().split('CSRF_TOKEN : "')[1].split('", IDs')[0]
      print_flushed("@@@@@@@@@@ Get ctrf token {} ".format(token))
      url = 'http://www.amazon.co.de/gp/delivery/ajax/address-change.html?locationType=LOCATION_INPUT&zipCode=60598&storeContext=office-products&deviceType=web&pageType=Detail&actionSource=glow&almBrandId=undefined'
      def interceptor2(request):
        del request.headers['anti-csrftoken-a2z']
        request.headers['anti-csrftoken-a2z'] = token 
      self.get_cur_driver_().request_interceptor = interceptor2

      print_flushed("@@@@@@@@@@ Change zipcode address-change.html API")
      self.load(url)
     
      is_valid = '"isValidAddress":1' in self.get_html()
      print_flushed("@@@@@@@ is return valid address? {}".format(is_valid))

      def interceptor3(request):
        del request.headers['anti-csrftoken-a2z']
        request.method = 'GET'
      #site_zipcode = gvar.web_mgr.get_value_by_selenium('//*[@id="glow-ingress-line2"]', "alltext")
      self.get_cur_driver_().request_interceptor = interceptor3 

      ###########################################################
    except Exception as e:
      if e.__class__.__name__ in selenium_chrome_erros:
      #if e.__class__.__name__ == 'WebDriverException' or e.__class__.__name__ == 'TimeoutException':
        raise e; 
      else:
        raise WebMgrErr(e)






if __name__ == '__main__':
  #url = "https://www.amazon.com/gp/glow/get-address-selections.html?deviceType=desktop&pageType=Gateway&storeContext=NoStoreName"
  #headers = {'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.107'}
  #response = requests.request("POST", url, headers=headers)
  #print_flushed(response)
  #print_flushed(response.text)
  #print_flushed(response.text.split('CSRF_TOKEN : "')[1].split('", IDs')[0])
  #token = response.text.split('CSRF_TOKEN : "')[1].split('", IDs')[0]
  try:
    web_manager = WebManager()
    #web_manager.init({"chromedriver_user_agent":"PostmanRuntime/7.19.0", 'token': token})
    web_manager.init({"chromedriver_user_agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.107"})
    print_flushed('after init')
    time.sleep(10)
    #web_manager.load('https://www.amazon.com/gp/glow/get-address-selections.html?deviceType=desktop&pageType=Gateway&storeContext=NoStoreName')
    #print_flushed(web_manager.get_html().split('CSRF_TOKEN : "')[1].split('", IDs')[0])
    #web_manager.get_cur_driver_().delete_all_cookies()
    #web_manager.load('https://www.jomashop.com/watches-for-women.html?price=%7B%22from%22%3A100%2C%22to%22%3A1460%7D&manufacturer=Accutron%7CAdee+Kaye%7CAkribos+Xxiv%7CAlpina%7CAnne+Klein%7CAppetime%7CApple%7CArmani+Exchange%7CBall%7CBallast%7CBaume+Et+Mercier%7CBedat%7CBell+And+Ross%7CBertha%7CBertolucci%7CBoucheron%7CBreitling%7CBruno+Magli%7CBulova%7CBurberry%7CBurgi%7CBvlgari%7CCalibre%7CCalvin+Klein%7CCarl+F.+Bucherer%7CCartier%7CCertina%7CCharmex%7CCharriol%7CChristian+Van+Sant%7CCitizen%7CCj+Lasso%7CCoach%7CConcord%7CCorum%7CCrayo%7CD1+Milano%7CDaniel+Wellington%7CDavidoff%7CDeep+Blue%7CDior%7CDkny%7CEarth%7CEbel%7CEdox%7CEmporio+Armani%7CEmpress%7CEnicar%7CErnest+Borel%7CEterna%7CFendi%7CFerragamo%7CFerre+Milano%7CFossil%7CFurla%7CGevril%7CGlashutte%7CGucci%7CGv2+By+Gevril%7CHaurex+Italy%7CHublot%7CJacob+%26+Co.%7CJbw%7CJivago%7CJohan+Eric%7CJoshua+And+Sons%7CJunghans%7CJust+Cavalli%7CKate+Spade%7CLongines')
    #web_manager.load('https://outlet.arcteryx.com/us/en/shop/mens/beta-lt-jacket-(2019)')
    #web_manager.load('https://outlet.arcteryx.com/us/en/shop/mens/beta-sl-hybrid-jacket')
    #web_manager.load('https://outlet.arcteryx.com/us/en/shop/mens/alpha-sv-jacket-(2016)')
    #print_flushed(web_manager.get_html())
    #web_manager.build_lxml_tree()
    #print_flushed('----------------------------')

     #list option test
    #option_name_query = "//*[@class='OptionLabel__OptionSpan-ef5zek-1 dvTlFU qa--option-label-size']" 
    #option_dropdown_query = "//*[@data-testid='size-list']"#self.props['option_dropdown_query']
    ##option_value_query = "//*[@class='Size__SizeListItem-sliccf-2 uJLTY']/button"#self.props['option_value_query']
    #option_value_query = "//*[@class='Size__SizeListItem-sliccf-2 uJLTY']/button[@class='Size__SizeListValue-sliccf-3 kOtQwH    ']" 
    #option_attr = "alltext"#self.props.get('option_attr', 'alltext')
    #web_manager.click_elements("//*[@id='features']//button")
    #web_manager.click_elements("//*[@id='materials']//button")
    #web_manager.build_lxml_tree()
    #print(web_manager.get_html())
    #web_manager.get_values_by_lxml("//*[@id='features']//*[@class='featureWrapper']", 'innerHTML')
    #web_manager.get_values_by_lxml("//*[@id='materials']//*[@class='featureWrapper']", 'innerHTML')
    #option_names = web_manager.get_values_by_lxml(
    #    option_name_query, 'alltext')
    #option_values = web_manager.get_option_values_by_lxml(
    #    option_dropdown_query, option_value_query, option_attr)
    #result = {}
    #for idx, option_name in enumerate(option_names):
    #    try:
    #        result[option_name] = option_values[idx]
    #    except:
    #        pass
    #print_flushed(result)

    #web_manager.click_elements_repeat("//div[@class='load-more-button']", "//li[@class='productItem']",5 ,'https://www.jomashop.com/watches-for-women.html?price=%7B%22from%22%3A100%2C%22to%22%3A1460%7D&manufacturer=Accutron%7CAdee+Kaye%7CAkribos+Xxiv%7CAlpina%7CAnne+Klein%7CAppetime%7CApple%7CArmani+Exchange%7CBall%7CBallast%7CBaume+Et+Mercier%7CBedat%7CBell+And+Ross%7CBertha%7CBertolucci%7CBoucheron%7CBreitling%7CBruno+Magli%7CBulova%7CBurberry%7CBurgi%7CBvlgari%7CCalibre%7CCalvin+Klein%7CCarl+F.+Bucherer%7CCartier%7CCertina%7CCharmex%7CCharriol%7CChristian+Van+Sant%7CCitizen%7CCj+Lasso%7CCoach%7CConcord%7CCorum%7CCrayo%7CD1+Milano%7CDaniel+Wellington%7CDavidoff%7CDeep+Blue%7CDior%7CDkny%7CEarth%7CEbel%7CEdox%7CEmporio+Armani%7CEmpress%7CEnicar%7CErnest+Borel%7CEterna%7CFendi%7CFerragamo%7CFerre+Milano%7CFossil%7CFurla%7CGevril%7CGlashutte%7CGucci%7CGv2+By+Gevril%7CHaurex+Italy%7CHublot%7CJacob+%26+Co.%7CJbw%7CJivago%7CJohan+Eric%7CJoshua+And+Sons%7CJunghans%7CJust+Cavalli%7CKate+Spade%7CLongines')
    #print(web_manager.get_values_by_selenium("//div[@class='productItemBlock']", "data-product-scroll-target"))
    print_flushed('----------------------------')
    #print_flushed(web_manager.get_value_by_lxml("//*[@id='prodDetails']/h2", 'alltext'))
    print_flushed('after load')
  except:
    print_flushed(str(traceback.format_exc()))
  web_manager.close()

