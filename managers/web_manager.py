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
      option.add_argument('--window-size=1920x1080')
      option.add_argument('window-size=1920x1080')
      option.add_argument('--disable-gpu')
      option.add_argument('--start-maximized')
      option.add_argument('--no-proxy-server')
      option.add_argument('--no-sandbox')
      option.add_argument('--blink-settings=imagesEnabled=false')
      option.add_argument('--lang=en_US')
      option.add_argument('--disable-dev-shm-usage')
      option.add_argument('disable-dev-shm-usage')
      prefs = {"profile.managed_default_content_settings.images": 2}
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
        driver.set_page_load_timeout(60)
        driver.get('about:blank')
        driver.execute_script("Object.defineProperty(navigator, 'plugins', {get: function() {return[1, 2, 3, 4, 5];},});")
        self.drivers.append(driver)
        self.drivers_is_zipcode_reset.append(True)
        self.drivers_last_amazon_country.append("")
      self.driver_idx = 0
    except Exception as e:
      if e.__class__.__name__ == 'WebDriverException' or e.__class__.__name__ == 'TimeoutException':
        raise e; 
      else:
        raise WebMgrErr(e)

  def restart(self,sleep_time):
    try:
      print('---start restart func--')
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
      option.add_argument('--window-size=1920x1080')
      option.add_argument('window-size=1920x1080')
      option.add_argument('--disable-gpu')
      option.add_argument('--start-maximized')
      option.add_argument('--no-proxy-server')
      option.add_argument('--no-sandbox')
      option.add_argument('--blink-settings=imagesEnabled=false')
      option.add_argument('--lang=en_US')
      option.add_argument('--disable-dev-shm-usage')
      option.add_argument('disable-dev-shm-usage')
      prefs = {"profile.managed_default_content_settings.images": 2}
      option.add_experimental_option("prefs", prefs)

      #print(self.settings)
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
        driver.set_page_load_timeout(60)
        driver.get('about:blank')
        driver.execute_script("Object.defineProperty(navigator, 'plugins', {get: function() {return[1, 2, 3, 4, 5];},});")
        self.drivers.append(driver)
        self.drivers_is_zipcode_reset.append(True)
        self.drivers_last_amazon_country.append("")
      self.driver_idx = 0
      print('---end restart func--')
    except Exception as e:
      if e.__class__.__name__ == 'WebDriverException' or e.__class__.__name__ == 'TimeoutException':
        raise e; 
      else:
        raise WebMgrErr(e)

  def close(self):
    try:
      print(self.drivers)
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
      if e.__class__.__name__ == 'WebDriverException' or e.__class__.__name__ == 'TimeoutException':
        raise e; 
      else:
        raise WebMgrErr(e)


  def get_html(self):
    try:
      driver = self.get_cur_driver_()
      page_source = driver.page_source 
      return page_source
    except Exception as e:
      if e.__class__.__name__ == 'WebDriverException' or e.__class__.__name__ == 'TimeoutException':
        raise e; 
      else:
        raise WebMgrErr(e)


  def build_lxml_tree(self):
    try:
      driver = self.get_cur_driver_()
      page_source = driver.page_source 
      self.lxml_tree = html.fromstring(page_source)
      time.sleep(5)
    except Exception as e:
      if e.__class__.__name__ == 'WebDriverException' or e.__class__.__name__ == 'TimeoutException':
        raise e; 
      else:
        raise WebMgrErr(e)


  def load(self, url):
    try:
      self.rotate_driver_()
      driver = self.get_cur_driver_()
      driver.get(url)
    except Exception as e:
      if e.__class__.__name__ == 'WebDriverException' or e.__class__.__name__ == 'TimeoutException':
        raise e; 
      else:
        raise WebMgrErr(e)


  def get_current_url(self):
    try:
      driver = self.get_cur_driver_()
      return driver.current_url
    except Exception as e:
      if e.__class__.__name__ == 'WebDriverException' or e.__class__.__name__ == 'TimeoutException':
        raise e; 
      else:
        raise WebMgrErr(e)

    
  def wait_loading(self):
    try:
      driver = self.get_cur_driver_()
      WebDriverWait(driver, 10).until(lambda d: d.execute_script('return document.readyState') == 'complete')
    except Exception as e:
      if e.__class__.__name__ == 'WebDriverException' or e.__class__.__name__ == 'TimeoutException':
        raise e; 
      else:
        raise WebMgrErr(e)


  def execute_script(self, script):
    try:
      driver = self.get_cur_driver_()
      return driver.execute_script(script)
    except Exception as e:
      if e.__class__.__name__ == 'WebDriverException' or e.__class__.__name__ == 'TimeoutException':
        raise e; 
      else:
        raise WebMgrErr(e)
 
  def get_subtree_no_parent_with_style(self, xpath):
    try:
      driver = self.get_cur_driver_()
      elements = driver.find_elements_by_xpath(xpath)
      if len(elements) > 0:
        driver.execute_script(self.javascripts['style'])
        print(driver.execute_script('return arguments[0].innerHTML;', elements[0]))
        return driver.execute_script('return arguments[0].innerHTML;', elements[0])
      return ''
    except Exception as e:
      if e.__class__.__name__ == 'WebDriverException' or e.__class__.__name__ == 'TimeoutException':
        raise e; 
      else:
        raise WebMgrErr(e)
  

  def get_subtree_no_parent_with_style_strong(self, xpath):
    try:
      driver = self.get_cur_driver_()
      elements = driver.find_elements_by_xpath(xpath)
      if len(elements) == 0: raise NoElementFoundError(xpath)
      driver.execute_script(self.javascripts['style'])
      print(driver.execute_script('return arguments[0].innerHTML;', elements[0]))
      return driver.execute_script('return arguments[0].innerHTML;', elements[0])
    except Exception as e:
      if e.__class__.__name__ == 'WebDriverException' or e.__class__.__name__ == 'TimeoutException':
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
      if e.__class__.__name__ == 'WebDriverException' or e.__class__.__name__ == 'TimeoutException':
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
      if e.__class__.__name__ == 'WebDriverException' or e.__class__.__name__ == 'TimeoutException':
        raise e; 
      else:
        raise WebMgrErr(e)


  def get_elements_by_selenium_(self, xpath):
    driver = self.get_cur_driver_()
    elements = driver.find_elements_by_xpath(xpath)
    print(len(elements), xpath)
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
    print(len(elements), xpath)
    return elements

  def get_elements_by_lxml_strong_(self, xpath):
    elements = self.get_elements_by_lxml_(xpath)
    if len(elements) == 0:
      print("Re-build lxml tree and retry")
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
    print(val)
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
      if e.__class__.__name__ == 'WebDriverException' or e.__class__.__name__ == 'TimeoutException':
        raise e; 
      else:
        raise WebMgrErr(e)

  def get_value_by_selenium_strong(self, xpath, attr):
    try:
      elements = self.get_elements_by_selenium_strong_(xpath)
      return self.get_attribute_by_selenium_strong_(elements[0], attr)
    except Exception as e:
      if e.__class__.__name__ == 'WebDriverException' or e.__class__.__name__ == 'TimeoutException':
        raise e; 
      else:
        raise WebMgrErr(e)


  def login_by_xpath(self, user_id, pwd, xpath_user_id, xpath_pwd, click_xpath):
    try:
      driver = self.get_cur_driver_()
      #print(xpath_user_id)
      #elem = driver.find_element_by_xpath(xpath_user_id)
      elem1 = driver.find_element_by_id("fm-login-id")
      print(user_id)
      print(elem1)
      elem1.send_keys(user_id)
      
      #print(xpath_pwd)
      #elem = driver.find_element_by_xpath(xpath_pwd)
      elem2 = driver.find_element_by_id("fm-login-password")
      print(pwd)
      print(elem2)
      elem2.send_keys(pwd)
      print(click_xpath)
      #driver.find_element_by_xpath(click_xpath).click()
      
      inputElement = driver.find_element_by_class_name('fm-submit')
      inputElement.click()
      time.sleep(20)
      print(driver.current_url)
    except Exception as e:
      if e.__class__.__name__ == 'WebDriverException' or e.__class__.__name__ == 'TimeoutException':
        raise e; 
      else:
        raise WebMgrErr(e)


  def get_value_by_lxml(self, xpath, attr):
    try:
      elements = self.get_elements_by_lxml_(xpath)
      if len(elements) == 0: return None
      return self.get_attribute_by_lxml_(elements[0], attr)
    except Exception as e:
      if e.__class__.__name__ == 'WebDriverException' or e.__class__.__name__ == 'TimeoutException':
        raise e; 
      else:
        raise WebMgrErr(e)

  def get_value_by_lxml_strong(self, xpath, attr):
    try:
      elements = self.get_elements_by_lxml_strong_(xpath)
      return self.get_attribute_by_lxml_strong_(elements[0], attr)
    except Exception as e:
      if e.__class__.__name__ == 'WebDriverException' or e.__class__.__name__ == 'TimeoutException':
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
      if e.__class__.__name__ == 'WebDriverException' or e.__class__.__name__ == 'TimeoutException':
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
        if e.__class__.__name__ == 'WebDriverException' or e.__class__.__name__ == 'TimeoutException':
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
      if e.__class__.__name__ == 'WebDriverException' or e.__class__.__name__ == 'TimeoutException':
        raise e; 
      else:
        raise WebMgrErr(e)


  def scroll_to_bottom(self):
    try:
      self.get_cur_driver_().execute_script("window.scrollTo(0, document.body.scrollHeight)")
      time.sleep(2)

    except Exception as e:
      if e.__class__.__name__ == 'WebDriverException' or e.__class__.__name__ == 'TimeoutException':
        raise e; 
      else:
        raise WebMgrErr(e)



  def click_elements_repeat(self, xpath, time_sleep, url):
    try:
      cnt = 0
      max_retry = 5
      while True:
        try:
          self.get_cur_driver_().execute_script("window.scrollTo(0, document.body.scrollHeight)")
          time.sleep(3)
          elements = self.get_elements_by_selenium_(xpath)
          num_elements = len(elements)
          if num_elements == 0: break
          while True:
            try:
              element = WebDriverWait(self.get_cur_driver_(), 60).until(EC.element_to_be_clickable((By.XPATH, xpath)))  
              self.get_cur_driver_().execute_script("window.scrollTo(0, document.body.scrollHeight)")
              time.sleep(2)
              element.click()
              break
            except Exception as e:
              raise
          time.sleep(time_sleep)
        except Exception as e:
          if e.__class__.__name__ == 'WebDriverException' or e.__class__.__name__ == 'TimeoutException':
            if cnt < max_retry:
              self.restart(5)
              self.load(url)
              cnt = cnt + 1
          else:
            raise e
    except Exception as e:
      if e.__class__.__name__ == 'WebDriverException' or e.__class__.__name__ == 'TimeoutException':
        raise e; 
      else:
        raise WebMgrErr(e)


  def click_elements(self, xpath):
    try:
      elements = self.get_elements_by_selenium_(xpath)
      num_elements = len(elements)
      if num_elements == 0: return
      element = WebDriverWait(self.get_cur_driver_(), 2).until(EC.element_to_be_clickable((By.XPATH, xpath)))
      element.click()
      #action = ActionChains(self.get_cur_driver_())
      #for element in elements:
      #  action.click(element)
      #action.perform()
    except Exception as e:
      elements = self.get_elements_by_selenium_(xpath)
      num_elements = len(elements)
      if num_elements == 0:
        if e.__class__.__name__ == 'WebDriverException' or e.__class__.__name__ == 'TimeoutException':
          raise e; 
        else:
          raise WebMgrErr(e)

  def click_elements_strong(self, xpath):
    try:
      elements = self.get_elements_by_selenium_strong_(xpath)
      action = ActionChains(self.get_cur_driver_())
      for element in elements:
        action.click(element)
      action.perform()
    except Exception as e:
      if e.__class__.__name__ == 'WebDriverException' or e.__class__.__name__ == 'TimeoutException':
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
      if e.__class__.__name__ == 'WebDriverException' or e.__class__.__name__ == 'TimeoutException':
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
      if e.__class__.__name__ == 'WebDriverException' or e.__class__.__name__ == 'TimeoutException':
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
      if e.__class__.__name__ == 'WebDriverException' or e.__class__.__name__ == 'TimeoutException':
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
      print(result)
      return result
    except Exception as e:
      if e.__class__.__name__ == 'WebDriverException' or e.__class__.__name__ == 'TimeoutException':
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
      if e.__class__.__name__ == 'WebDriverException' or e.__class__.__name__ == 'TimeoutException':
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
      if e.__class__.__name__ == 'WebDriverException' or e.__class__.__name__ == 'TimeoutException':
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
      if e.__class__.__name__ == 'WebDriverException' or e.__class__.__name__ == 'TimeoutException':
        raise e; 
      else:
        raise WebMgrErr(e)


  def get_option_values_by_lxml(self, xpath, vxpath, vattr):
    try:
      elements = self.get_elements_by_lxml_(xpath)
      if len(elements) == 0: return []
      result = []
      for element in elements:
        velements = element.xpath(vxpath)
        if len(velements) == 0: continue
        res_tmp = []
        for velement in velements:
          val = self.get_attribute_by_lxml_(velement, vattr)
          res_tmp.append(val)
        result.append(res_tmp)
      return result
    except Exception as e:
      if e.__class__.__name__ == 'WebDriverException' or e.__class__.__name__ == 'TimeoutException':
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
      if e.__class__.__name__ == 'WebDriverException' or e.__class__.__name__ == 'TimeoutException':
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
      if e.__class__.__name__ == 'WebDriverException' or e.__class__.__name__ == 'TimeoutException':
        raise e; 
      else:
        raise WebMgrErr(e)

  def change_zipcode_us(self):
    try:
      print('@@@@@@@@ Get ctrf token get-address-slections.html API')
      # get token for zipcode
      url = "https://www.amazon.com/gp/glow/get-address-selections.html?deviceType=desktop&pageType=Gateway"
      def interceptor(request):
        request.method = 'POST'
      self.get_cur_driver_().request_interceptor = interceptor 
      self.load(url)
      time.sleep(1) 
      print(self.get_html())
      token = self.get_html().split('CSRF_TOKEN : "')[1].split('", IDs')[0]
      print("@@@@@@@@@@ Get ctrf token {} ".format(token))
      url = 'http://www.amazon.com/gp/delivery/ajax/address-change.html?locationType=LOCATION_INPUT&zipCode=94024&storeContext=office-products&deviceType=web&pageType=Detail&actionSource=glow&almBrandId=undefined'
      def interceptor2(request):
        del request.headers['anti-csrftoken-a2z']
        request.headers['anti-csrftoken-a2z'] = token 
      self.get_cur_driver_().request_interceptor = interceptor2

      print("@@@@@@@@@@ Change zipcode address-change.html API")
      self.load(url)
      time.sleep(1)
      is_valid = '"isValidAddress":1' in self.get_html()
      print("@@@@@@@ is return valid address? {}".format(is_valid))

      def interceptor3(request):
        del request.headers['anti-csrftoken-a2z']
        request.method = 'GET'
      #site_zipcode = gvar.web_mgr.get_value_by_selenium('//*[@id="glow-ingress-line2"]', "alltext")
      self.get_cur_driver_().request_interceptor = interceptor3 

      ###########################################################
    except Exception as e:
      if e.__class__.__name__ == 'WebDriverException' or e.__class__.__name__ == 'TimeoutException':
        raise e; 
      else:
        raise WebMgrErr(e)

  def change_zipcode_uk(self):
    try:
      print('@@@@@@@@ Get ctrf token get-address-slections.html API')
      # get token for zipcode
      url = "https://www.amazon.co.uk/gp/glow/get-address-selections.html?deviceType=desktop&pageType=Gateway"
      def interceptor(request):
        request.method = 'POST'
      self.get_cur_driver_().request_interceptor = interceptor 
      self.load(url)
      time.sleep(1) 
      print(self.get_html())
      token = self.get_html().split('CSRF_TOKEN : "')[1].split('", IDs')[0]
      print("@@@@@@@@@@ Get ctrf token {} ".format(token))
      url = 'http://www.amazon.co.uk/gp/delivery/ajax/address-change.html?locationType=LOCATION_INPUT&zipCode=TW13 6DH&storeContext=office-products&deviceType=web&pageType=Detail&actionSource=glow&almBrandId=undefined'
      def interceptor2(request):
        del request.headers['anti-csrftoken-a2z']
        request.headers['anti-csrftoken-a2z'] = token 
      self.get_cur_driver_().request_interceptor = interceptor2

      print("@@@@@@@@@@ Change zipcode address-change.html API")
      self.load(url)
      time.sleep(1)
      is_valid = '"isValidAddress":1' in self.get_html()
      print("@@@@@@@ is return valid address? {}".format(is_valid))

      def interceptor3(request):
        del request.headers['anti-csrftoken-a2z']
        request.method = 'GET'
      #site_zipcode = gvar.web_mgr.get_value_by_selenium('//*[@id="glow-ingress-line2"]', "alltext")
      self.get_cur_driver_().request_interceptor = interceptor3 

      ###########################################################
    except Exception as e:
      if e.__class__.__name__ == 'WebDriverException' or e.__class__.__name__ == 'TimeoutException':
        raise e; 
      else:
        raise WebMgrErr(e)

  def change_zipcode_de(self):
    try:
      print('@@@@@@@@ Get ctrf token get-address-slections.html API')
      # get token for zipcode
      url = "https://www.amazon.co.de/gp/glow/get-address-selections.html?deviceType=desktop&pageType=Gateway"
      def interceptor(request):
        request.method = 'POST'
      self.get_cur_driver_().request_interceptor = interceptor 
      self.load(url)
      time.sleep(1) 
      print(self.get_html())
      token = self.get_html().split('CSRF_TOKEN : "')[1].split('", IDs')[0]
      print("@@@@@@@@@@ Get ctrf token {} ".format(token))
      url = 'http://www.amazon.co.de/gp/delivery/ajax/address-change.html?locationType=LOCATION_INPUT&zipCode=60598&storeContext=office-products&deviceType=web&pageType=Detail&actionSource=glow&almBrandId=undefined'
      def interceptor2(request):
        del request.headers['anti-csrftoken-a2z']
        request.headers['anti-csrftoken-a2z'] = token 
      self.get_cur_driver_().request_interceptor = interceptor2

      print("@@@@@@@@@@ Change zipcode address-change.html API")
      self.load(url)
      time.sleep(1)
      is_valid = '"isValidAddress":1' in self.get_html()
      print("@@@@@@@ is return valid address? {}".format(is_valid))

      def interceptor3(request):
        del request.headers['anti-csrftoken-a2z']
        request.method = 'GET'
      #site_zipcode = gvar.web_mgr.get_value_by_selenium('//*[@id="glow-ingress-line2"]', "alltext")
      self.get_cur_driver_().request_interceptor = interceptor3 

      ###########################################################
    except Exception as e:
      if e.__class__.__name__ == 'WebDriverException' or e.__class__.__name__ == 'TimeoutException':
        raise e; 
      else:
        raise WebMgrErr(e)






if __name__ == '__main__':
  url = "https://www.amazon.com/gp/glow/get-address-selections.html?deviceType=desktop&pageType=Gateway&storeContext=NoStoreName"
  headers = {'User-Agent':'PostmanRuntime/7.19.0'}
  response = requests.request("POST", url, headers=headers)
  print(response.text.split('CSRF_TOKEN : "')[1].split('", IDs')[0])
  token = response.text.split('CSRF_TOKEN : "')[1].split('", IDs')[0]

  web_manager = WebManager()
  web_manager.init({"chromedriver_user_agent":"PostmanRuntime/7.19.0", 'token': token})
  #web_manager.load('https://www.amazon.com/gp/glow/get-address-selections.html?deviceType=desktop&pageType=Gateway&storeContext=NoStoreName')
  #print(web_manager.get_html().split('CSRF_TOKEN : "')[1].split('", IDs')[0])
  web_manager.get_cur_driver_().delete_all_cookies()
  web_manager.load('http://www.amazon.com/gp/delivery/ajax/address-change.html?locationType=LOCATION_INPUT&zipCode=94024&storeContext=office-products&deviceType=web&pageType=Detail&actionSource=glow&almBrandId=undefined')
  print(web_manager.get_html())
  web_manager.close()
#
#  try:
#    web_manager.load("https://www.amazon.com/Sensodyne-Pronamel-Whitening-Strengthening-Toothpaste/dp/B0762LYFKP?pf_rd_p=9dbbfba7-e756-51ca-b790-09e9b92beee1&pf_rd_r=EG4J8ZAJZNB9B3HBQ9G1&pd_rd_wg=W8hx6&ref_=pd_gw_ri&pd_rd_w=kynj4&pd_rd_r=6365323e-7c16-4273-a2c5-5d85b04565f5")
#    web_manager.wait_loading()
#
#    print(web_manager.get_value_by_selenium("//span[@id='productTitle']", "alltext"))
#    #print(web_manager.get_value_by_selenium("//span[@id='productTitle1']", "alltext"))
#    print(web_manager.get_value_by_selenium_strong("//span[@id='productTitle']", "alltext"))
#    #print(web_manager.get_value_by_selenium_strong("//span[@id='productTitle1']", "alltext"))
#    print(web_manager.get_values_by_selenium("//div[@id='centerCol']//li/span", "alltext"))
#    print(web_manager.get_values_by_selenium("//div[@id='centerCol']//li/span1", "alltext"))
#    print(web_manager.get_values_by_selenium_strong("//div[@id='centerCol']//li/span", "alltext"))
#    #print(web_manager.get_values_by_selenium_strong("//div[@id='centerCol']//li/span1", "alltext"))
#
#    print(web_manager.get_key_values_by_selenium("//div[@class='content']/ul/li", "./b", "alltext", ".", "alltext"))
#    print(web_manager.get_key_values_by_selenium("//div[@class='content']/ul/li1", "./b", "alltext", ".", "alltext"))
#    print(web_manager.get_key_values_by_selenium_strong("//div[@class='content']/ul/li", "./b", "alltext", ".", "alltext"))
#    #print(web_manager.get_key_values_by_selenium_strong("//div[@class='content']/ul/li1", "./b", "alltext", ".", "alltext"))
#
#    web_manager.build_lxml_tree()
#    print(web_manager.get_value_by_lxml("//span[@id='productTitle']", "alltext"))
#    #print(web_manager.get_value_by_lxml("//span[@id='productTitle1']", "alltext"))
#    print(web_manager.get_value_by_lxml_strong("//span[@id='productTitle']", "alltext"))
#    #print(web_manager.get_value_by_lxml_strong("//span[@id='productTitle1']", "alltext"))
#    print(web_manager.get_values_by_lxml("//div[@id='centerCol']//li/span", "alltext"))
#    print(web_manager.get_values_by_lxml("//div[@id='centerCol']//li/span1", "alltext"))
#    print(web_manager.get_values_by_lxml_strong("//div[@id='centerCol']//li/span", "alltext"))
#    #print(web_manager.get_values_by_lxml_strong("//div[@id='centerCol']//li/span1", "alltext"))
#
#    print(web_manager.get_key_values_by_lxml("//div[@class='content']/ul/li", "./b", "alltext", ".", "alltext"))
#    print(web_manager.get_key_values_by_lxml("//div[@class='content']/ul/li1", "./b", "alltext", ".", "alltext"))
#    print(web_manager.get_key_values_by_lxml_strong("//div[@class='content']/ul/li", "./b", "alltext", ".", "alltext"))
#    #print(web_manager.get_key_values_by_lxml_strong("//div[@class='content']/ul/li1", "./b", "alltext", ".", "alltext"))
#
#    print(web_manager.get_subtree_with_style("//ul[@class='a-unordered-list a-vertical a-spacing-none']"))
#    print(web_manager.get_subtree_with_style_strong("//ul[@class='a-unordered-list a-vertical a-spacing-none']"))
#
#  except Exception as e:
#    print(e)
#    pass
#  web_manager.close()

