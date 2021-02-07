from lxml import html
import time
from lxml import etree

from selenium import webdriver
#from seleniumwire import webdriver
from selenium.common.exceptions import *
import traceback
from selenium.webdriver.common.proxy import Proxy, ProxyType
from selenium.webdriver.support.ui import WebDriverWait 
from random_user_agent.user_agent import UserAgent
from random_user_agent.params import SoftwareName, OperatingSystem
from selenium.webdriver.common.action_chains import ActionChains
import time

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
      self.settings = {}

  def init(self, settings):
    try:
      self.settings = settings
      num_driver = settings.get('num_driver', 1)
      option = webdriver.ChromeOptions()
      
      option.add_argument('--headless')
      option.add_argument('--window-size=1920x1080')
      option.add_argument('--disable-gpu')
      option.add_argument('--start-maximized')
      option.add_argument('no-proxy-server')
      option.add_argument('disable-dev-shm-usage')
      option.add_argument('no-sandbox')
      option.add_argument('blink-settings=imagesEnabled=false')
      option.add_argument('--lang=en_US')
      prefs = {"profile.managed_default_content_settings.images": 2}
      option.add_experimental_option("prefs", prefs)

      print(settings)
      driver_path = settings.get('chromedriver_path', './web_drivers/chromedriver')
      
      self.javascripts = {
        'style': './managers/SerializeWithStyles.js'
      }

      for javascript in self.javascripts.keys():
        f = open('./managers/SerializeWithStyles.js')
        code = f.read()
        f.close()
        self.javascripts[javascript] = code

      for i in range(num_driver):
        user_agent = settings['chromedriver_user_agent']
        #user_agent = user_agent_rotator.get_random_user_agent()
        option.add_argument('--user-agent={}'.format(user_agent))
        
        driver = webdriver.Chrome(driver_path, chrome_options = option)
        driver.set_page_load_timeout(100)
        driver.get('about:blank')
        driver.execute_script("Object.defineProperty(navigator, 'plugins', {get: function() {return[1, 2, 3, 4, 5];},});")
        self.drivers.append(driver)
      self.driver_idx = 0
    except Exception as e:
      raise WebMgrErr(e)

  def restart(self,sleep_time):
    try:
      for driver in self.drivers:
        driver.quit()
      self.drivers = []
      time.sleep(sleep_time)
      num_driver = self.settings.get('num_driver', 1)
      option = webdriver.ChromeOptions()
      option.add_argument('headless')
      option.add_argument('window-size=1920x1080')
      option.add_argument('disable-gpu')
      option.add_argument('no-proxy-server')
      option.add_argument('disable-dev-shm-usage')
      option.add_argument('no-sandbox')
      option.add_argument('blink-settings=imagesEnabled=false')
      option.add_argument('lang=en_US')
      option.add_argument('--accept=*/*')
      option.add_argument('accept=*/*')
      prefs = {"profile.managed_default_content_settings.images": 2}
      option.add_experimental_option("prefs", prefs)

      print(self.settings)
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
        driver = webdriver.Chrome(driver_path, chrome_options = option)
        driver.set_page_load_timeout(100)
        driver.get('about:blank')
        driver.execute_script("Object.defineProperty(navigator, 'plugins', {get: function() {return[1, 2, 3, 4, 5];},});")
        self.drivers.append(driver)
      self.driver_idx = 0
    except Exception as e:
      raise WebMgrErr(e)

  def close(self):
    try:
      for driver in self.drivers:
        driver.quit()
    except Exception as e:
      raise WebMgrErr(e)

 
  def get_cur_driver_(self):
    return self.drivers[self.driver_idx]


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
      raise WebMgrErr(e)


  def get_html(self):
    try:
      driver = self.get_cur_driver_()
      page_source = driver.page_source 
      return page_source
    except Exception as e:
      raise WebMgrErr(e)


  def build_lxml_tree(self):
    try:
      driver = self.get_cur_driver_()
      page_source = driver.page_source 
      self.lxml_tree = html.fromstring(page_source)
    except Exception as e:
      raise WebMgrErr(e)


  def load(self, url):
    try:
      self.rotate_driver_()
      driver = self.get_cur_driver_()
      driver.get(url)
    except Exception as e:
      raise WebMgrErr(e)


  def get_current_url(self):
    try:
      driver = self.get_cur_driver_()
      return driver.current_url
    except Exception as e:
      raise WebMgrErr(e)

    
  def wait_loading(self):
    try:
      driver = self.get_cur_driver_()
      WebDriverWait(driver, 10).until(lambda d: d.execute_script('return document.readyState') == 'complete')
    except Exception as e:
      raise WebMgrErr(e)


  def execute_script(self, script):
    try:
      driver = self.get_cur_driver_()
      return driver.execute_script(script)
    except Exception as e:
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
      raise WebMgrErr(e)
  

  def get_subtree_with_style_strong(self, xpath):
    try:
      driver = self.get_cur_driver_()
      elements = driver.find_elements_by_xpath(xpath)
      if len(elements) == 0: raise NoElementFoundError(xpath)
      driver.execute_script(self.javascripts['style'])
      return driver.execute_script('return arguments[0].serializeWithStyles();', elements[0])
    except Exception as e:
      raise WebMgrErr(e)


  def get_elements_by_selenium_(self, xpath):
    driver = self.get_cur_driver_()
    elements = driver.find_elements_by_xpath(xpath)
    print(len(elements), xpath)
    return elements


  def get_elements_by_selenium_strong_(self, xpath):
    elements = self.get_elements_by_selenium_(xpath)
    if len(elements) == 0:
      time.sleep(5)
      elements = self.get_elements_by_selenium_(xpath)
      if len(elements) == 0:
        raise NoElementFoundError(xpath)
    return elements

  def get_elements_by_lxml_(self, xpath):
    self.build_lxml_tree()
    elements = self.lxml_tree.xpath(xpath)
    print(len(elements), xpath)
    return elements

  def get_elements_by_lxml_strong_(self, xpath):
    elements = self.get_elements_by_lxml_(xpath)
    if len(elements) == 0:
      time.sleep(5)
      elements = self.get_elements_by_lxml_(xpath)
      raise NoElementFoundError(xpath)
    return elements


  def get_attribute_by_selenium_(self, element, attr):
    if attr == 'alltext':
      return element.text.strip()
    else:
      print(element.get_attribute(attr))
      return element.get_attribute(attr)

  def get_attribute_by_selenium_strong_(self, element, attr):
    val = self.get_attribute_by_selenium_(element, attr)
    if val == None: raise NoElementFoundError(xpath)
    return val

  def get_attribute_by_lxml_(self, element, attr):
    if attr == 'alltext': val = ''.join(element.itertext()).strip()
    elif attr == 'text': val = element.text.strip()
    elif attr == 'innerHTML': val = etree.tostring(element, pretty_print=True)
    else: val = element.get(attr)
    print(val)
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
      raise WebMgrErr(e)

  def get_value_by_selenium_strong(self, xpath, attr):
    try:
      elements = self.get_elements_by_selenium_strong_(xpath)
      return self.get_attribute_by_selenium_strong_(elements[0], attr)
    except Exception as e:
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
      raise WebMgrErr(e)


  def get_value_by_lxml(self, xpath, attr):
    try:
      elements = self.get_elements_by_lxml_(xpath)
      if len(elements) == 0: return None
      return self.get_attribute_by_lxml_(elements[0], attr)
    except Exception as e:
      raise WebMgrErr(e)

  def get_value_by_lxml_strong(self, xpath, attr):
    try:
      elements = self.get_elements_by_lxml_strong_(xpath)
      return self.get_attribute_by_lxml_strong_(elements[0], attr)
    except Exception as e:
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
      raise WebMgrErr(e) 

  def send_keys_to_elements_strong(self, xpath, txt):
    try:
      elements = self.get_elements_by_selenium_strong_(xpath)
      action = ActionChains(self.get_cur_driver_())
      for element in elements:
        action.send_keys_to_element(element, txt)
      action.perform()
    except Exception as e:
      raise WebMgrErr(e) 


  def scroll_to_bottom(self):
    try:
      self.get_cur_driver_().execute_script("window.scrollTo(0, document.body.scrollHeight)")
      time.sleep(2)

    except Exception as e:
      raise WebMgrErr(e)



  def click_elements_repeat(self, xpath, time_sleep):
    try:
      while True:
        self.get_cur_driver_().execute_script("window.scrollTo(0, document.body.scrollHeight)")
        time.sleep(3)
        elements = self.get_elements_by_selenium_(xpath)
        num_elements = len(elements)
        if num_elements == 0: break
        elements[0].click()
        time.sleep(time_sleep)

    except Exception as e:
      raise WebMgrErr(e)


  def click_elements(self, xpath):
    try:
      elements = self.get_elements_by_selenium_(xpath)
      num_elements = len(elements)
      if num_elements == 0: return
      action = ActionChains(self.get_cur_driver_())
      for element in elements:
        action.click(element)
      action.perform()
    except Exception as e:
      raise WebMgrErr(e) 

  def click_elements_strong(self, xpath):
    try:
      elements = self.get_elements_by_selenium_strong_(xpath)
      action = ActionChains(self.get_cur_driver_())
      for element in elements:
        action.click(element)
      action.perform()
    except Exception as e:
      raise WebMgrErr(e) 

  def move_to_elements(self, xpath):
    try:
      action = ActionChains(self.get_cur_driver_())
      elements = self.get_elements_by_selenium_(xpath)
      for element in elements:
        action.move_to_element(element)
      action.perform();
    except Exception as e:
      raise WebMgrErr(e) 

  def move_to_elements_strong(self, xpath):
    try:
      elements = self.get_elements_by_selenium_(xpath)
      if len(elements) == 0: raise NoElementFoundError(xpath)
      action = ActionChains(self.get_cur_driver_())
      for element in elements:
        action.move_to_element(element).perform();
    except Exception as e:
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
      raise WebMgrErr(e) 

  def get_values_by_selenium_strong(self, xpath, attr):
    try:
      elements = self.get_elements_by_selenium_strong_(xpath)
      result = []
      for element in elements:
        val = self.get_attribute_by_selenium_(element, attr)
        if val != None: result.append(val)
      if len(result) == 0: raise NoElementFoundError(xpath)
      return result
    except Exception as e:
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
      raise WebMgrErr(e)

  def get_key_values_by_selenium_strong(self, xpath, kxpath, kattr, vxpath, vattr):
    try:
      elements = self.get_elements_by_selenium_strong_(xpath)
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
      if len(result) == 0: raise NoElementFoundError(xpath)
      return result
    except Exception as e:
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
      raise WebMgrErr(e)    


if __name__ == '__main__':
  web_manager = WebManager()
  web_manager.init(2)

  try:
    web_manager.load("https://www.amazon.com/Sensodyne-Pronamel-Whitening-Strengthening-Toothpaste/dp/B0762LYFKP?pf_rd_p=9dbbfba7-e756-51ca-b790-09e9b92beee1&pf_rd_r=EG4J8ZAJZNB9B3HBQ9G1&pd_rd_wg=W8hx6&ref_=pd_gw_ri&pd_rd_w=kynj4&pd_rd_r=6365323e-7c16-4273-a2c5-5d85b04565f5")
    web_manager.wait_loading()

    print(web_manager.get_value_by_selenium("//span[@id='productTitle']", "alltext"))
    #print(web_manager.get_value_by_selenium("//span[@id='productTitle1']", "alltext"))
    print(web_manager.get_value_by_selenium_strong("//span[@id='productTitle']", "alltext"))
    #print(web_manager.get_value_by_selenium_strong("//span[@id='productTitle1']", "alltext"))
    print(web_manager.get_values_by_selenium("//div[@id='centerCol']//li/span", "alltext"))
    print(web_manager.get_values_by_selenium("//div[@id='centerCol']//li/span1", "alltext"))
    print(web_manager.get_values_by_selenium_strong("//div[@id='centerCol']//li/span", "alltext"))
    #print(web_manager.get_values_by_selenium_strong("//div[@id='centerCol']//li/span1", "alltext"))

    print(web_manager.get_key_values_by_selenium("//div[@class='content']/ul/li", "./b", "alltext", ".", "alltext"))
    print(web_manager.get_key_values_by_selenium("//div[@class='content']/ul/li1", "./b", "alltext", ".", "alltext"))
    print(web_manager.get_key_values_by_selenium_strong("//div[@class='content']/ul/li", "./b", "alltext", ".", "alltext"))
    #print(web_manager.get_key_values_by_selenium_strong("//div[@class='content']/ul/li1", "./b", "alltext", ".", "alltext"))

    web_manager.build_lxml_tree()
    print(web_manager.get_value_by_lxml("//span[@id='productTitle']", "alltext"))
    #print(web_manager.get_value_by_lxml("//span[@id='productTitle1']", "alltext"))
    print(web_manager.get_value_by_lxml_strong("//span[@id='productTitle']", "alltext"))
    #print(web_manager.get_value_by_lxml_strong("//span[@id='productTitle1']", "alltext"))
    print(web_manager.get_values_by_lxml("//div[@id='centerCol']//li/span", "alltext"))
    print(web_manager.get_values_by_lxml("//div[@id='centerCol']//li/span1", "alltext"))
    print(web_manager.get_values_by_lxml_strong("//div[@id='centerCol']//li/span", "alltext"))
    #print(web_manager.get_values_by_lxml_strong("//div[@id='centerCol']//li/span1", "alltext"))

    print(web_manager.get_key_values_by_lxml("//div[@class='content']/ul/li", "./b", "alltext", ".", "alltext"))
    print(web_manager.get_key_values_by_lxml("//div[@class='content']/ul/li1", "./b", "alltext", ".", "alltext"))
    print(web_manager.get_key_values_by_lxml_strong("//div[@class='content']/ul/li", "./b", "alltext", ".", "alltext"))
    #print(web_manager.get_key_values_by_lxml_strong("//div[@class='content']/ul/li1", "./b", "alltext", ".", "alltext"))

    print(web_manager.get_subtree_with_style("//ul[@class='a-unordered-list a-vertical a-spacing-none']"))
    print(web_manager.get_subtree_with_style_strong("//ul[@class='a-unordered-list a-vertical a-spacing-none']"))

  except Exception as e:
    print(e)
    pass
  web_manager.close()

