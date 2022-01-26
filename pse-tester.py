from engine.operators import *
from managers.graph_manager import *
from managers.web_manager import *
from util.pse_timeout import *
from PIL import Image
from io import StringIO
from io import BytesIO
import traceback
import time
import cfscrape
import cloudscraper
import base64


#class test():
#  @pse_timeout(2)
#  def init(self):
#    print("Hi!~")
#    time.sleep(5)
#    print("Bye!~")

if __name__ == '__main__':
  #gvar = GlovalVariable()
  #gvar.graph_mgr = GraphManager()
  #gvar.graph_mgr.connect("host= port= user=pse password=pse dbname=pse")
  try:
    #tmp = test()
    #tmp.init()
    link = 'https://cdn2.jomashop.com/media/catalog/product/placeholder/default/placeholder_1.png'
    print_flushed('START download image from link: ', link)
    if link[:12] == 'http://cdn2':
        scraper = cloudscraper.create_scraper()
        r = scraper.get(link)
        print(r)
        print(r.text)
        u = r.content
        print(u)
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
        print(u)

        u = u.content

    print_flushed('END download image from link: ', link)
    u = str(base64.b64encode(u))
    print(u[2:-1])


  #  client = gvar.graph_mgr.get_client('mallmalljmjm')
  #  while client is None:   
  #    client = gvar.graph_mgr.get_client('mallmalljmjm')
  #    if client is not None:
  #      client_id = client[0]
  #      client_sc = client[1]
  #      print(client_id, client_sc)
  #      break;
  #    time.sleep(10)
  
  except Exception as e:
    print(e)
    print(str(traceback.format_exc()))
    pass
  #gvar.graph_mgr.disconnect()
