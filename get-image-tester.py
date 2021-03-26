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
from PIL import Image
from io import StringIO
from io import BytesIO



link = 'https://cdn2.jomashop.com/media/catalog/product/placeholder/default/placeholder_1.png'
if link[:12] == 'https://cdn2':
  scraper = cfscrape.create_scraper()
  r = scraper.get(link)
  print(r)
  u = r.content
  print(u)
  im = Image.open(BytesIO(u))
  print(im)
elif link[:len("data:image/webp;base64,")] == "data:image/webp;base64,":
  im = link[len("data:image/webp;base64,"):]
  dimage = BytesIO()
  im = Image.open(BytesIO(base64.b64decode(im))).convert("RGB").save(dimage, "JPEG")
  u = dimage.getvalue()
elif link[:len("data:image/jpeg;base64,")] == "data:image/jpeg;base64,":
  im = link[len("data:image/jpeg;base64,"):]
  dimage = BytesIO()
  im = Image.open(BytesIO(base64.b64decode(im))).convert("RGB").save(dimage, "JPEG")
  u = dimage.getvalue()
else:
  headers = {
    'User-Agent':'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Ubuntu Chromium/79.0.3945.36 Chrome/79.0.3945.36 Safari/537.36',
    "Accept-Encoding": "gzip, deflate, br",
    "Accept": "*/*"
  }
  u = requests.request("GET", link, headers=headers)

  u = u.content


u = str(base64.b64encode(u))
print( u[2:-1])


