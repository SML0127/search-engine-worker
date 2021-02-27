from engine.operators import *
from managers.graph_manager import *
from managers.web_manager import *
import traceback
import time

if __name__ == '__main__':
  gvar = GlovalVariable()
  gvar.graph_mgr = GraphManager()
  gvar.graph_mgr.connect("host=141.223.197.35 port=54320 user=pse password=pse dbname=pse")
  try:
    client = gvar.graph_mgr.get_client('mallmalljmjm')
    while client is None:   
      client = gvar.graph_mgr.get_client('mallmalljmjm')
      if client is not None:
        client_id = client[0]
        client_sc = client[1]
        print(client_id, client_sc)
        break;
      time.sleep(10)
  
  except Exception as e:
    print(e)
    print(str(traceback.format_exc()))
    pass
  gvar.graph_mgr.disconnect()
