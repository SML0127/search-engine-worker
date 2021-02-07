from engine.operators import *
from managers.graph_manager import *
from managers.web_manager import *
import traceback


if __name__ == '__main__':
  gvar = GlovalVariable()
  gvar.web_mgr = WebManager()
  gvar.web_mgr.init(1)
  gvar.graph_mgr = GraphManager()
  gvar.graph_mgr.connect("host=141.223.197.36 port=5434 user=smlee password=smlee dbname=pse")
  try:
    gvar.task_id = 0
    gvar.exec_id = 0
    gvar.task_url = "https://www.amazon.com/DEWALT-DCS367B-Brushless-Compact-Reciprocating/dp/B01M69K91R/ref=sxbs_sxwds-stvp?pd_rd_i=B01M69K91R&pd_rd_r=9d714625-005b-4671-878a-048c1bd713a6&pd_rd_w=oL54A&pd_rd_wg=U7an0&pf_rd_p=a6d018ad-f20b-46c9-8920-433972c7d9b7&pf_rd_r=YKCRE6PA0DFT9HG703TZ&qid=1571757232&refinements=p_72:1248909011%2Cp_36:5000-%2Cp_89:DEWALT&rnid=2528832011&s=hi"
    bfs_iterator = BFSIterator()
    bfs_iterator.props = { 'id': 1, 'query': "//span[@id='productTitle']" }
    bfs_iterator.run(gvar)

    values_scrapper = ValuesScrapper()
    values_scrapper.props = { 
      'id': 2, 
      'queries': [
        {'key': 'name', "query": "//span[@id='productTitle']", "attr": "alltext"},
        {'key': 'tree', "query": "//span[@id='productTitle']", "attr": "outerHTML"}
        ] 
    }
    values_scrapper.run(gvar)

    click_operator = ClickOperator()
    click_operator.props = {
      'id': 5,
      'queries': [
        { 'query': '//li[@class="a-spacing-small item imageThumbnail a-declarative"]' }
      ]
    }
    click_operator.run(gvar)

    lists_scrapper = ListsScrapper()
    lists_scrapper.props = {
      'id': 3,
      'queries': [
        {'key': 'description', "query": "//div[@id='centerCol']//li", "attr": "alltext"},
        {'key': 'images', 'query': "//li[contains(@class, 'image item itemNo') and not(contains(@class, 'select'))]//img", "attr": "src"}
      ]
    }
    lists_scrapper.run(gvar)


    dicts_scrapper = DictsScrapper()
    dicts_scrapper.props = {
      'id': 4,
      'queries': [
        {'key': 'details', 'rows_query': "//div[@class='a-column a-span6']//tr", 'key_query': './th', 'key_attr': 'alltext', 'value_query': './td', 'value_attr': 'alltext' }
      ]
    }

    dicts_scrapper.run(gvar)

  except Exception as e:
    print(e)
    print(str(traceback.format_exc()))
    pass
  gvar.web_mgr.close()
  gvar.graph_mgr.disconnect()
