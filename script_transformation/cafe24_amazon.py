def user_defined_export(graph_mgr, node_id, node_properties):
  try:
    result = {}

    result['display'] = 'T'

    result['product_name'] = node_properties['brand'] + ' - ' + node_properties['name']

    price = node_properties['price']
    print('shit', price)
    result['price'] = str(Price.fromstring(price).amount_float)
 
    result['supply_price'] = result['price']

    result['brand_code'] = node_properties['brand']

    result['detail_image'] = node_properties['images'][0]

    result['additional_image'] = [] #node_properties['images']
    result['selling'] = 'T'

    description_title = '<div><h2 style="text-align: center;">{}</h2><br><br></div>'.format(result['product_name'])
    description_images = ''
    for image in node_properties['images']:
      description_images += '<br><img src\"{}\"></img>'.format(image)
    description_images = '<br><br><div style="padding-left: 1em;"><h2>Images</h2>' + description_images + '</div>'
    description_info = '<br><br><div style="padding-left: 1em;"><h2>Information</h2>{}</div>'.format(node_properties['information']) 

    result['description'] = description_title + description_info + description_images

    result['memo'] = node_properties.get('url', 'no url')

    result['has_option'] = 'F'
    result['custom_product_code'] = node_properties['asin']
    result['add_category_no'] = [{"category_no": 64, "recommend": "F", "new": "T"}]
    #result['url'] = node_properties['url']
  except:
    print(result.keys())
    return {}
  print(result.keys())
  print(result['price'], result['supply_price'])
  return result
  
