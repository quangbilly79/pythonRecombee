from recombee_api_client.api_client import RecombeeClient, Region
from recombee_api_client.api_requests import *
from pyspark.sql import *
from pyspark.sql.functions import *


client = RecombeeClient('vega-corp-prod', \
        'ra2rKUlqZwUq5MV8Icse0jOMhFLDTILrE2UTZ4JwP6Kiy9qgN0eJIrB1nmWGmyEC', \
         region=Region.AP_SE)

client.send(AddItemProperty('author', 'set'))
client.send(AddItemProperty('category', 'set'))

spark = SparkSession. \
    builder.getOrCreate()
def RecombeeSetProperties():
    listPropertiesRequest=[]
    # Dung` collect_set de? agg string (vd 1 cuon' sach' co' nh category)
    sqlquery = """with cte as(
          select c.content_id, collect_set(aud.author_name) over(partition by c.content_id order by aud.author_name) as author_name,
          collect_set(cad.category_name) over(partition by c.content_id order by cad.category_name) as category_name
              from waka.content_dim as c join waka.content_author_brid as au on c.content_id = au.content_id
              join (select * from waka.author_dim where cast(author_name as integer) is null) as aud on au.author_id = aud.author_id
              join waka.content_category_brid as ca on c.content_id = ca.content_id join waka.category_dim as cad on ca.category_id = cad.category_id
          ),
          cte1 as(
          select content_id, author_name, category_name, row_number() over(partition by content_id order by size(category_name) desc) as rank from cte
          )
          select content_id, author_name, category_name from cte1 where rank = 1
          """
    dfProperties = spark.sql(sqlquery)
    print('Start Collect')
    PropertiesList = dfProperties.collect()
    print('End Collect')
    for row in PropertiesList:
        request = SetItemValues(item_id=row["content_id"],
                                values = {
                                    "author": row["author_name"],
                                    "category": row["category_name"]
                                }, cascade_create=True)
        listPropertiesRequest.append(request)
    print('Before Send')
    client.send(Batch(listPropertiesRequest))
    print('Import Done')

RecombeeSetProperties()
spark.stop()

