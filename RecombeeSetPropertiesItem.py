from recombee_api_client.api_client import RecombeeClient, Region
from recombee_api_client.api_requests import *
from pyspark.sql import *
from pyspark.sql.functions import *



client = RecombeeClient('vega-corp-prod', \
        '6soFUlF7dqsC9XI6dSPqXdpsyr05jKreP1fbiF4tzW9NOp3iGJqfes9BpigWi3Yc', \
         region=Region.AP_SE)

# client.send(AddItemProperty('author', 'set'))
# client.send(AddItemProperty('category', 'set'))
# client.send(AddItemProperty('tag', 'set'))
# client.send(AddItemProperty('contentset', 'set'))

spark = SparkSession. \
    builder.getOrCreate()
def RecombeeSetProperties():
    listPropertiesRequest=[]
    # Dung` collect_set de? agg string (vd 1 cuon' sach' co' nh category)
    sqlquery = """
        select c.content_id, collect_set(cast(au.author_id as string)) as author_id, 
        collect_set(cast(ca.category_id as string)) as category_id, 
        collect_set(cast(ta.tag_id as string)) as tag_id, collect_set(cast(se.contentset_id as string)) as contentset_id
        from waka.content_dim as c join waka.content_author_brid as au on c.content_id = au.content_id
        join waka.content_category_brid as ca on c.content_id = ca.content_id
        join waka.content_tag_brid as ta on c.content_id = ta.content_id
        join waka.contentset_content_brid as se on c.content_id = se.content_id
        group by c.content_id
        """
    dfProperties = spark.sql(sqlquery)
    PropertiesList = dfProperties.collect()
    for row in PropertiesList:
        request = SetItemValues(item_id=row["content_id"],
                                values = {
                                    "author": row["author_id"],
                                    "category": row["category_id"],
                                    "tag": row["tag_id"],
                                    "contentset": row["contentset_id"]
                                }, cascade_create=True)
        listPropertiesRequest.append(request)
    client.send(Batch(listPropertiesRequest))

RecombeeSetProperties()

# spark-submit RecombeeSetPropertiesItem.py \
#     --master yarn \
#     --deploy-mode cluster \
#     --driver-memory 6g \
#     --executor-memory 6g \
#     --executor-cores 6
