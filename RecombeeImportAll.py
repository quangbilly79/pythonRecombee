
from recombee_api_client.api_client import RecombeeClient, Region
from recombee_api_client.exceptions import APIException
from recombee_api_client.api_requests import *

from pyspark.sql import *
from pyspark.sql.functions import *


client = RecombeeClient('vega-corp-prod', \
        '6soFUlF7dqsC9XI6dSPqXdpsyr05jKreP1fbiF4tzW9NOp3iGJqfes9BpigWi3Yc', \
         region=Region.AP_SE)

spark = SparkSession. \
    builder.getOrCreate()
def RecombeeImportAll():
    listRequest = []

    # Rate [-1,1] nen phai? doi? tu` 1-5 sao => -1 den 1
    dfRate = spark.sql("""select user_id as user_id, content_id as content_id, 
                       (rate-3)/2 as normalizedRate from waka.waka_pd_fact_rate""")
    listRate = dfRate.collect() #Co' the? dung` collect(), nhung ton' memory+
    dfRead = spark.sql(
        "select user_id, content_id, duration_time from waka.waka_pd_fact_reader")
    listRead = dfRead.collect()
    dfWishlist = spark.sql(
        """select user_id, content_id, count(*) as Amount
            from waka.waka_pd_fact_wishlist
            group by user_id, content_id""")
    listWishlist = dfWishlist.collect()


    for row in listRate:
        # Rating => [-1 -> 1] => 5 sao rating => (5-3)/2
        request = AddRating(user_id=str(row["user_id"]), item_id=str(row["content_id"]),
                            rating=row["normalizedRate"], cascade_create=True)
        listRequest.append(request)

    for row in listRead:
        request = AddDetailView(user_id=str(row["user_id"]), item_id=str(row["content_id"]), duration=row["duration_time"],
                            cascade_create=True)
        listRequest.append(request)

    for row in listWishlist:
        request = AddCartAddition(user_id=str(row["user_id"]), item_id=str(row["content_id"]), amount=row["Amount"],
                            cascade_create=True)
        listRequest.append(request)


    client.send(Batch(listRequest))
    print('Import Done')


RecombeeImportAll()
spark.stop()
#spark-submit RecombeeImportAll.py --executor-memory 6G --driver-memory 6G

# spark-submit RecombeeImportAll.py \
#     --master yarn \
#     --deploy-mode cluster \
#     --driver-memory 6g \
#     --executor-memory 6g \
#     --executor-cores 6


