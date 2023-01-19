
from recombee_api_client.api_client import RecombeeClient, Region
from recombee_api_client.exceptions import APIException
from recombee_api_client.api_requests import *

from pyspark.sql import *
from pyspark.sql.functions import *


client = RecombeeClient('vega-corp-prod', \
        'ra2rKUlqZwUq5MV8Icse0jOMhFLDTILrE2UTZ4JwP6Kiy9qgN0eJIrB1nmWGmyEC', \
         region=Region.AP_SE)

spark = SparkSession. \
    builder.getOrCreate()
def RecombeeImportAll():
    listRequest = []
    # Rate [-1,1] nen phai? doi? tu` 1-5 sao => -1 den 1
    dfRate = spark.sql( """
          select distinct user_id, content_id, (rate-3)/2 as normalizedRate 
          from waka.waka_pd_fact_rate
          where data_date_key < 20220701""")
    listRate = dfRate.collect() #Co' the? dung` collect(), nhung ton' memory+
    print('Rate Collect')
    dfRead = spark.sql(
        """select distinct user_id, content_id from waka.waka_pd_fact_reader
        where data_date_key < 20220701""")
    listRead = dfRead.collect()
    print('Read Collect')
    dfWishlist = spark.sql(
        """select distinct user_id, content_id from waka.waka_pd_fact_wishlist
          where data_date_key < 20220701""")
    listWishlist = dfWishlist.collect()
    print('Wishlist Collect')
    for row in listRead:
        request = AddDetailView(user_id=str(row["user_id"]), item_id=str(row["content_id"]), duration=1,
                            cascade_create=True)
        listRequest.append(request)

    for row in listRate:
        # Rating => [-1 -> 1] => 5 sao rating => (5-3)/2
        request = AddRating(user_id=str(row["user_id"]), item_id=str(row["content_id"]),
                            rating=row["normalizedRate"], cascade_create=True)
        listRequest.append(request)

    for row in listWishlist:
        request = AddCartAddition(user_id=str(row["user_id"]), item_id=str(row["content_id"]), amount=1,
                            cascade_create=True)
        listRequest.append(request)

    print('Before Batch Send')
    client.send(Batch(listRequest))
    print('Import Done')


RecombeeImportAll()
spark.stop()




