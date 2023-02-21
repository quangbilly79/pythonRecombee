
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
def RecombeeImportAllOther():
    listRequestRate = []
    listRequestWishlist = []

    # Rate [-1,1] nen phai? doi? tu` 1-5 sao => -1 den 1
    dfRate = spark.sql( """
          select user_id, content_id, (rate-3)/2 as normalizedRate 
          from waka.waka_pd_fact_rate
          where data_date_key < 20220701 and data_date_key >= 20220101""")
    listRate = dfRate.collect() #Co' the? dung` collect(), nhung ton' memory+
    print('Rate Collect')

    for row in listRate:
        # Rating => [-1 -> 1] => 5 sao rating => (5-3)/2
        request = AddRating(user_id=str(row["user_id"]), item_id=str(row["content_id"]),
                            rating=row["normalizedRate"], cascade_create=True)
        listRequestRate.append(request)
    print('Import Rate Start')
    client.send(Batch(listRequestRate))
    print('Import Rate Done')

    dfWishlist = spark.sql(
        """select user_id, content_id from waka.waka_pd_fact_wishlist
          where data_date_key < 20220701 and data_date_key >= 20220101""")
    listWishlist = dfWishlist.collect()
    print('Wishlist Collect')

    for row in listWishlist:
        request = AddCartAddition(user_id=str(row["user_id"]), item_id=str(row["content_id"]), amount=1,
                                  cascade_create=True)
        listRequestWishlist.append(request)

    print('Import Wishlist Start')
    client.send(Batch(listRequestWishlist))
    print('Import Wishlist Done')


RecombeeImportAllOther()
spark.stop()




