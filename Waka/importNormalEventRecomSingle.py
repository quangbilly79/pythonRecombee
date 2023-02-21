
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

    # Rate [-1,1] nen phai? doi? tu` 1-5 sao => -1 den 1
    dfRate = spark.sql( """
          select user_id, content_id, (rate-3)/2 as normalizedRate 
          from waka.waka_pd_fact_rate
          where data_date_key < 20220701 and data_date_key >= 20220101""")
    listRate = dfRate.collect() #Co' the? dung` collect(), nhung ton' memory+
    print('Rate Collect')

    print('Import Rate Start')
    for row in listRate:
        # Rating => [-1 -> 1] => 5 sao rating => (5-3)/2
        client.send(AddRating(user_id=str(row["user_id"]), item_id=str(row["content_id"]),
                            rating=row["normalizedRate"], cascade_create=True))
    print('Import Rate Done')

    dfRead = spark.sql(
        """select user_id, content_id from waka.waka_pd_fact_reader
        where data_date_key < 20220701 and data_date_key >= 20220101""")
    listRead = dfRead.collect()
    print('Read Collect')

    print('Import Read Start')
    for row in listRead:
        client.send(AddDetailView(user_id=str(row["user_id"]), item_id=str(row["content_id"]), duration=1,
                            cascade_create=True))
    print('Import Read Done')

    dfWishlist = spark.sql(
        """select user_id, content_id from waka.waka_pd_fact_wishlist
          where data_date_key < 20220701 and data_date_key >= 20220101""")
    listWishlist = dfWishlist.collect()
    print('Wishlist Collect')

    print('Import Wishlist Start')
    for row in listWishlist:
        client.send(AddCartAddition(user_id=str(row["user_id"]), item_id=str(row["content_id"]), amount=1,
                                  cascade_create=True))
    print('Import Wishlist Done')


RecombeeImportAll()
spark.stop()




