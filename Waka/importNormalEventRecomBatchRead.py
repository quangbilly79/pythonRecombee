
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
def RecombeeImportRead():
    listRequestRead1 = []
    listRequestRead2 = []
    listRequestRead3 = []
    listRequestRead4 = []
    listRequestRead5 = []
    listRequestRead6 = []

    dfRead5 = spark.sql(
        """select user_id, content_id from waka.waka_pd_fact_reader
        where data_date_key <= 20220630 and data_date_key >= 20220601""")
    listRead5 = dfRead5.collect()
    print('Read6 Collect')
    for row in listRead5:
        request = AddDetailView(user_id=str(row["user_id"]), item_id=str(row["content_id"]), duration=1,
                            cascade_create=True)
        listRequestRead6.append(request)
    print('Import Read6 Start')
    client.send(Batch(listRequestRead6))
    print('Import Read6 Done')

    dfRead5 = spark.sql(
        """select user_id, content_id from waka.waka_pd_fact_reader
        where data_date_key <= 20220531 and data_date_key >= 20220501""")
    listRead5 = dfRead5.collect()
    print('Read5 Collect')
    for row in listRead5:
        request = AddDetailView(user_id=str(row["user_id"]), item_id=str(row["content_id"]), duration=1,
                                cascade_create=True)
        listRequestRead5.append(request)
    print('Import Read5 Start')
    client.send(Batch(listRequestRead5))
    print('Import Read5 Done')

    dfRead4 = spark.sql(
        """select user_id, content_id from waka.waka_pd_fact_reader
        where data_date_key <= 20220430 and data_date_key >= 20220401""")
    listRead4 = dfRead4.collect()
    print('Read4 Collect')
    for row in listRead4:
        request = AddDetailView(user_id=str(row["user_id"]), item_id=str(row["content_id"]), duration=1,
                                cascade_create=True)
        listRequestRead4.append(request)
    print('Import Read4 Start')
    client.send(Batch(listRequestRead4))
    print('Import Read4 Done')
    
    dfRead3 = spark.sql(
        """select user_id, content_id from waka.waka_pd_fact_reader
        where data_date_key <= 20220331 and data_date_key >= 20220301""")
    listRead3 = dfRead3.collect()
    print('Read3 Collect')
    for row in listRead3:
        request = AddDetailView(user_id=str(row["user_id"]), item_id=str(row["content_id"]), duration=1,
                                cascade_create=True)
        listRequestRead3.append(request)
    print('Import Read3 Start')
    client.send(Batch(listRequestRead3))
    print('Import Read3 Done')

    dfRead2 = spark.sql(
        """select user_id, content_id from waka.waka_pd_fact_reader
        where data_date_key <= 20220231 and data_date_key >= 20220201""")
    listRead2 = dfRead2.collect()
    print('Read2 Collect')
    for row in listRead2:
        request = AddDetailView(user_id=str(row["user_id"]), item_id=str(row["content_id"]), duration=1,
                                cascade_create=True)
        listRequestRead2.append(request)
    print('Import Read2 Start')
    client.send(Batch(listRequestRead2))
    print('Import Read2 Done')
    
    dfRead1 = spark.sql(
        """select user_id, content_id from waka.waka_pd_fact_reader
        where data_date_key <= 20220131 and data_date_key >= 20220101""")
    listRead1 = dfRead1.collect()
    print('Read1 Collect')
    for row in listRead1:
        request = AddDetailView(user_id=str(row["user_id"]), item_id=str(row["content_id"]), duration=1,
                                cascade_create=True)
        listRequestRead1.append(request)
    print('Import Read1 Start')
    client.send(Batch(listRequestRead1))
    print('Import Read1 Done')

RecombeeImportRead()
spark.stop()




