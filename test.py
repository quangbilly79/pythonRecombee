from recombee_api_client.api_client import RecombeeClient, Region
from recombee_api_client.exceptions import APIException
from recombee_api_client.api_requests import *

from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *

mp = 2.323423
print(f"MP: {mp:.2f}")

# spark = SparkSession.builder.appName("CreateDataFrame").getOrCreate()
#
# # Define the schema for the DataFrame
# schema = StructType([
#     StructField("userId", IntegerType(), True),
#     StructField("userName", StringType(), True)
# ])
#
# # Define the data for the DataFrame
# data = [
#     (1, "User1"),
#     (2, "User2"),
#     (3, "User3"),
#     (4, "User4"),
#     (5, "User5")
# ]
#
# # Create the DataFrame
# df = spark.createDataFrame(data, schema)
# df.show()
# listdf = df.collect()
# for row in listdf:
#     print(f'the user id {row["userId"]} doesnt exist')

# df1 = df.orderBy(col("userId")).limit(df.count() // 2)
# df2 = df.exceptAll(df1)
# df1.show()
# df2.show()

# client = RecombeeClient('vegacorp-dev', \
#         '4wWrZd2X7N6svVNBoFZ1E8LWKifKUVSWJUUD213FNjr9iu4X3HTDEpJSEnOAmLAu', \
#          region=Region.AP_SE)
# spark = SparkSession. \
#     builder.getOrCreate()
#
# dfRate = spark.sql("""select user_id as user_id, content_id as content_id,
#                    (rate-3)/2 as normalizedRate from waka.waka_pd_fact_rate""")
# listRate = dfRate.collect()
# listRequest = []
# for row in listRate:
#     # Rating => [-1 -> 1] => 5 sao rating => (5-3)/2
#     request = AddRating(user_id=str(row["user_id"]), item_id=str(row["content_id"]), cascade_create=True)
#     print(request.item_id, request.rating)
#     listRequest.append(request)
# client.send(Batch(listRequest))
# print('Import Done')

# from pyspark.sql import SparkSession
# spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()
#
# data = [('James','Smith','M',30),('Anna','Rose','F',41),
#   ('Robert','Williams','M',62),
# ]
# columns = ["firstname","lastname","gender","salary"]
# df = spark.createDataFrame(data=data, schema = columns)
# df.show()
#
# def testFunct(row, string1):
#   print(row["firstname"])
#   print(string1)
# string1 = "abc"
# df.foreach(lambda x: testFunct(x, string1))
#
# varA = "abc"
# varA.t

# from recombee_api_client.api_client import RecombeeClient, Region
# from recombee_api_client.api_requests import AddItemProperty, SetItemValues, AddPurchase
# from recombee_api_client.api_requests import RecommendItemsToItem, SearchItems, Batch, ResetDatabase, RecommendUsersToUser,\
# RecommendItemsToUser
#
# client = RecombeeClient('vega-corp-prod', \
#         'ra2rKUlqZwUq5MV8Icse0jOMhFLDTILrE2UTZ4JwP6Kiy9qgN0eJIrB1nmWGmyEC', \
#          region=Region.AP_SE)
# scenario = "Items-to-User1" #Items-to-User
# logic = "recombee:personal" #"recombee:default"
# returnQuery = client.send(
#   RecommendItemsToUser(scenario=scenario, user_id="2897713",
#                        count=4, logic=logic)
# )
# print(returnQuery)
import json
# with open('Waka/debugRecom.txt','r+') as f:
#     totalPrecision = 0
#     totalAveragePrecision = 0
#     count = 0
#     for i in f.readlines():
#         splitList = i.split(",")
#
#         #'averagePrecision'", " 1.0, 'precision'", ' 4}\n'
#         precision = int(splitList[-1].split(":")[1][1])
#         averagePrecision = float(splitList[-2].split(":")[1])
#         totalPrecision += precision
#         totalAveragePrecision += averagePrecision
#         count += 1
#
#     print(count)
#     MAP = totalAveragePrecision / count * 100
#     MP = totalPrecision / count * 100
#     print('MAP: ', MAP)
#     print('MP', MP)

