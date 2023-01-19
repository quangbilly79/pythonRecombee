from recombee_api_client.api_client import RecombeeClient, Region
from recombee_api_client.exceptions import APIException
from recombee_api_client.api_requests import *
import pyodbc
import pandas as pd

client = RecombeeClient('vegacorp-prod', \
                        'ZyAnZu1vmc1xcIARm031wOc1QLXlOERdKxMc9LOdaVM1foCU2VHASGwoyxjZyraw', \
                        region=Region.AP_SE)

# with pyodbc.connect("DSN=Hive_Con", autocommit=True) as conn:
#     client = RecombeeClient('vegacorp-prod', \
#                             'ZyAnZu1vmc1xcIARm031wOc1QLXlOERdKxMc9LOdaVM1foCU2VHASGwoyxjZyraw', \
#                             region=Region.AP_SE)
#
#     df = pd.read_sql_query("select user_id as user_id, content_id as content_id, rate as rate from waka.waka_pd_fact_rate order by user_id", conn)
#     # print(f"DataFrame:\n{df}\n")
#     # print(f"column types:\n{df.dtypes}") # ca? 3 cot deu la int
#     RatingList = df.values.tolist()
#     print(RatingList[0:10])
#     userListSend = []
#     for userItem in RatingList:
#         request = AddRating(user_id=str(userItem[0]), item_id=userItem[1], rating=[userItem[2]],  cascade_create=True)
#         #<recombee_api_client.api_requests.add_user.AddUser object at 0x000002A93628CCD0>
#         userListSend.append(request)
#
#     client.send(Batch(userListSend))



