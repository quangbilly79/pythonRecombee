from recombee_api_client.api_client import RecombeeClient, Region
from recombee_api_client.api_requests import AddItemProperty, SetItemValues, AddPurchase
from recombee_api_client.api_requests import RecommendItemsToItem, SearchItems, Batch, ResetDatabase, RecommendUsersToUser,\
RecommendItemsToUser
from pyspark.sql import *
from pyspark.sql.functions import *


client = RecombeeClient('vega-corp-prod', \
        'ra2rKUlqZwUq5MV8Icse0jOMhFLDTILrE2UTZ4JwP6Kiy9qgN0eJIrB1nmWGmyEC', \
         region=Region.AP_SE)

spark = SparkSession.builder.getOrCreate()


sqlUserRead = """
select user_id, collect_set(cast(content_id as string)) as content_id from waka.waka_pd_fact_reader
where  data_date_key > 20220631 and data_date_key <= 20220731
group by user_id
order by user_id
"""

scenario = "Items-to-User" #Items-to-User
logic = "recombee:default" #"recombee:default"
dfAllUserid = spark.sql(sqlUserRead).select(col("user_id")).orderBy(col("user_id"))
listAllUserId = dfAllUserid.collect()
dictUserRecommend = {} # Dict chua' userId va` list item id cua? cac' item dc recommend cho user do'
listUserIdNotExist = [] # List chua userId k ton` tai. trong Train Set
print("step 1")
for row in listAllUserId:

    numOfReturnRecom = 4
    try:
        returnQuery = client.send(
            RecommendItemsToUser(scenario=scenario, user_id=str(row["user_id"]),
                                 count=4, logic=logic)
            )
    except:
        listUserIdNotExist.append(str(row["user_id"]))
        print(f'the user id {row["user_id"]} doesnt exist')
        continue
    #{'recommId': 'c0f4f5d50ba6cda16e25a57cd3104401', 'recomms': [{'id': '21058'}, {'id': '38045'}, {'id': '37976'},
    listItemId = returnQuery["recomms"]
    #[{'id': '21058'}, {'id': '38045'}, {'id': '37976'}, {'id': '37736'}, {'id': '37535'}]
    listItem = map(lambda x: x["id"], listItemId)
    # [24671, 36986, ...]
    dictUserRecommend.update({str(row["user_id"]): list(listItem)})
    # {1: [24671, 36986,...], 2: [23245, ...], ...}
    print(row["user_id"])
print("step 2")
with open('userRecommendRecom.txt', 'w+') as f:
    for key, value in dictUserRecommend.items():
        f.write('%s:%s\n' % (key, value))

# Lay' ds cac' userid va` itemid ma` ho. read trong thang' 7
dfUserRead = spark.sql(sqlUserRead)
listUserRead = dfUserRead.collect()
# row[userid: 1, content_id: [24671, 36986,...]]
dictUserRead = {} # Dict chua' cac' userid va` list itemid ma` ho. read trong thang' 7
for row in listUserRead:
    dictUserRead.update({str(row["user_id"]): row["content_id"]})
    # {1: [24671, 36986, ...], 2: [23245, ...], ...}
print("step 3")
with open('userReadRecom.txt', 'w+') as f:
    for key, value in dictUserRead.items():
        f.write('%s:%s\n' % (key, value))

# __Recommendations__	__Precision @k's__	        __AP@3__
# [0, 0, 1]	            [0, 0, 1/3]	            (1/3)(1/3) = 0.11
# [0, 1, 1]	            [0, 1/2, 2/3]	        (1/3)[(1/2) + (2/3)] = 0.38
# [1, 1, 1]	            [1/1, 2/2, 3/3]	        (1/3)[(1) + (2/2) + (3/3)] = 1
def averagePrecisionAtK(list1, list2):
    # Average Precision At 4
    averagePrecisionAtK = 0
    j = 1
    for i in range(4):
        if list1[i] in list2:
            averagePrecisionAtK += (1/4)*(j/(i+1))
            #print(list1[i], '-', str(j), '-', str(i+1), '-', str((1/4)*(j/(i+1))))
            j += 1
    return averagePrecisionAtK

# Tuong tu. nhu tren, nhung k quan tam den' thu' tu.
# __Recommendations__	__Precision
# [0, 0, 1]	            1/3
# [0, 1, 1]	            2/3
# [1, 1, 1]	            3/3
def Precision(list1, list2):
    return len(list(set(list1) & set(list2)))/4

dictResult = {} # Dict tong? hop. kq de? debug
totalAveragePrecision = 0 # Total AveragePrecision at 4 of all users
totalPrecision = 0 # Total Precision at 4 of all users
for userid in dictUserRead.keys():
    if userid in listUserIdNotExist:
        print(f'the user id {row["user_id"]} doesnt exist (calculate MAP part)')
        continue
    averagePrecision = averagePrecisionAtK(dictUserRecommend[userid], dictUserRead[userid])
    totalAveragePrecision += averagePrecision

    precision = Precision(dictUserRecommend[userid], dictUserRead[userid])
    totalPrecision += precision

    dictResult.update({str(userid): {"Recommend": dictUserRecommend[userid],
    "Read": dictUserRead[userid], "averagePrecision": averagePrecision, "precision": precision}})
    #{6913688:{'Recommend': ['37064', '1365', '1344', '36920'], 'Purchase':
    # ['37064', '1365', '1344', '36479'], 'precision': 0.75}

with open('debugRecom.txt', 'w+') as f:
    # Sort lai. theo precision cho de~ debug
    for key, value in sorted(dictResult.items(), key=lambda x_y: x_y[1]["precision"],reverse=True):
        f.write('%s:%s\n' % (key, value))

# Mean Average Precision and Mean Precision
MAP = totalAveragePrecision / (len(dictUserRead)-len(listUserIdNotExist)) * 100
MP = totalPrecision / (len(dictUserRead)-len(listUserIdNotExist)) * 100
with open('resultRecom.txt', 'w+') as f:
    f.write(str(scenario) + ' - ' + str(logic))
    f.write("Mean Average Precision: "+ str(MAP)+"%" + "\n" + "Mean Precision: "+ str(MP)+"%")

spark.stop()
#"recombee:default" 1 Month
# Mean Average Precision: 8.37%
# Mean Precision: 9.95%
# Scenario: Items to User, ur - default
#"recombee:personal"

#"recombee:default" 6 Month
# Mean Average Precision: 8.38
# Mean Precision: 9.95
# Scenario: Items to User, ur - default



# UserARow x R
# 0 1 2 3 4
#[4 8 4 7 4] #User A

# [0, 0, 1]	            [0, 0, 1/3]	            (1/3)(1/3) = 0.11
