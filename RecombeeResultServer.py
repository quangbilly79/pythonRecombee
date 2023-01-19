from recombee_api_client.api_client import RecombeeClient, Region
from recombee_api_client.api_requests import AddItemProperty, SetItemValues, AddPurchase
from recombee_api_client.api_requests import RecommendItemsToItem, SearchItems, Batch, ResetDatabase, RecommendUsersToUser,\
RecommendItemsToUser

client = RecombeeClient('vegacorp-prod', \
        'ZyAnZu1vmc1xcIARm031wOc1QLXlOERdKxMc9LOdaVM1foCU2VHASGwoyxjZyraw', \
         region=Region.AP_SE)

# Item to Item, default/similar, for computer-0, user-0
# Recommends set of items that are somehow related to one given item, X. Typical scenario is when user
# A is viewing X. Then you may display items to the user that he might be also interested in.
# Recommend items to item request gives you Top-N such items, opstionally taking the target user A into account.
# itemid = 35954
# recommended = client.send(
#     RecommendItemsToItem(scenario="Items-to-Item", item_id=str(itemid), target_user_id="2442568",
#                          count=5, logic="recombee:default")
#                          #return_properties=True)#, filter="'userId' != \"user-83\"")
# )
# print(recommended)
# userid 2442568; itemid 35954; default: 'recomms': [{'id': '36638'}, {'id': '36539'}, {'id': '36761'}, {'id': '34130'}, {'id': '34331'}]

# Item to User, default/personal,user-0
# Based on user's past interactions (purchases, ratings, etc.) with the items, recommends top-N items'
# that are most likely to be of high value for a given user.
# The most typical use cases are recommendations at homepage, in some "Picked just for you" section or in email.
recommended = client.send(
    RecommendItemsToUser(scenario="Items-to-User", user_id="6873783", count=5, logic="recombee:default")
)
print(recommended)
testDict = recommended["recomms"]
print(testDict)
testList = list(map(lambda x: x["id"], testDict))
print(testList)
# id 2442568; default: 'recomms': [{'id': '34331'}, {'id': '34226'}, {'id': '34130'}, {'id': '33959'}, {'id': '36638'}]
# personal: recomms': giong tren

# id 7408343; default: 'recomms': [{'id': '36947'}, {'id': '36857'}, {'id': '36950'}, {'id': '36908'}, {'id': '36833'}]
# id 6873783; default: 'recomms': [{'id': '28977'}, {'id': '27781'}, {'id': '36854'}, {'id': '10990'}, {'id': '36149'}]