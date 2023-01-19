from recombee_api_client.api_client import RecombeeClient, Region
from recombee_api_client.api_requests import AddItemProperty, SetItemValues, AddPurchase
from recombee_api_client.api_requests import RecommendItemsToItem, SearchItems, Batch, ResetDatabase, RecommendUsersToUser,\
RecommendItemsToUser
import random

NUM = 100
PROBABILITY_PURCHASED = 0.1

client = RecombeeClient('vegacorp-prod', \
        'ZyAnZu1vmc1xcIARm031wOc1QLXlOERdKxMc9LOdaVM1foCU2VHASGwoyxjZyraw', \
         region=Region.AP_SE)


# Get 5 user - recommendations for user-0
recommended = client.send(
    RecommendUsersToUser(scenario="users-to-user", user_id='user-0', count=5)#, filter="'userId' != \"user-83\"")
)
print("Recommended user to user: %s" % recommended)
#'recomms': [{'id': 'user-83'}, {'id': 'user-15'}, {'id': 'user-9'}, {'id': 'user-2'}, {'id': 'user-59'}], 'numberNextRecommsCalls': 0}
#'recomms': [{'id': 'user-15'}, {'id': 'user-9'}, {'id': 'user-2'}, {'id': 'user-59'}, {'id': 'user-4'}], 'numberNextRecommsCalls': 0}

# Item to Item, default/similar, for computer-0, user-0
# Recommends set of items that are somehow related to one given item, X. Typical scenario is when user
# A is viewing X. Then you may display items to the user that he might be also interested in.
# Recommend items to item request gives you Top-N such items, optionally taking the target user A into account.
recommended = client.send(
    RecommendItemsToItem(scenario="Items-to-Item", item_id="computer-0", target_user_id="user-0",
                         count=5, logic="recombee:similar-properties")
                         #return_properties=True)#, filter="'userId' != \"user-83\"")
)
print("Recommended item to item: %s" % recommended)

# default: 'recomms': [{'id': 'computer-74'}, {'id': 'computer-14'},
# {'id': 'computer-24'}, {'id': 'computer-90'}, {'id': 'computer-38'}],
# similar:'recomms': [{'id': 'computer-90'}, {'id': 'computer-85'}, {'id': 'computer-81'},
# {'id': 'computer-74'}, {'id': 'computer-66'}],

# Item to User, default/personal,user-0
# Based on user's past interactions (purchases, ratings, etc.) with the items, recommends top-N items'
# that are most likely to be of high value for a given user.
# The most typical use cases are recommendations at homepage, in some "Picked just for you" section or in email.
recommended = client.send(
    RecommendItemsToUser(scenario="Items-to-User", user_id="user-0", count=5, logic="recombee:personal")
)
print("Recommended item to user: %s" % recommended)
# default: 'recomms': [{'id': 'computer-71'}, {'id': 'computer-23'}, {'id': 'computer-57'},
# {'id': 'computer-94'}, {'id': 'computer-3'}]
# personal: recomms': [{'id': 'computer-71'}, {'id': 'computer-23'},
# {'id': 'computer-57'}, {'id': 'computer-94'}, {'id': 'computer-3'}], 'numberNextRecommsCalls': 0}