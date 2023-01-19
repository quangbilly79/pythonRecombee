from recombee_api_client.api_client import RecombeeClient, Region
from recombee_api_client.api_requests import AddItemProperty, SetItemValues, AddPurchase
from recombee_api_client.api_requests import RecommendItemsToItem, SearchItems, Batch, ResetDatabase
import random

NUM = 100
PROBABILITY_PURCHASED = 0.1

client = RecombeeClient('vegacorp-prod', \
        'ZyAnZu1vmc1xcIARm031wOc1QLXlOERdKxMc9LOdaVM1foCU2VHASGwoyxjZyraw', \
         region=Region.AP_SE)


# Clear the entire database
client.send(ResetDatabase())

# We will use computers as items in this example
# Computers have four properties
#   - price (floating point number)
#   - number of processor cores (integer number)
#   - description (string)
#   - image (url of computer's photo)

# Add properties of items
client.send(AddItemProperty('price', 'double'))
client.send(AddItemProperty('num-cores', 'int'))
client.send(AddItemProperty('description', 'string'))
client.send(AddItemProperty('image', 'image'))

# Prepare requests for setting a catalog of computers
requests = [SetItemValues(
    "computer-%s" % i, #itemId
    #values:
    {
      'price': random.uniform(500, 2000),
      'num-cores': random.randrange(1,9),
      'description': 'Great computer',
      'image': 'http://examplesite.com/products/computer-%s.jpg' % i
    },
    cascade_create=True   # Use cascadeCreate for creating item
                          # with given itemId if it doesn't exist
  ) for i in range(NUM)]


# Send catalog to the recommender system
client.send(Batch(requests))

# Prepare some purchases of items by users
requests = []
items = ["computer-%s" % i for i in range(NUM)]
users = ["user-%s" % i for i in range(NUM)]

for item_id in items:
    #Use cascadeCreate to create unexisting users
    purchasing_users = [user_id for user_id in users if random.random() < PROBABILITY_PURCHASED]
    requests += [AddPurchase(user_id, item_id, cascade_create=True) for user_id in purchasing_users]

# Send purchases to the recommender system
client.send(Batch(requests))

# Get 5 recommendations for user-42, who is currently viewing computer-6
# Recommend only computers that have at least 3 cores
recommended = client.send(
    RecommendItemsToItem('computer-6', 'user-42', 5, filter="'num-cores'>=3")
)
print("Recommended items with at least 3 processor cores: %s" % recommended)

# Recommend only items that are more expensive then currently viewed item (up-sell)
recommended = client.send(
    RecommendItemsToItem('computer-6', 'user-42', 5, filter="'price' > context_item[\"price\"]")
)
print("Recommended up-sell items: %s" % recommended)

# Filters, boosters and other settings can be also set in the Admin UI (admin.recombee.com)
# when scenario is specified
recommended = client.send(
  RecommendItemsToItem('computer-6', 'user-42', 5, scenario='product_detail')
  )

# Perform personalized full-text search with a user's search query (e.g. 'computers').
matches = client.send(SearchItems('user-42', 'computers', 5, scenario='search_top'))
print("Matched items: %s" % matches)