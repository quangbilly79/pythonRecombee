from recombee_api_client.api_client import RecombeeClient, Region
from recombee_api_client.exceptions import APIException
from recombee_api_client.api_requests import *
import random

client = RecombeeClient('vegacorp-prod', \
        'ZyAnZu1vmc1xcIARm031wOc1QLXlOERdKxMc9LOdaVM1foCU2VHASGwoyxjZyraw', \
         region=Region.AP_SE)

#Generate some random purchases of items by users
PROBABILITY_PURCHASED = 0.1
NUM = 100
purchase_requests = []

for user_id in ["user-%s" % i for i in range(NUM) ]:
  for item_id in ["item-%s" % i for i in range(NUM) ]:
    if random.random() < PROBABILITY_PURCHASED:

      request = AddPurchase(user_id, item_id, cascade_create=True)
      purchase_requests.append(request)

try:
    # Send the data to Recombee, use Batch for faster processing of larger data
    print('Send purchases')
    client.send(Batch(purchase_requests))

    # Get recommendations for user 'user-25'
    response = client.send(RecommendItemsToUser('user-25', 5))
    print("Recommended items: %s" % response)

    # User scrolled down - get next 3 recommended items
    response = client.send(RecommendNextItems(response['recommId'], 3))
    print("Next recommended items: %s" % response)

except APIException as e:
    print(e)