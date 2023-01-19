from recombee_api_client.api_client import RecombeeClient, Region
from recombee_api_client.api_requests import *
from recombee_api_client.api_requests.search_items import SearchItems

client = RecombeeClient('vegacorp-prod', \
        'ZyAnZu1vmc1xcIARm031wOc1QLXlOERdKxMc9LOdaVM1foCU2VHASGwoyxjZyraw', \
         region=Region.AP_SE)

SearchItems()