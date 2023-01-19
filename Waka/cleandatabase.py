from recombee_api_client.api_client import RecombeeClient, Region
from recombee_api_client.api_requests import *
import pyspark
client = RecombeeClient('vega-corp-prod', \
        'ra2rKUlqZwUq5MV8Icse0jOMhFLDTILrE2UTZ4JwP6Kiy9qgN0eJIrB1nmWGmyEC', \
         region=Region.AP_SE)

client1 = RecombeeClient('vegacorp-dev', \
        '4wWrZd2X7N6svVNBoFZ1E8LWKifKUVSWJUUD213FNjr9iu4X3HTDEpJSEnOAmLAu', \
         region=Region.AP_SE)
client.send(ResetDatabase()) #db vegacorp-prod
print('complete')