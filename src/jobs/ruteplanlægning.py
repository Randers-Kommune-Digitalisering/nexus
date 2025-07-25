import logging
import datetime

from utils.config import NEXUS_CLIENT_ID, NEXUS_CLIENT_SECRET, NEXUS_URL
from nexus.nexus_client import NexusClient, NexusRequest, execute_nexus_flow

logger = logging.getLogger(__name__)
nexus_client = NexusClient(NEXUS_CLIENT_ID, NEXUS_CLIENT_SECRET, NEXUS_URL)


def job():
    try:
        logger.info("Starting ruteplanlægning job")
        orders = get_orders()

        # Orders is a list of dictionaries
        # An order has the key 'deliveryAddress' which is in the form below:
        # {
        #     "type": "PRIMARY",
        #     "line1": "Regimentvej 16",
        #     "zipCode": "8930",
        #     "city": "Randers NØ"
        # }

    except Exception as e:
        logger.error(f"Error in job: {e}")
        return False
    return True


def get_orders(date=datetime.date.today()):
    home = nexus_client.home_resource()

    if not home:
        raise Exception("Home resource not found")

    date = date.strftime('%Y-%m-%d')

    request1 = NexusRequest(input_response=home, link_href="preferences", method="GET")

    available_lists = execute_nexus_flow([request1])

    koersel_filter_id = None

    for list in available_lists.get('HCL_ORDER', []):
        if list.get('name', '') == 'Køreliste':
            koersel_filter_id = list.get('id', None)

    if koersel_filter_id is None:
        raise ValueError("Selvafhentning filter not found")

    request2 = NexusRequest(input_response=home, link_href="hclRegisterOrderFilterConfiguration", params={'nexusPreferenceId': koersel_filter_id}, method="GET")

    filtered_views = execute_nexus_flow([request2])

    request3 = NexusRequest(input_response=filtered_views[0], link_href="orders", method="GET")

    orders = execute_nexus_flow([request3])

    filtered_orders = []
    for order in orders:
        if (order.get('requestedDeliveryDate', '') == date and order.get('status', '') == 'ACTIVE' and order.get('handoverType', '') == 'DRIVE'):
            filtered_orders.append(order)
        else:
            logger.warning(f"Something went wrong with order: {order.get('uid', 'unknown id')} - skipping")
            continue
    return filtered_orders
