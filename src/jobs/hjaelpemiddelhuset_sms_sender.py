import logging
import random
import datetime
import pytz

from utils.config import NEXUS_CLIENT_ID, NEXUS_CLIENT_SECRET, NEXUS_URL
from nexus.nexus_client import NexusClient, NexusRequest, execute_nexus_flow
from sms_client import send_sms

logger = logging.getLogger(__name__)
nexus_client = NexusClient(NEXUS_CLIENT_ID, NEXUS_CLIENT_SECRET, NEXUS_URL)

MSG_PREFIX = "\n*** Beskded fra SMS service ***\n"
MSG_SUFFIX = "*** *** *** *** *** *** *** ***\n"
# TODO: Get the message template and door codes from a database
message_template = "Hej <navn>\nDine hjælpemidler er klar til afhentning på Hjælpemiddelhuset Kronjylland, Randers kommune.\nAgerskellet 22, 8920 Randers NV\n\nHvis du henter udenfor åbningstiden, skal du anvende nedenstående kode og ordre ID.\nDu bedes afhente dine hjælpemidler indenfor 3 dage.\n\nDin kode til hoveddøren er: <doerkode>\n\nDit ordre ID: <ordreid>\n\nHvis du vil vide mere:\nhttps://www.randers.dk/borger/socialt/hjaelpemidler-og-hjaelp/hjaelpemiddelhuset/\n\nDu er altid velkommen til at kontakte os på telefon 89158600\n\nTak for din hjælp og god dag.\n\nVenlig hilsen\nHjælpemiddelhuset Kronjylland"
door_codes = ["TESTKODE1", "TESTKODE2", "TESTKODE3", "TESTKODE4", "TESTKODE5", "TESTKODE6"]


def job():
    try:
        logger.info("Starting SMS service job")
        home = nexus_client.home_resource()
        orders = get_orders(home)

        for item in orders:
            request1 = NexusRequest(input_response=item, link_href="self", method="GET")
            order = execute_nexus_flow([request1])

            if MSG_PREFIX in order.get('deliveryNote', ''):
                logger.info(f"Order {order.get('uid', 'unknown id')} has already been handled. Skipping.")
                continue
            if order['patientId'] == 1:  # TODO: Remove this line when testing is done
                # print(json.dumps(order, indent=4))
                phone_numbers = []
                for _, value in order.get("phones", {}).items():
                    phone_numbers.append(value)

                message = ''

                if not phone_numbers:
                    logger.info(f"Order {order.get('uid', 'unknown id')} has no phone numbers")
                    message = MSG_PREFIX + "Ingen telefonnumre tilknyttet ordren." + MSG_SUFFIX
                else:
                    name = get_patient_name(home, order['patientId'])
                    if name:
                        text_message = construct_message(name, order['orderNumber'])
                        if text_message:
                            message = MSG_PREFIX
                            for phone_number in phone_numbers:
                                timestamp = f"{datetime.datetime.now(pytz.timezone('Europe/Copenhagen')).strftime('%d-%m-%Y %H:%M:%S')} - "
                                message += timestamp + send_sms(phone_number, text_message)
                            message += MSG_SUFFIX
                        else:
                            logger.error("Malformed text message!")
                            return False
                    else:
                        logger.warning(f"Order {order.get('uid', 'unknown id')} has no name")
                        message = MSG_PREFIX + "Intet navn tilknyttet ordren." + MSG_SUFFIX
                # Updating the order with a message in the delivery note
                nexus_client.put_request(order['_links']['update']['href'], json={"phones": order['phones'], "requestedDeliveryDate": order['requestedDeliveryDate'], "deliveryNote": order['deliveryNote'] + message})
            else:
                logger.info('skipping this order')
    except Exception as e:
        logger.error(f"Error in job: {e}")
        return False


def get_orders(home):
    request1 = NexusRequest(input_response=home, link_href="preferences", method="GET")

    available_lists = execute_nexus_flow([request1])

    selvafhentning_filter_id = None

    for list in available_lists.get('HCL_ORDER', []):
        if list.get('name', '') == 'Selvafhentning':
            selvafhentning_filter_id = list.get('id', None)

    if selvafhentning_filter_id is None:
        raise ValueError("Selvafhentning filter not found")

    request2 = NexusRequest(input_response=home, link_href="hclRegisterOrderFilterConfiguration", params={'nexusPreferenceId': selvafhentning_filter_id}, method="GET")

    filtered_views = execute_nexus_flow([request2])

    request3 = NexusRequest(input_response=filtered_views[0], link_href="orders", method="GET")

    orders = execute_nexus_flow([request3])

    filtered_orders = []
    for order in orders:
        requests = order.get('requests', [])
        if all(req.get('handoverType') == 'SELF_COLLECT' and req.get('status') == 'READY_FOR_DELIVERY' for req in requests) and order.get('handoverType') == 'SELF_COLLECT':
            filtered_orders.append(order)
        else:
            logger.warning(f"Something went wrong with order: {order.get('uid', 'unknown id')} - skipping")
            continue

    return filtered_orders


def get_patient_name(home, id):
    name_request = NexusRequest(input_response=home, link_href="patients", method="GET", params={'id': id})
    patient = execute_nexus_flow([name_request])
    return patient.get('firstName', None)


def construct_message(name, order_number):
    if len(door_codes) > 0 and "<orderid>" in message_template and "<doerkode>" in message_template:
        door_code = random.choice(door_codes)
        return message_template.replace("<ordreid>", order_number).replace("<doerkode>", door_code).replace("<navn>", name)
