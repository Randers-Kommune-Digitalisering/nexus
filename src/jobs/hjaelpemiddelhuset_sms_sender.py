import logging
import random
import datetime
import pytz

from utils.config import NEXUS_CLIENT_ID, NEXUS_CLIENT_SECRET, NEXUS_URL, DOOR_CODES
from nexus.nexus_client import NexusClient, NexusRequest, execute_nexus_flow
from sms_client import send_sms

logger = logging.getLogger(__name__)
nexus_client = NexusClient(NEXUS_CLIENT_ID, NEXUS_CLIENT_SECRET, NEXUS_URL)

MSG_PREFIX = "\n***Beskded fra SMS service "
MSG_SUFFIX = " ***\n"

message_template = """Hej {navn}
Dine hjælpemidler er klar til afhentning på Hjælpemiddelhuset Kronjylland, Randers kommune.
Agerskellet 22, 8920 Randers NV

Når du henter dine hjælpemidler, skal du anvende følgende ordre-id. Dit ordre ID: {ordreid}

Hvis du henter udenfor åbningstiden, skal du anvende nedenstående kode.
Du bedes afhente dine hjælpemidler indenfor 3 dage.

Din kode til hoveddøren er: {doerkode}

Hvis du vil vide mere:
https://www.randers.dk/borger/socialt/hjaelpemidler-og-hjaelp/hjaelpemiddelhuset/

Du er altid velkommen til at kontakte os på telefon 89158600

Tak for din hjælp og god dag.

Venlig hilsen
Hjælpemiddelhuset Kronjylland

Denne sms kan ikke besvares.
"""


def job():
    try:
        logger.info("Starting SMS service job")
        home = nexus_client.home_resource()
        orders = get_orders(home)

        for item in orders:
            request1 = NexusRequest(input_response=item, link_href="self", method="GET")
            order = execute_nexus_flow([request1])

            if not all(k in order for k in ['deliveryNote', 'requestedDeliveryDate', 'phones']):
                logger.warning(f"Order {order.get('uid', 'unknown id')} is missing required fields. Skipping.")
                continue

            delivery_note = order.get('deliveryNote', '')
            order_number = order.get('orderNumber', None)

            if MSG_PREFIX in delivery_note:
                # logger.info(f"Order {order.get('uid', 'unknown id')} has already been handled. Skipping.")
                continue

            if not order_number:
                logger.warning(f"Order {order.get('uid', 'unknown id')} has no order number. Skipping.")
                continue

            phone_numbers = []
            for _, value in order.get("phones", {}).items():
                phone_numbers.append(value)

            message = MSG_PREFIX + f"{datetime.datetime.now(pytz.timezone('Europe/Copenhagen')).strftime('%d/%m/%Y %H:%M:%S')}: "

            if not phone_numbers:
                logger.info(f"Order {order.get('uid', 'unknown id')} has no phone numbers")
                message = MSG_PREFIX + "Ingen telefonnumre tilknyttet ordren" + MSG_SUFFIX
            else:
                name = get_patient_name(home, order['patientId'])
                if name:
                    # Updating order with the same info to ensure it can be updated later
                    if nexus_client.put_request(order['_links']['update']['href'], json={"phones": order['phones'], "requestedDeliveryDate": order['requestedDeliveryDate'], "deliveryNote": delivery_note}):
                        text_message = construct_message(name, order_number)
                        if text_message:
                            for phone_number in phone_numbers:
                                message += send_sms(phone_number, text_message)
                                message += ", "
                            message = message.rstrip(', ') + MSG_SUFFIX
                        else:
                            logger.error("Malformed text message!")
                            return False
                    else:
                        logger.error(f"Failed to update order {order.get('uid', 'unknown id')}")
                        continue
                else:
                    logger.warning(f"Order {order.get('uid', 'unknown id')} has no name")
                    message = MSG_PREFIX + "Intet navn tilknyttet ordren" + MSG_SUFFIX
            # Updating the order with a message in the delivery note
            nexus_client.put_request(order['_links']['update']['href'], json={"phones": order['phones'], "requestedDeliveryDate": order['requestedDeliveryDate'], "deliveryNote": delivery_note + message})
        logger.info("SMS service job completed successfully")
        return True
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
    if len(DOOR_CODES) > 0:
        door_code = random.choice(DOOR_CODES)
        return message_template.format(navn=name, ordreid=order_number, doerkode=door_code)
