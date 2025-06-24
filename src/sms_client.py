import logging

from xml.etree import ElementTree as ET

from utils.api_client import APIClient
from utils.config import SMS_URL, SMS_USER, SMS_PASS
import time

logger = logging.getLogger(__name__)
sms_client = APIClient(SMS_URL, username=SMS_USER, password=SMS_PASS)

# Constants
first_number_for_mobile_phones = [2, 30, 31, 40, 41, 42, 50, 51, 52, 53, 60, 61, 71, 81, 91, 92, 93]
xml_template = '<?xml version="1.0" encoding="UTF-8"?><sms><countrycode>45</countrycode><number>{phone_number}</number><message>{message}</message></sms>'

sms_sent = {}


def send_sms(phone_number, text_message):
    try:
        cleaned_phone_number = check_if_mobile_number_and_clean(phone_number)
        if not cleaned_phone_number:
            raise ValueError("Ikke et gyldigt mobilnummer")
        if any(char in text_message for char in "&<>"):
            raise ValueError("Ulovlig charakter i besked.")
        try:
            xml_payload = xml_template.format(phone_number=cleaned_phone_number, message=text_message)

            if get_sms_sent(cleaned_phone_number) > 2:
                last_sent = get_last_sms_time(cleaned_phone_number)
                if last_sent and (time.time() - last_sent) < 86400:
                    logger.warning(f"SMS to {cleaned_phone_number} was sent less than a day ago.")
                    return f"3 SMSer allerede sendt til {cleaned_phone_number} indenfor det sidste døgn."

            response = sms_client.make_request(data=xml_payload.encode('utf-8'), headers={"Content-Type": "application/xml; charset=utf-8"})

            response_xml = response.text

            root = ET.fromstring(response_xml)
            description = root.find(".//description").text

            if description.lower() == "message handled successfully.":
                add_to_sms_sent(cleaned_phone_number)
                return f"SMS sendt til {phone_number}"
            else:
                logger.error(f"Error in SMS response: {description}")
                raise Exception("Fejl")
        except Exception as e:
            logger.error(f"Error sending SMS: {e}")
            raise Exception("Fejl")
    except Exception as e:
        logger.warning(f"Error sending SMS: {e}")
        return str(e) + f" - kunne ikke sende SMS til {phone_number}"


# Helper functions
def check_if_mobile_number_and_clean(number):
    if number.startswith("+45"):
        number = number[3:]
    elif number.startswith("45") and len(number) > 8:
        number = number[2:]
    elif number.startswith("0"):
        number = number[1:]

    if len(number) == 8 and (int(number[0]) in first_number_for_mobile_phones or int(number[:2]) in first_number_for_mobile_phones):
        return number
    else:
        return False


def add_to_sms_sent(phone_number):
    now = time.time()
    if phone_number in sms_sent:
        sms_sent[phone_number]['count'] += 1
        sms_sent[phone_number]['last_sent'] = now
    else:
        sms_sent[phone_number] = {'count': 1, 'last_sent': now}


def get_sms_sent(phone_number):
    entry = sms_sent.get(phone_number)
    if entry:
        return entry['count']
    return 0


def get_last_sms_time(phone_number):
    entry = sms_sent.get(phone_number)
    if entry:
        return entry['last_sent']
    return None
