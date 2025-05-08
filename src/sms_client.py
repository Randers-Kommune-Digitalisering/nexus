import logging

from xml.etree import ElementTree as ET

from utils.api_client import APIClient
from utils.config import SMS_URL, SMS_USER, SMS_PASS

logger = logging.getLogger(__name__)
sms_client = APIClient(SMS_URL, username=SMS_USER, password=SMS_PASS)

# Constants
first_number_for_mobile_phones = [2, 30, 31, 40, 41, 42, 50, 51, 52, 53, 60, 61, 71, 81, 91, 92, 93]
xml_template = '<?xml version="1.0" encoding="UTF-8"?><sms><countrycode>45</countrycode><number>{phone_number}</number><message>{message}</message></sms>'


def send_sms(phone_number, text_message):
    try:
        cleaned_phone_number = check_if_mobile_number_and_clean(phone_number)
        if not cleaned_phone_number:
            raise ValueError("Ikke et gyldigt mobilnummer")
        if any(char in text_message for char in "&<>"):
            raise ValueError("Ulovlig charakter i besked.")
        try:
            xml_payload = xml_template.format(phone_number=cleaned_phone_number, message=text_message)

            response = sms_client.make_request(data=xml_payload.encode('utf-8'), headers={"Content-Type": "application/xml; charset=utf-8"})

            response_xml = response.text

            root = ET.fromstring(response_xml)
            description = root.find(".//description").text

            if description.lower() == "message handled successfully":
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
    elif number.startswith("45"):
        number = number[2:]
    elif number.startswith("0"):
        number = number[1:]

    if len(number) == 8 and (int(number[0]) in first_number_for_mobile_phones or int(number[:2]) in first_number_for_mobile_phones):
        return number
    else:
        return False
