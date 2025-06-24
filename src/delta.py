import os
import time
# import base64
import logging
import pathlib
import threading
import collections
# import requests_pkcs12

from datetime import datetime, timedelta, timezone

from utils.api_requests import APIClient
from utils.config import DELTA_URL, DELTA_CLIENT_ID, DELTA_CLIENT_SECRET, DELTA_REALM, DELTA_AUTH_URL, DELTA_TOP_ADM_UNIT_UUID

logger = logging.getLogger(__name__)

# Harded coded list of employment types to import TODO: FIX THIS!
employments_to_import = [
    "Assistent HK (RG_3014)",
    "Bachelor (RG_3111)",
    "Beskæft.vejl. (RG_7313)",
    "Ergoterapeut (RG_7031)",
    "Fysioterapeut (RG_7032)",
    "Hjemmehjælper (RG_7312)",
    "Kandidatudd. (RG_7014)",
    "Klin.Diætist (RG_7017)",
    "Leder N3 FOA (RG_7204)",
    "Leder N4 SHK (RG_7102)",
    "Leder N5 F/E (RG_7124)",
    "Musikterapeut (RG_3167)",
    "Plejehjemsass. (RG_7308)",
    "Plejemedhjælp. (RG_7309)",
    "Plejer",
    "Prof.bachelor (RG_7010)",
    "Psyk.Terapeut (RG_7013)",
    "Psykolog (RG_3144)",
    "Pæd.særl.stil (RG_6933)",
    "Sosu-assistent (RG_7307)",
    "Sosu-assistentelev",
    "Sosu-hj.elev (RG_7304)",
    "Sosu-hjælper (RG_7306)",
    "Sosu-hjælper (RG_7306)",
    "Specialist HK (RG_3017)",
    "Sundhedsmedhj. (RG_7315)",
    "Sygehjælper (RG_7303)",
    "Sygeplejerske (RG_7002)",
    "Uudd Sosu (RG_7316)",
    "Sygeplejestuderende"
]


class DeltaClient(APIClient):
    def __init__(self, relative_assets_path='assets/delta/'):
        self.top_adm_org_uuid = DELTA_TOP_ADM_UNIT_UUID
        self.assets_path = os.path.join(pathlib.Path(__file__).parent.resolve(), relative_assets_path)
        self.last_adm_org_list_updated = None
        self.adm_org_list = None
        self.payloads = {os.path.splitext(file)[0]: os.path.join(os.path.join(self.assets_path, 'payloads/'), file) for file in os.listdir(os.path.join(self.assets_path, 'payloads/')) if file.endswith('.json')}
        super().__init__(base_url=DELTA_URL, auth_url=DELTA_AUTH_URL, realm=DELTA_REALM, client_id=DELTA_CLIENT_ID, client_secret=DELTA_CLIENT_SECRET, add_auth_to_path=False)

    def _get_payload(self, payload_name):
        if payload_name.endswith('.json'):
            payload_name = os.path.splitext(payload_name)[0]
        payload_path = self.payloads.get(payload_name)
        if payload_path:
            with open(payload_path, 'r') as file:
                return file.read()
        else:
            logger.error(f'Payload "{payload_name}" not found.')
            return

    def _set_params(self, payload, params):
        if isinstance(payload, str):
            if isinstance(params, dict):
                for key, value in params.items():
                    key = key.replace('<', '').replace('>', '') if key and '<' in key and '>' in key else key
                    payload = payload.replace(f'<{key}>', value)
            else:
                logger.error('Params must be a dictionary.')
                return
        else:
            logger.error('Payload must be a string.')
            return
        return payload

    def _make_post_request(self, payload):
        try:
            path = '/query' if 'queries' in payload else '/graph-query' if 'graphQueries' in payload else '/history' if 'queryList' in payload else None
            if not path:
                logger.error('Payload is invalid.')
                return
            url = 'api/object' + path
            response = self.make_request(method='POST', path=url, data=payload, headers={'Content-Type': 'application/json'})
            return response
        except Exception as e:
            logger.error(f'Error making POST request: {e}')

    def _recursive_get_adm_org_units(self, adm_unit_tree_json, list_of_adm_units):
        for adm in adm_unit_tree_json:
            if 'identity' in adm:
                if 'uuid' in adm['identity']:
                    list_of_adm_units.append(adm['identity']['uuid'])
            if 'childrenObjects' in adm:
                for child in adm['childrenObjects']:
                    self._recursive_get_adm_org_units([child], list_of_adm_units)

    def _check_has_employees_and_add_sub_adm_org_units(self, adm_org_list, payload):
        try:
            adm_org_dict = {}
            for adm_org in adm_org_list:
                payload_with_params = self._set_params(payload, {'uuid': adm_org})
                if not payload_with_params:
                    logger.error('Error setting payload params.')
                    return
                json_res = self._make_post_request(payload_with_params)
                if len(json_res['graphQueryResult'][0]['instances']) > 0:
                    sub_adm_orgs = []
                    self._recursive_get_adm_org_units(json_res['graphQueryResult'][0]['instances'], sub_adm_orgs)
                    sub_adm_orgs = [e for e in sub_adm_orgs if e != adm_org]
                    adm_org_dict[adm_org] = sub_adm_orgs

            # Deletes adm. org. units with sub adm. org. units with employees
            keys_to_remove = []
            for key, value in adm_org_dict.items():
                for sub_adm_org in value:
                    if sub_adm_org in adm_org_dict.keys() and key not in keys_to_remove:
                        keys_to_remove.append(key)
                        break

            for key in keys_to_remove:
                adm_org_dict.pop(key)

            return adm_org_dict
        except Exception as e:
            logger.error(f'Error checking sub adm. org. and employees: {e}')
            return

    def _get_adm_org_list(self):
        try:
            payload = self._get_payload('adm_org_tree')
            payload_with_params = self._set_params(payload, {'uuid': self.top_adm_org_uuid})
            if not payload_with_params:
                logger.error('Error setting payload params.')
                return
            json_res = self._make_post_request(payload_with_params)
            if len(json_res['graphQueryResult'][0]['instances']) > 0:
                adm_org_list = []
                self._recursive_get_adm_org_units(json_res['graphQueryResult'][0]['instances'], adm_org_list)
                payload = self._get_payload('adm_ord_with_employees_two_layers_down')
                return self._check_has_employees_and_add_sub_adm_org_units(adm_org_list, payload)
        except Exception as e:
            logger.error(f'Error getting adm. org. list: {e}')
            return

    def _update_job(self):
        logger.info('Updating adm. org. list')
        start = time.time()
        adm_org_list = self._get_adm_org_list()
        if adm_org_list:
            self.adm_org_list = adm_org_list
            self.last_adm_org_list_updated = datetime.now()
            logger.info(f'Administrative organizations {len(self.adm_org_list)}')
            logger.info(f'Adm. org. list updated in {str(timedelta(seconds=(time.time() - start)))}')
        else:
            logger.error('Error adm. org. list not updated.')

    def _update_adm_org_list_background(self):
        logger.info('Background update')
        thread = threading.Thread(target=self._update_job)
        thread.start()

    # returns a dictionaries with the admin organization unit UUID as the key and a list of sub admin organization unit UUIDs as the value
    def get_adm_org_list(self):
        if not self.adm_org_list:
            logger.info('Foreground update')
            self._update_job()
        else:
            if self.last_adm_org_list_updated:
                # Update every hour
                if (datetime.now() - self.last_adm_org_list_updated).total_seconds() > 60 * 60:
                    self._update_adm_org_list_background()
            else:
                self._update_adm_org_list_background()
        return self.adm_org_list

    # Returns all ids in the adm org list dict as a list
    def get_all_organizations(self):
        return [item for key, values in self.get_adm_org_list().items() for item in [key] + values]

    # Returns a list of dictionaries with key 'user' containing DQ-numberand key 'organizations' containing a list of UUIDs for organizations they need access to
    def get_employees_changed(self, time_back_days=30):
        try:
            adm_org_units_with_employees = self.get_adm_org_list()
            if not adm_org_units_with_employees:
                raise Exception('Error getting adm. org. units with employees.')

            start = time.time()
            payload_changes = self._get_payload('employee_changes')

            # Delta uses UTC time
            time_back_days = timedelta(days=time_back_days)
            from_time = (datetime.now(tz=timezone.utc) - time_back_days).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + 'Z'
            to_time = datetime.now(tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + 'Z'

            payload_changes_with_params = self._set_params(payload_changes, {'fromTime': from_time, "toTime": to_time})

            json_res = self._make_post_request(payload_changes_with_params)

            changes_list = []
            # json_res = r.json()
            employee_changed_list = []

            # Function to parse the from date
            def get_from_date(employee_change):
                return datetime.strptime(employee_change['validityInterval']['from'], '%Y-%m-%d')

            # If any changes
            if len(json_res['queryResultList'][0]['registrationList']) > 0:
                # Iterate over changes
                for change in json_res['queryResultList'][0]['registrationList']:
                    # Adm. org. unit changes
                    if len(change['typeRefBiList']) > 0:
                        filtered_ec = [ec for ec in change['typeRefBiList'] if ec['value']['userKey'] == 'APOS-Types-Engagement-TypeRelation-AdmUnit' and ec["value"]["refObjIdentity"]['uuid'] in adm_org_units_with_employees.keys()]
                        ec = max(filtered_ec, key=get_from_date, default=None)
                        # Changed to an admin unit with employees
                        if ec:
                            changes_list.append({'employee': change['objectUuid'], 'admunit': ec["value"]["refObjIdentity"]['uuid'], 'regDateTime': datetime.strptime(change['regDateTime'], '%Y-%m-%dT%H:%M:%S.%fZ'), 'validityDate': datetime.strptime(change['validityDate'], '%Y-%m-%d')})

                    # State inactive changes -TODO: Is this needed?
                    # if len(change['closedStateBiList']) > 0:
                    #     ec = max(change['closedStateBiList'], key=get_from_date, default=None)
                    #     if ec:
                    #         changes_list.append({'employee': change['objectUuid'], 'regDateTime': datetime.strptime(change['regDateTime'], '%Y-%m-%dT%H:%M:%S.%fZ'), 'validityDate': datetime.strptime(change['validityDate'], '%Y-%m-%d')})

            # Split _list into a list of lists (for each employee)
            by_employee = collections.defaultdict(list)
            today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)

            for d in changes_list:
                # Only keep changes that are valid today or in the past
                if d['validityDate'] <= today:
                    by_employee[d['employee']].append(d)

            # Only keep the lastest for each employee
            employee_list = []
            for same_employee_list in list(by_employee.values()):
                same_employee_list = sorted(same_employee_list, key=lambda x: x['validityDate'], reverse=True)
                same_employee_list = [same_employee_list[0]] if same_employee_list else []
                employee_list.extend(same_employee_list)

            if len(employee_list) > 0:
                payload_employee = self._get_payload('employee_dq_number')
                for employee in employee_list:
                    dq_number = None
                    employment_type = None
                    current_adm_unit = None
                    payload_employee_with_params = self._set_params(payload_employee, {'uuid': employee['employee']})

                    json_res = self._make_post_request(payload_employee_with_params)
                    if len(json_res['queryResults'][0]['instances']) > 0:
                        first_res = json_res['queryResults'][0]['instances'][0]
                        # Check employee is active - TODO: should also get inactive employees, they need to be set inactive in Nexus
                        if first_res['state'] == 'STATE_ACTIVE' and len(first_res['typeRefs']) > 0:
                            for relation in first_res['typeRefs']:
                                if relation['userKey'] == 'APOS-Types-Engagement-TypeRelation-AdmUnit':
                                    current_adm_unit = relation['refObjIdentity']['uuid']
                                    if len(first_res["inTypeRefs"]) > 0:
                                        for ref in first_res["inTypeRefs"]:
                                            if ref['refObjTypeUserKey'] == 'APOS-Types-User':
                                                dq_number = ref['refObjIdentity']['userKey']
                                elif relation['userKey'] == 'APOS-Types-Engagement-TypeRelation-Position':
                                    employment_type = relation['refObjIdentity']['userKey']

                    if dq_number and employment_type in employments_to_import:
                        # Add employee to dictionary with key DQ number and value admin unit UUID
                        if current_adm_unit in adm_org_units_with_employees.keys():
                            employee_changed_list.append({'user': dq_number, 'organizations': [employee['admunit']] + adm_org_units_with_employees[employee['admunit']]})
                        else:
                            employee_changed_list.append({'user': dq_number, 'organizations': []})

            logger.info(f'Employees with changes {len(employee_changed_list)}')
            logger.info(f'Got employee changes in {str(timedelta(seconds=(time.time() - start)))}')
            return employee_changed_list

        except Exception as e:
            logger.error(f'Error getting employee changes: {e}')
            return
