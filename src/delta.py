import os
import time
import base64
import logging
import pathlib
import threading
import requests_pkcs12

from collections import defaultdict
from datetime import datetime, timedelta
import json

from utils.config import TEST

logger = logging.getLogger(__name__)

# TODO: Change all the dq-numbers / username to CPR or get someone to fix username in Delta/fk org for external substitutes (eksterne vikarer)

# Harded coded list of employment types to import TODO: FIX THIS! (add to config library)
position_types_to_import = [
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
    "Sygeplejestuderende",
    # "Specialist HK (RG_3017)"  # TODO: Remove this line  - added for testing with Jette, also get a confirmation of the list
]

job_functions_to_import = [
    "Vikar Sosu-hjælper"  # TODO: Get the entire list of job functions to import
]


class DeltaClient:
    def __init__(self, cert_base64, cert_pass, base_url, top_adm_org_uuid, relative_assets_path='assets/delta/'):
        self.cert_base64 = cert_base64
        self.cert_pass = cert_pass
        self.base_url = base_url
        self.top_adm_org_uuid = top_adm_org_uuid
        self.assets_path = os.path.join(pathlib.Path(__file__).parent.resolve(), relative_assets_path)
        self.last_adm_org_list_updated = None
        self.adm_org_list = None
        self.cert_data = base64.b64decode(cert_base64)
        # self.payloads = {os.path.splitext(file)[0]: with open(os.path.join(os.path.join(self.assets_path, 'payloads/'), file) for file in os.listdir(os.path.join(self.assets_path, 'payloads/')) if file.endswith('.json'), 'r') as f: f.read()}
        self.payloads = {os.path.splitext(file)[0]: open(os.path.join(self.assets_path, 'payloads', file), 'r').read() for file in os.listdir(os.path.join(self.assets_path, 'payloads')) if file.endswith('.json')}
        self.headers = {'Content-Type': 'application/json'}

    def _get_cert_data_and_pass(self):
        if self.cert_data is not None and self.cert_pass is not None:
            return self.cert_data, self.cert_pass
        return False, False

    def _get_payload(self, payload_name):
        # if payload_name.endswith('.json'):
        #     payload_name = os.path.splitext(payload_name)[0]
        # payload_path = self.payloads.get(payload_name)
        # if payload_path:
        #     try:
        #         with open(payload_path, 'r') as file:
        #             return file.read()
        #     except Exception as e:
        #         logger.error(f'Error reading payload file: {e}')
        if payload_name in self.payloads.keys():
            return self.payloads[payload_name]
        else:
            logger.error(f'Payload "{payload_name}" not found.')

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
        cert_data, cert_pass = self._get_cert_data_and_pass()
        if cert_data and cert_pass:
            try:
                path = '/query' if 'queries' in payload else '/graph-query' if 'graphQueries' in payload else '/history' if 'queryList' in payload else None
                if not path:
                    logger.error('Payload is invalid.')
                    return
                url = self.base_url.rstrip('/') + path
                response = requests_pkcs12.post(url, data=payload, headers=self.headers, pkcs12_data=cert_data, pkcs12_password=cert_pass)
                response.raise_for_status()
                return response
            except Exception as e:
                logger.error(f'Error making POST request: {e}')
        else:
            logger.error('Certificate path or password is invalid.')
        return

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
                r = self._make_post_request(payload_with_params)
                r.raise_for_status()
                json_res = r.json()
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
            r = self._make_post_request(payload_with_params)
            r.raise_for_status()
            json_res = r.json()
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
        if TEST and not self.adm_org_list:
            logger.info('Test update')
            data_path = os.path.join(self.assets_path, 'data')
            with open(os.path.join(data_path, 'adm_org_list.json'), 'r') as json_file:
                self.adm_org_list = json.load(json_file)
                self.last_adm_org_list_updated = datetime.now()
        elif not self.adm_org_list:
            logger.info('Foreground update')
            self._update_job()
        else:
            if self.last_adm_org_list_updated:
                # Update every hour
                if (datetime.now() - self.last_adm_org_list_updated).total_seconds() > 60 * 60:
                    self._update_adm_org_list_background()
            else:
                self._update_adm_org_list_background()

        # Write adm_org_list to JSON file - for testing purposes
        data_path = os.path.join(self.assets_path, 'data')
        with open(os.path.join(data_path, 'adm_org_list.json'), 'w') as json_file:
            json.dump(self.adm_org_list, json_file)

        return self.adm_org_list

    # Returns all ids in the adm org list dict as a list
    def get_all_organizations(self):
        return [item for key, values in self.get_adm_org_list().items() for item in [key] + values]

    # Returns a list of dictionaries with key 'user' containing DQ-numberand key 'organizations' containing a list of UUIDs for organizations they need access to
    # TODO: Add more information to the return value - type of employee (intern / ekstern vikar, fastansat) or if they should have their supplier (standard leverandør) set and if they should their position (stillingsbetegnelse) set
    def get_employees_changed(self, date=datetime.today()):
        # Helper functions
        def relevant_time_or_type_of_change(employee_delta_dict, date):
            changes_to_look_for = ['APOS-Types-Engagement-TypeRelation-AdmUnit',  # AdmUnit (arbejdsplads)
                                   'APOS-Types-Engagement-TypeRelation-Position',  # Position (stillingsbetegnelse)
                                   'APOS-Types-Engagement-TypeRelation-AdditionalAssociation',  # AdditionalAssociation (forhold ved intern vikar, der indeholder arbejdsplads)
                                   'APOS-Types-Engagement-TypeRelation-Jobfunctions']  # Jobfunctions (jobfunktion, brugt for vikarer), ekstra stillingsbetegnelse

            state_change = True if employee_delta_dict.get('stateBiList', []) else False
            added_on_date = [obj for obj in employee_delta_dict.get('typeRefBiList', []) if obj.get('validityInterval', {}).get('from', '') == date.strftime("%Y-%m-%d") and obj.get('value', {}).get('userKey') in changes_to_look_for]
            removed_on_date = [obj for obj in employee_delta_dict.get('closedTypeRefBiList', []) if obj.get('value', {}).get('userKey') in changes_to_look_for]
            return any([state_change, added_on_date, removed_on_date])

        def unpack_employee_details(employee_details_response):
            query_results = employee_details_response.json().get('graphQueryResult', [])
            instances = query_results[0].get('instances', []) if query_results else []
            if instances:
                uuid, state = instances[0].get('identity', {}).get('uuid', None), instances[0].get('state', None)
                if not state or not uuid:
                    logger.error(f'No state or uuid found for employee: {uuid}')
                    return
                user, org, postion, jobs_add, jobs_remove, aa_orgs_add, aa_orgs_remove = None, None, None, [], [], [], []
                for type_ref in instances[0].get('typeRefs', []) + instances[0].get('inTypeRefs', []):
                    if type_ref.get('refObjTypeUserKey', '') == 'APOS-Types-AdditionalAssociation':
                        if type_ref.get('targetObject', {}).get('state', '') == 'STATE_ACTIVE':
                            aa_type_refs = type_ref.get('targetObject', {}).get('typeRefs', [])
                            if aa_type_refs:
                                aa_orgs_add.append(aa_type_refs[0].get('targetObject', {}).get('identity', {}).get('uuid', None))
                        elif type_ref.get('targetObject', {}).get('state', '') == 'STATE_INACTIVE':
                            aa_type_refs = type_ref.get('targetObject', {}).get('typeRefs', [])
                            if aa_type_refs:
                                aa_orgs_remove.append(aa_type_refs[0].get('targetObject', {}).get('identity', {}).get('uuid', None))
                    elif type_ref.get('refObjTypeUserKey', '') == 'APOS-Types-Jobfunction':
                        if type_ref.get('targetObject', {}).get('state', '') == 'STATE_ACTIVE':
                            jobs_add.append(type_ref.get('targetObject', {}).get('identity', {}).get('userKey', None))
                        elif type_ref.get('targetObject', {}).get('state', '') == 'STATE_INACTIVE':
                            jobs_remove.append(type_ref.get('targetObject', {}).get('identity', {}).get('userKey', None))
                    elif type_ref.get('refObjTypeUserKey', '') == 'APOS-Types-AdministrativeUnit':
                        org = type_ref.get('targetObject', {}).get('identity', {}).get('uuid', None)
                    elif type_ref.get('refObjTypeUserKey', '') == 'APOS-Types-PositionType':
                        postion = type_ref.get('targetObject', {}).get('identity', {}).get('userKey', None)
                    elif type_ref.get('refObjTypeUserKey', '') == 'APOS-Types-User':
                        if user:
                            logger.warning(f'Multiple users found for employee: {uuid}')
                            return
                        else:
                            user = type_ref.get('targetObject', {}).get('identity', {}).get('userKey', None)

                if not user:
                    # A lot of employees do not have a user and should be ignored for Nexus changes
                    logger.debug(f'No user found for employee: {uuid}')
                    return

                return {'uuid': uuid, 'state': state, 'user': user, 'position': postion, 'org': org, 'jobs_add': jobs_add, 'jobs_remove': jobs_remove, 'aa_orgs_add': aa_orgs_add, 'aa_orgs_remove': aa_orgs_remove}
            else:
                logger.error(f'No instances found in employee details response. Response. {employee_details_response}')

        # Main function
        try:
            # Get employees with relevant changes
            adm_org_units_with_employees = self.get_adm_org_list()
            if not adm_org_units_with_employees:
                raise Exception('Error getting adm. org. units with employees.')

            payload_employee_changes = self._get_payload('history')
            if not payload_employee_changes:
                raise Exception('Error getting payload for employee changes.')

            payload_employee_changes_with_params = self._set_params(payload_employee_changes, {'validFrom':  date.strftime("%Y-%m-%d"), "objType": "APOS-Types-Engagement"})
            if not payload_employee_changes_with_params:
                raise Exception('Error setting params for employee changes.')

            res_employee_changes = self._make_post_request(payload_employee_changes_with_params)
            if res_employee_changes:
                query_results = res_employee_changes.json().get('queryResultList', [])
                registrations = query_results[0].get('registrationList', []) if query_results else []
                # filter out employee changes which are valid on a later date than 'date'
                all_employee_changes = [reg for reg in registrations if reg.get('validityDate', None) == date.strftime("%Y-%m-%d")]
                employees_with_relevant_changes = [change['objectUuid'] for change in all_employee_changes if relevant_time_or_type_of_change(change, date)]
            else:
                raise Exception('Error getting employee changes.')

            # Get employee details
            employees_to_change = defaultdict(list)
            for employee_uuid in employees_with_relevant_changes:
                employee_details_payload = self._get_payload('employee_details')
                if not employee_details_payload:
                    raise Exception('Error getting payload for employee details.')

                employee_details_payload_with_params = self._set_params(employee_details_payload, {'uuid': employee_uuid})
                if not employee_details_payload_with_params:
                    raise Exception('Error setting params for employee details.')

                res_employee_details = self._make_post_request(employee_details_payload_with_params)
                if res_employee_details:
                    employee_details = unpack_employee_details(res_employee_details)
                    if employee_details:
                        if employee_details['state'] == 'STATE_ACTIVE':
                            orgs = []
                            if any([job in job_functions_to_import for job in employee_details['jobs_add']]):
                                # Internal substitutes (interne vikarer) has an additional associations and job functions
                                if employee_details['aa_orgs_add']:
                                    for aa_org in employee_details['aa_orgs_add']:
                                        if aa_org in adm_org_units_with_employees.keys():
                                            orgs = orgs + [aa_org] + adm_org_units_with_employees[aa_org]
                                # External substitutes (eksterne vikarer) has no additional associations but job functions and an organization (administrativ enhed)
                                elif employee_details['org'] in adm_org_units_with_employees.keys():
                                    orgs = orgs + [employee_details['org']] + adm_org_units_with_employees[employee_details['org']]

                            if employee_details['position'] in position_types_to_import:
                                # Regular employees has a position and an organization (administrativ enhed), internal substitutes (interne vikarer) can be regular employees
                                if employee_details['org'] in adm_org_units_with_employees.keys():
                                    orgs = orgs + [employee_details['org']] + adm_org_units_with_employees[employee_details['org']]

                            # Filter if connected to Nexus
                            if any([any([job in job_functions_to_import for job in employee_details['jobs_add']]),
                                    any([org in adm_org_units_with_employees.keys() for org in employee_details['aa_orgs_add']]),
                                    any([job in job_functions_to_import for job in employee_details['jobs_remove']]),
                                    any([org in adm_org_units_with_employees.keys() for org in employee_details['aa_orgs_remove']]),
                                    (employee_details['position'] in position_types_to_import and employee_details['org'] in adm_org_units_with_employees.keys())]):
                                employees_to_change[employee_details['user']].extend(orgs)
                        else:
                            pass  # do nothing with inactive users - Nexus/FK Org should handle this
                            # Filter if connected to Nexus
                            # if any([any([job in job_functions_to_import for job in employee_details['jobs_remove']]),
                            #         any([org in adm_org_units_with_employees.keys() for org in employee_details['aa_orgs_remove']]),
                            #         (employee_details['position'] in position_types_to_import and employee_details['org'] in adm_org_units_with_employees.keys())]):
                            #     employees_to_change[employee_details['user']].extend([])
                else:
                    logger.warning(f'Error getting employee details for {employee_uuid} - continuing')

            return employees_to_change

        except Exception as e:
            logger.error(f'Error getting employee changes: {e}')
            return
