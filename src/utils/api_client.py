import time
import base64
import logging

logger = logging.getLogger(__name__)


class APIClient:
    def __init__(self, base_url, api_key=None, realm=None, client_id=None, client_secret=None, username=None, password=None, cert_base64=None, use_bearer=None):
        self.base_url = base_url
        self.api_key = api_key
        self.realm = realm
        self.client_id = client_id
        self.client_secret = client_secret
        self.username = username
        self.password = password
        self.use_bearer = use_bearer

        self.access_token = None
        self.refresh_token = None
        self.token_expiry = None
        self.refresh_token_expiry = None
        self.cert_data = None

        if cert_base64:
            self.cert_data = base64.b64decode(cert_base64)

    def _authenticate(self):
        try:
            import requests
            if self.api_key:
                if self.use_bearer:
                    return {'Authorization': f'Bearer {self.api_key}'}
                else:
                    return {'Authorization': f'{self.api_key}'}
            elif self.client_id and self.client_secret:
                if not self.realm:
                    raise ValueError('Realm is required for client_id and client_secret authentication')

                if self.access_token:
                    if self.token_expiry:
                        if time.time() < self.token_expiry:
                            return {'Authorization': f'Bearer {self.api_key}'}
                        else:
                            if self.refresh_token:
                                if self.refresh_token_expiry:
                                    if time.time() < self.refresh_token_expiry:
                                        refresh_token = True

                tmp_url = f'{self.base_url}/auth/realms/{self.realm}/protocol/openid-connect/token'

                tmp_headers = {
                    'Content-Type': 'application/x-www-form-urlencoded'
                }

                tmp_json_data = {
                    'client_id': self.client_id,
                    'client_secret': self.client_secret
                }

                if refresh_token:
                    tmp_json_data['grant_type'] = 'refresh_token'
                    tmp_json_data['refresh_token'] = self.refresh_token
                if self.username and self.password:
                    tmp_json_data['grant_type'] = 'password'
                    tmp_json_data['username'] = self.username
                    tmp_json_data['password'] = self.password
                else:
                    tmp_json_data['grant_type'] = 'client_credentials'

                now = time.time()

                response = requests.post(tmp_url, headers=tmp_headers, data=tmp_url)
                response.raise_for_status()
                data = response.json()

                self.access_token = data['access_token']
                self.token_expires = now + data['expires_in']

                if 'refresh_token' in data:
                    self.refresh_token = data['refresh_token']
                    self.refresh_token_expiry = now + data['refresh_expires_in']

                return {'Authorization': f'Bearer {self.access_token}'}
            elif self.username and self.password:
                auth_str = f"{self.username}:{self.password}"
                b64_auth_str = base64.b64encode(auth_str.encode()).decode()
                return {'Authorization': f'Basic {b64_auth_str}'}
            else:
                return {}
        except Exception as e:
            logger.error(e)
            return {}

    def make_request(self, **kwargs):
        if 'path' in kwargs:
            if not isinstance(kwargs['path'], str) and not kwargs['path'] is None:
                raise ValueError('Path must be a string')

        if self.cert_data:
            import requests_pkcs12 as requests
            kwargs['pkcs12_data'] = self.cert_data
            kwargs['pkcs12_password'] = self.password
        else:
            import requests

        if 'path' in kwargs:
            url = self.base_url.rstrip('/') + '/' + kwargs.pop('path').lstrip('/')
        else:
            url = self.base_url

        if 'headers' in kwargs:
            if not isinstance(kwargs['headers'], dict):
                raise ValueError('Headers must be a dictionary')
            kwargs['headers'] = kwargs['headers'] | self._authenticate()
        else:
            kwargs['headers'] = self._authenticate()

        if not any(ele in kwargs for ele in ['method', 'json', 'data', 'files']):
            method = requests.get
        elif 'method' in kwargs:
            method = getattr(requests, kwargs['method'].lower())
        else:
            method = requests.post

        kwargs.pop('method', None)

        if 'json' in kwargs:
            kwargs['headers']['Content-Type'] = 'application/json'

        response = method(url, **kwargs)
        if response.status_code != 200:
            logger.info(response.content)
        response.raise_for_status()

        if 'application/json' in response.headers.get('Content-Type', ''):
            return response.json()
        else:
            if not response.content:
                return b' '
            return response.content
