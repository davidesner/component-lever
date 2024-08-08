from typing import Dict, Any, Generator

from keboola.http_client import HttpClient


class LeverClient(HttpClient):

    def __init__(self, token: str):
        super().__init__(auth=(token, ''), base_url='https://api.lever.co/v1/', max_retries=4,
                         status_forcelist=(429, 500, 502, 503, 504))

    def fetch_data_paginated(self, endpoint: str,
                             params: Dict[str, Any]) -> Generator[list[dict[str, Any]], None, None]:
        """
        Fetch data from the specified Lever API endpoint and yield each page of results.
        """
        offset = None

        while True:
            params['limit'] = 50
            if offset:
                params['offset'] = offset
            response = self.get_raw(endpoint, params=params)
            response.raise_for_status()
            data = response.json()
            yield data['data']

            # Check for pagination
            if 'next' in data:
                offset = data['next']
            else:
                break

    def fetch_data(self, endpoint: str, params: Dict[str, Any]) -> list[dict[str, Any]]:
        """
        Fetch data from the specified Lever API endpoint that does not have pagination
        """
        response = self.get_raw(endpoint, params=params)
        response.raise_for_status()
        return response.json()['data']
