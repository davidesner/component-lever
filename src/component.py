"""
Template Component main class.

"""
import logging
from dataclasses import asdict, dataclass
from datetime import datetime

from keboola.component.base import ComponentBase
from keboola.component.dao import TableDefinition
from keboola.component.exceptions import UserException
from keboola.csvwriter import ElasticDictWriter
from keboola.utils.date import parse_datetime_interval

from client import LeverClient
from configuration import Configuration
from json_parser import FlattenJsonParser


@dataclass
class WriterCacheRecord:
    writer: ElasticDictWriter
    table_definition: TableDefinition


class Component(ComponentBase):
    """
        Extends base class for general Python components. Initializes the CommonInterface
        and performs configuration validation.

        For easier debugging the data folder is picked up by default from `../data` path,
        relative to working directory.

        If `debug` parameter is present in the `config.json`, the default logger is set to verbose DEBUG mode.
    """

    def __init__(self):
        super().__init__()
        self.endpoint_mapping = {"opportunities": self.get_opportunities,
                                 "postings": self.get_postings,
                                 "requisitions": self.get_requisitions
                                 }
        self.parser = FlattenJsonParser()
        self._writer_cache: dict[str, WriterCacheRecord] = dict()
        self._configuration: Configuration
        self._client: LeverClient

    def run(self):
        self._init_configuration()
        self._init_client()

        for endpoint in self._configuration.endpoints:
            self.endpoint_mapping[endpoint]()

        for table, cache_record in self._writer_cache.items():
            cache_record.writer.writeheader()
            cache_record.writer.close()
            self.write_manifest(cache_record.table_definition)

    def get_opportunities(self):
        start_date, end_date = self._get_date_range()
        params = {}

        if start_date:
            params['updated_at_start'] = int(start_date.timestamp() * 1000)
        if end_date:
            params['updated_at_end'] = int(end_date.timestamp() * 1000)

        params.update(asdict(self._configuration.sync_options.additional_filters))

        # Fetch and process data for each selected endpoint
        incremental_load = self._configuration.destination.load_type.is_incremental()
        logging.info("Fetching data from opportunities")

        table_def = self.create_out_table_definition('opportunities.csv', primary_key=['id'],
                                                     incremental=incremental_load)

        for page_data in self._client.fetch_data_paginated('opportunities', params):
            for item in page_data:
                self.get_resumes(item['id'])
                self.get_applications(item['id'])

                self.write_to_csv(self.parser.parse_row(item), 'opportunities', table_def)
        logging.info("Opportunities extraction completed successfully.")

    def get_resumes(self, opportunity_id: str):
        # Fetch and process data for each selected endpoint
        incremental_load = self._configuration.destination.load_type.is_incremental()

        table_def = self.create_out_table_definition('resumes.csv', primary_key=['id'],
                                                     incremental=incremental_load)

        for item in self._client.fetch_data(f'opportunities/{opportunity_id}/resumes', {}):
            item['opportunity_id'] = opportunity_id
            self.write_to_csv(self.parser.parse_row(item), 'resumes', table_def)

    def get_applications(self, opportunity_id: str):
        # Fetch and process data for each selected endpoint
        incremental_load = self._configuration.destination.load_type.is_incremental()

        table_def = self.create_out_table_definition('applications.csv', primary_key=['id'],
                                                     incremental=incremental_load)

        for item in self._client.fetch_data(f'opportunities/{opportunity_id}/applications', {}):
            item['opportunity_id'] = opportunity_id
            self.write_to_csv(self.parser.parse_row(item), 'applications', table_def)

    def get_postings(self):
        start_date, end_date = self._get_date_range()

        params = {}

        if start_date:
            params['updated_at_start'] = int(start_date.timestamp() * 1000)
        if end_date:
            params['updated_at_end'] = int(end_date.timestamp() * 1000)

        params.update(asdict(self._configuration.sync_options.additional_filters))

        # Fetch and process data for each selected endpoint
        incremental_load = self._configuration.destination.load_type.is_incremental()
        logging.info("Fetching data from postings")

        table_def = self.create_out_table_definition('postings.csv', primary_key=['id'],
                                                     incremental=incremental_load)

        for page_data in self._client.fetch_data_paginated('postings', params):
            for item in page_data:
                self.write_to_csv(self.parser.parse_row(item), 'postings', table_def)
        logging.info("Postings extraction completed successfully.")

    def get_requisitions(self):
        start_date, end_date = self._get_date_range()

        params = {}

        if start_date:
            params['created_at_start'] = int(start_date.timestamp() * 1000)
        if end_date:
            params['created_at_end'] = int(end_date.timestamp() * 1000)

        params.update(asdict(self._configuration.sync_options.additional_filters))

        # Fetch and process data for each selected endpoint
        incremental_load = self._configuration.destination.load_type.is_incremental()
        logging.info("Fetching data from requisitions")

        table_def = self.create_out_table_definition('requisitions.csv', primary_key=['id'],
                                                     incremental=incremental_load)

        for page_data in self._client.fetch_data_paginated('requisitions', params):
            for item in page_data:
                self.write_to_csv(self.parser.parse_row(item), 'requisitions', table_def)
        logging.info("Requisitions extraction completed successfully.")

    def _get_date_range(self) -> tuple[datetime, datetime]:
        start_date, end_date = None, None
        if self._configuration.sync_options.start_date and self._configuration.sync_options.end_date:
            start_date, end_date = parse_datetime_interval(self._configuration.sync_options.start_date,
                                                           self._configuration.sync_options.end_date)
        return start_date, end_date

    def write_to_csv(self, parsed_data: dict,
                     table_name: str,
                     table_def: TableDefinition,
                     ) -> None:
        if not self._writer_cache.get(table_name):
            writer = ElasticDictWriter(table_def.full_path, [])
            writer.writeheader()

            self._writer_cache[table_name] = WriterCacheRecord(writer, table_def)

        writer = self._writer_cache[table_name].writer
        writer.writerow(parsed_data)

    def _init_client(self):
        self._client = LeverClient(self._configuration.authentication.pswd_token)

    def _init_configuration(self):
        self.validate_configuration_parameters(Configuration.get_dataclass_required_parameters())
        self._configuration: Configuration = Configuration.load_from_dict(self.configuration.parameters)


"""
        Main entrypoint
"""
if __name__ == "__main__":
    try:
        comp = Component()
        # this triggers the run method by default and is controlled by the configuration.action parameter
        comp.execute_action()
    except UserException as exc:
        logging.exception(exc)
        exit(1)
    except Exception as exc:
        logging.exception(exc)
        exit(2)
