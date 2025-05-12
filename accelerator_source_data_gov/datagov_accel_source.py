import requests
import logging
import uuid
import json
import os

from accelerator_core.workflow.accel_source_ingest import AccelIngestComponent
from accelerator_core.workflow.accel_source_ingest import IngestSourceDescriptor
from accelerator_core.workflow.accel_source_ingest import (
    IngestSourceDescriptor,
    IngestPayload,
)

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s: %(filename)s:%(funcName)s:%(lineno)d: %(message)s"

)
logger = logging.getLogger(__name__)


class DataGovAccelSource(AccelIngestComponent):
    """
    A subclass of AccelIngestComponent that implements the ingest process for Data.gov
    """
    def __init__(self, ingest_source_descriptor: IngestSourceDescriptor):
        super().__init__(ingest_source_descriptor)

    def ingest(self, additional_parameters: dict) -> IngestPayload:
        """
        Ingest data from a data.gov and return the result in an IngestResult object.

        :param additional_parameters: Dictionary containing parameters such as the api url and token
        :return: IngestResult with the parsed data from the spreadsheet
        """
        logger.info("DataGovAccelSource::ingest()")
        api_url = additional_parameters.get('api_url')
        params = additional_parameters.get('params')

        if not api_url or not params:
            raise ValueError("API URL and parameters must be provided")

        # Call the basic dataset search method
        query_result = self.basic_dataset_search(api_url=api_url, params=params)

        # Get the result datasets
        count = query_result.get("count", 0)
        datasets = query_result.get("datasets", [])

        logger.info("Results found: %s", query_result.get("count", 0))
        if count < 1:
            return None
        else:
            #self.dump_data(datasets=datasets)

            # Create an IngestResult object
            IngestPayload = IngestPayload(self.ingest_source_descriptor)
            IngestPayload.payload = datasets
            IngestPayload.ingest_successful = True
            return IngestPayload

    @staticmethod
    def basic_dataset_search(api_url: str = None, params: dict = None, rows: int = 1000) -> dict:
        """
        Get JSON-formatted lists of data.gov site’s datasets
        """
        logger.info("DataGovAccelSource::basic_dataset_search()")
        if not api_url or not params:
            logger.info("API URL and parameters must be provided.")
            return None

        # add pagination parameters
        params["rows"] = min(rows, 1000)   # CKAN API max limit is 1000
        params["start"] = 0  # Start at the beginning
        all_datasets = []

        try:
            while True:
                response = requests.get(api_url, params=params)
                response.raise_for_status()
                data = response.json()

                if not data.get("success"):
                    logger.info("Error:", data.get("error", "Unknown error"))
                    return None

                datasets = data["result"]["results"]
                all_datasets.extend(datasets)

                # Stop if fewer datasets are returned than the batch size
                if len(datasets) < params["rows"]:
                    break  # No more data left to fetch

                # Increment start for the next batch
                params["start"] += params["rows"]

            return {"count": len(all_datasets), "datasets": all_datasets}

        except requests.exceptions.RequestException as e:
            logger.info("Request failed:", e)
            return None

    @staticmethod
    def dump_data(datasets: list):
        """
        Dump the data into JSON files in a specified folder.
        :param datasets: A list of datasets
        :return:
        """
        logger.info("DataGovAccelSource::dump_data()")
        # Folder containing JSON files
        folder_path = "../tests/test_resources/datagov_dump_04_02_2025"
        MAX_FILENAME_LENGTH = 200  # Set max length for the dataset title
        file_count = 0
        file_name_list = []

        # Check if the folder exists, if not, create it
        if not os.path.exists(folder_path):
            os.makedirs(folder_path)
            logger.info(f"Folder created: {folder_path}")
        else:
            logger.info(f"Folder already exists: {folder_path}")

        for item in datasets:
            dataset_title = item.get('title', None)
            if dataset_title:
                dataset_title = dataset_title.replace('/', '_').replace(':', '_').replace(',', '_').replace('.',
                                                                                                            '_').replace(
                    '"', '_').replace('(', '').replace(')', '')
                dataset_title = dataset_title[:MAX_FILENAME_LENGTH]  # Truncate if too long
                file_name = f"{folder_path}/{dataset_title}.json"
            else:
                file_name = f"{folder_path}/{uuid.uuid4()}.json"
            if file_name in file_name_list:
                file_name = f"{folder_path}/{dataset_title}_{file_count}.json"
            logger.info('File Name: %s', file_name)
            file_name_list.append(file_name)
            file_count += 1
            with open(file_name, "w") as f:
                json.dump(item, f, indent=4)
        logger.info("File count: %d", file_count)
