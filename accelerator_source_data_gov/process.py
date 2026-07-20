import logging

from accelerator_core.workflow.accel_source_ingest import IngestSourceDescriptor
from datagov_accel_source import DataGovAccelSource
from datagov_crosswalk import DataGovCrosswalk

from accelerator_core.workflow.accel_source_ingest import (
    IngestSourceDescriptor,
    IngestPayload,
)

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s: %(filename)s:%(funcName)s:%(lineno)d: %(message)s"
)

logger = logging.getLogger(__name__)


def main(api_url: str, params: dict, type: str, submitter_name: str, submitter_email: str):
    logger.info("process.py::main()")
    # Create an IngestSourceDescriptor instance and populate metadata
    ingest_source_descriptor = IngestSourceDescriptor()
    ingest_source_descriptor.type = type
    ingest_source_descriptor.submitter_name = submitter_name
    ingest_source_descriptor.submitter_email = submitter_email
    ingest_source_descriptor.submit_date = '2021-01-01'

    # Initialize the specific ingest component
    data_gov_accel_source = DataGovAccelSource(ingest_source_descriptor)

    # Ingest the data
    ingest_results = data_gov_accel_source.ingest({'api_url': api_url, 'params': params})

    # Perform data transformation and ingestion into a repository
    for entry in ingest_results.payload:
        logger.info("Processing entry: %s", entry)
        # Create an IngestPayload object
        ingest_payload = IngestPayload(ingest_source_descriptor)
        ingest_payload.ingest_source_descriptor = ingest_source_descriptor
        ingest_payload.source_document_detail = 'data.gov'
        ingest_payload.ingest_successful = False
        ingest_payload.payload_inline = False
        ingest_payload.payload = entry



        # Transform the data using a crosswalk
        crosswalk = DataGovCrosswalk()
        ingest_result = crosswalk.transform(ingest_payload)


if __name__ == '__main__':
    api_url = "https://catalog.data.gov/api/3/action/package_search"
    params = {
        "fq": "organization:epa-gov",
        "rows": 1000,  # Maximum per request (adjust if needed)
        "start": 0  # Start at 0 and increase in increments of `rows`
    }
    type = 'CHORDS'
    submitter_name = 'John Doe'
    submitter_email = 'john.doe@test.com'
    main(api_url=api_url, params=params, type=type, submitter_name=submitter_name, submitter_email=submitter_email)
