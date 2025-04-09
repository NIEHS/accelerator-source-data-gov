import logging

from accelerator_core.workflow.accel_source_ingest import IngestSourceDescriptor
from data_gov_accel_source import DataGovAccelSource
from datagov_crosswalk import DataGovCrosswalk

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s: %(filename)s:%(funcName)s:%(lineno)d: %(message)s"
)

logger = logging.getLogger(__name__)


def main(api_url: str, params: dict, type: str, submitter_name: str, submitter_email: str):
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

        # Transform the data using a crosswalk
        crosswalk = DataGovCrosswalk()
        for doc in ingest_results:
            ingest_result = crosswalk.transform
            pass


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
