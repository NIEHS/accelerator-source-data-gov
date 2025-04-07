import os
import json
import shutil
import logging
import unittest

from accelerator_core.utils.xcom_utils import DirectXcomPropsResolver, XcomUtils
from accelerator_source_data_gov.datagov_crosswalk import DataGovCrosswalk
from accelerator_core.workflow.accel_source_ingest import (
    IngestSourceDescriptor,
    IngestPayload,
)

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s: %(filename)s:%(funcName)s:%(lineno)d: %(message)s"

)
logger = logging.getLogger(__name__)


class TestDataGovCrosswalk(unittest.TestCase):
    def test_datagov_crosswalk(self):
        temp_dirs_path = 'test_resources/temp_dirs'
        runid = "test_crosswalk_key_dataset"
        item_id = "test_crosswalk_key_dataset_item"
        path = os.path.join(temp_dirs_path, runid)

        if os.path.exists(path):
            shutil.rmtree(path)

        try:
            xcom_props_resolver = DirectXcomPropsResolver(temp_files_supported=True, temp_files_location=temp_dirs_path)
            xcom_utils = XcomUtils(xcom_props_resolver)

            # Create an IngestSourceDescriptor instance and populate metadata
            ingest_source_descriptor = IngestSourceDescriptor()
            ingest_source_descriptor.ingest_identifier = "test"
            ingest_source_descriptor.type = type
            ingest_source_descriptor.submitter_name = "submitter_name"
            ingest_source_descriptor.submitter_email = "submitter_email"
            ingest_source_descriptor.submit_date = '2021-01-01'
            ingest_payload = IngestPayload(ingest_source_descriptor)

            ingest_payload.payload_inline = False

            file_path = 'test_resources/datagov_dump_04_02_2025/% Viability and zeta potential values of metal nanoparticles used in in vitro dermal irritation assays.json'

            with open(file_path, "r", encoding="utf-8") as f:
                contents_json = json.loads(f.read())
                stored_path = xcom_utils.store_dict_in_temp_file(item_id, contents_json, runid)
                ingest_payload.payload_path.append(stored_path)

            '''
            # Create an IngestPayload object
            ingest_payload = IngestPayload(ingest_source_descriptor)
            ingest_payload.ingest_source_descriptor = ingest_source_descriptor
            ingest_payload.source_document_detail = 'data.gov'
            ingest_payload.ingest_successful = False
            ingest_payload.payload_inline = False
            ingest_payload.payload = data
            '''

            # Transform the data using a crosswalk
            crosswalk = DataGovCrosswalk(xcom_props_resolver)
            actual = crosswalk.transform(ingest_result=ingest_payload)
            self.assertIsNotNone(actual)

        except json.JSONDecodeError:
            print(f"Skipping invalid JSON: {file_path}")


if __name__ == '__main__':
    unittest.main()