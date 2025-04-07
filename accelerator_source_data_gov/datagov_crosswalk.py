from accelerator_core.workflow.accel_source_ingest import IngestResult
from accelerator_core.workflow.crosswalk import Crosswalk

class DataGovCrosswalk(Crosswalk):
    """
    Crosswalks data from the ingest result into the appropriate format for downstream processing.
    """

    def transform(self, ingest_result: IngestResult) -> IngestResult:
        # For demonstration, let's assume the transformation just returns the payload directly
        return ingest_result