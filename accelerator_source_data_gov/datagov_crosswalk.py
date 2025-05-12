import os
import json
import logging

from accelerator_core.workflow.accel_source_ingest import (
    IngestSourceDescriptor,
    IngestPayload,
)

from accelerator_core.workflow.crosswalk import Crosswalk

from accelerator_core.schema.models.accel_model import (
    AccelProgramModel,
    AccelProjectModel,
    AccelIntermediateResourceModel,
    AccelResourceReferenceModel,
    AccelResourceUseAgreementModel,
    AccelPublicationModel,
    AccelDataResourceModel,
    AccelDataLocationModel,
    AccelGeospatialDataModel,
    AccelTemporalDataModel,
    AccelPopulationDataModel,
)
from accelerator_core.schema.models.base_model import (
    SubmissionInfoModel,
    TechnicalMetadataModel,
)

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s: %(filename)s:%(funcName)s:%(lineno)d: %(message)s"
)

logger = logging.getLogger(__name__)


class DataGovCrosswalk(Crosswalk):
    """
    Crosswalks data from the ingest result into the appropriate format for downstream processing.
    """

    def transform(self, ingest_result: IngestPayload) -> IngestPayload:
        # For demonstration, let's assume the transformation just returns the payload directly
        logger.info("DataGovCrosswalk::transform()")
        logger.info("Transforming ingest result: %s", ingest_result)

        payload = ingest_result.payload
        extras = payload.get('extras', [])

        # Submission Info
        submission = SubmissionInfoModel()
        submission.submitter_name = payload.get('author', None)
        submission.submitter_email = payload.get('author_email', None)
        submission.submitter_comment = payload.get('organization', None).get('approval_status', None)

        # Program
        program = AccelProgramModel()
        program.code = 'CHORDS'
        program.name = 'CHORDS'
        program.preferred_label = 'CHORDS'

        # Project
        project = AccelProjectModel()
        project.code = payload.get('id', None)
        project.name = payload.get('title', None)
        project.sponsor = payload.get('organization', None).get('title', None)

        for item in extras:
            if item.get('key') == 'identifier':
                project.identifier = item.get('value', None)
                break








        pass