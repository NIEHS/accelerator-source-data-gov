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
from accelerator_core.utils.schema_tools import SchemaTools
from accelerator_core.schema.models.accel_model import build_accel_from_model

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
        program.name = payload.get('organization', None).get('name', None)
        program.preferred_label = payload.get('organization', None).get('title', None)

        # Project
        project = AccelProjectModel()
        if 'display_name' in payload.get('groups', []) and payload.get('groups', []) is not None:
            project.project_name = payload.get('groups', []).get('display_name', None)
        if 'id' in payload.get('groups', []) and payload.get('groups', []) is not None:
            project.project_code = payload.get('groups', []).get('id', None)
        if 'name' in payload.get('groups', []) and payload.get('groups', []) is not None:
            project.project_short_name = payload.get('groups', []).get('name', None)
        if 'title' in payload.get('groups', []) and payload.get('groups', []) is not None:
            project.name = payload.get('groups', []).get('title', None)
        if 'type' in payload.get('organization', []) and payload.get('organization', []) is not None:
            project.project_sponsor = payload.get('organization', []).get('type', None)

        # resource
        resource = AccelIntermediateResourceModel()
        resource.name = payload.get('title', None)
        resource.description = payload.get('notes', None)
        resource.resource_type = payload.get('type', None)
        resource.resource_url = payload.get('url', None)
        resource.version = payload.get('version', None)

        for item in extras:
            if item.get('key') == 'display_name':
                resource.keywords = item.get('value', None)


        rendered = build_accel_from_model(
            version="1.0.0",
            submission=submission,
            technical=None,
            program=program,
            project=project,
            resource=resource,
            data_resource=None,
            temporal=None,
            geospatial=None,
            population=None,
        )
        schema_tools = SchemaTools(self.config)
        result = schema_tools.validate_json_against_schema(
            rendered, "accelerator", "1.0.0"
        )

        return result
