from accelerator_core.utils.logger import setup_logger
from accelerator_core.utils.xcom_utils import XcomPropsResolver
from accelerator_core.workflow.accel_source_ingest import (
    IngestSourceDescriptor,
    IngestPayload,
)

from accelerator_core.workflow.crosswalk import Crosswalk

from accelerator_core.schema.models.accel_model import (
    AccelProgramModel,
    AccelProjectModel,
    AccelIntermediateResourceModel,
    build_accel_from_model
)
from accelerator_core.schema.models.base_model import (
    SubmissionInfoModel,
    TechnicalMetadataModel,
)

logger = setup_logger("accelerator-source-cedar")


class DataGovCrosswalk(Crosswalk):
    """
    Crosswalks data from the ingest result into the appropriate format for downstream processing.
    """

    """Abstract superclass for mapping raw data to a structured JSON format."""

    def __init__(self, xcom_props_resolver: XcomPropsResolver):
        """
        @param: xcom_properties_resolver XcomPropertiesResolver that can access
        handling configuration
        """

        super().__init__(xcom_props_resolver)

    def transform(self, ingest_result: IngestPayload) -> IngestPayload:
        logger.info("DataGovCrosswalk::transform()")
        output_payload = IngestPayload(ingest_result.ingest_source_descriptor)

        payload_len = self.get_payload_length(ingest_result)
        logger.info(f"payload len: {payload_len}")
        for i in range(payload_len):
            payload = self.payload_resolve(ingest_result, i)
            logger.info(f"payload is resolved: {payload}")
            transformed = self.translate_to_accel_model(ingest_result, payload)
            self.report_individual(output_payload,"itemid", transformed)

        return output_payload

    @staticmethod
    def translate_to_accel_model(ingest_result: IngestPayload, payload: dict) -> dict:
        logger.info("DataGovCrosswalk::translate_to_accel_model()")
        logger.info("Transforming ingest result: %s", ingest_result)

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
        program.preferred_label = payload.get('organization', None).get('title', None)

        # Project
        project = AccelProjectModel()
        if 'name' in payload.get('groups', []) and payload.get('groups', []) is not None:
            project.project_short_name = payload.get('name', None)
        if 'display_name' in payload.get('groups', []) and payload.get('groups', []) is not None:
            project.project_name = payload.get('title', None)
        if 'id' in payload.get('groups', []) and payload.get('groups', []) is not None:
            project.project_code = payload.get('id', None)

        '''
        if 'title' in payload.get('groups', []) and payload.get('groups', []) is not None:
            project.name = payload.get('groups', []).get('title', None)
        if 'type' in payload.get('organization', []) and payload.get('organization', []) is not None:
            project.project_sponsor = payload.get('organization', []).get('type', None)
        '''

        # resource
        resource = AccelIntermediateResourceModel()
        resource.resource_use_agreement = payload.get('license_title', None)
        #resource.description = payload.get('notes', None)
        resource.name = payload.get('resources', None)[0].get('name', None)

        '''

        resource.resource_type = payload.get('type', None)
        resource.resource_url = payload.get('url', None)
        resource.version = payload.get('version', None)
        '''

        for item in extras:
            if item.get('key') == 'display_name':
                resource.keywords = item.get('value', None)

        technical = TechnicalMetadataModel()
        technical.created = ingest_result.ingest_source_descriptor.submit_date

        rendered = build_accel_from_model(
            version="1.0.0",
            submission=submission,
            data_resource=None,
            temporal=None,
            population=None,
            geospatial=None,
            program=program,
            project=project,
            resource=resource,
            technical=technical
        )
        logger.info("Rendered model: %s", rendered)

        return rendered

