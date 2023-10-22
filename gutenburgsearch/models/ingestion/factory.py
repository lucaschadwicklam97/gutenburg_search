from enum import Enum

class IngestType(Enum):
    """
    Enum of ingestion types
    """
    gutenburg_api = 'gutenburg_api'
    # other concrete implementations of Ingestion would be added here


class IngestFactory:
    @staticmethod
    def create_ingest(ingest_type, spark_session, **kwargs):
        if ingest_type == IngestType.gutenburg_api:
            from gutenburgsearch.models.ingestion.gutenburg_api import GutenburgAPIIngestion
            return GutenburgAPIIngestion(spark_session, **kwargs)
        else:
            raise ValueError(f'Unknown ingest type {ingest_type}')