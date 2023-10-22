from pyspark.sql import SparkSession
import argparse
from gutenburgsearch.models.ingestion.factory import IngestFactory, IngestType


def ingest(args):
    
    spark = SparkSession.builder \
            .appName(args.app_name) \
            .master("local[*]") \
            .getOrCreate()

    ingestion = IngestFactory.create_ingest(ingest_type=IngestType(args.ingest), 
                                            spark_session=spark, 
                                            load_path=args.load_path)

    extract_df = ingestion.extract()

    transform_df = ingestion.transform(extract_df)

    ingestion.load(transform_df)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Run Gutenburg Search')

    parser.add_argument('--app_name', type=str, default='Gutenburg Search App', help='Name of application')                    
    parser.add_argument('--ingest', type=str, default='gutenburg_api', help='Name of ingest type to run (default: gutenburg_api)')
    parser.add_argument('--load_path', type=str, default='/app/data', help='Name of ingest type to run (default: gutenburg_api)')

    # Parse the command-line arguments
    args = parser.parse_args()

    ingest(args)