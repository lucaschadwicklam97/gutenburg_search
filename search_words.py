from pyspark.sql import SparkSession
import argparse
from ingest import ingest

def query(args):

    spark = SparkSession.builder \
            .appName(args.app_name) \
            .master("local[*]") \
            .getOrCreate()
    
    df = spark.read.parquet(args.load_path)
    df.createOrReplaceTempView("BookWordCount")

    spark.sql(f"SELECT book_id, word, count FROM BookWordCount WHERE word = '{args.word.lower()}' ORDER BY count DESC LIMIT {args.limit}").show()
    

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Run Gutenburg Search')

    parser.add_argument('--app_name', type=str, default='Gutenburg Search App', help='Name of application')                    
    parser.add_argument('--ingest', type=str, default='gutenburg_api', help='Name of ingest type to run (default: gutenburg_api)')
    parser.add_argument('--load_path', type=str, default='/app/data', help='Name of ingest type to run (default: gutenburg_api)')
    parser.add_argument('--limit', type=int, default=10, help='Number of records to return (default: 10)')
    parser.add_argument('--word', type=str, help='Word to search for')

    # Parse the command-line arguments
    args = parser.parse_args()

    # ingest
    # ingest(args)

    query(args)

