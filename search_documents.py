from pyspark.sql import SparkSession, Row
import argparse
# import sparksql row

from ingest import ingest

def query(args):
    """
    Query the data to find top words appeared in a given book
    """

    spark = SparkSession.builder \
            .appName(args.app_name) \
            .master("local[*]") \
            .getOrCreate()
    
    df = spark.read.parquet(args.load_path)
    df.createOrReplaceTempView("BookWordCount")


    distinct_books = spark.sql(f"SELECT distinct book_id from BookWordCount").collect()

    if Row(book_id=args.book_id) in distinct_books:
        spark.sql(f"SELECT word, count FROM BookWordCount WHERE book_id = {args.book_id} ORDER BY count DESC LIMIT {args.limit}").show()
    else:
        raise ValueError(f"Book ID {args.book_id} not indexed. Please choose from {[row.book_id for row in distinct_books]}")

    

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Run Gutenburg Search')

    parser.add_argument('--app_name', type=str, default='Gutenburg Search App', help='Name of application')                    
    parser.add_argument('--ingest', type=str, default='gutenburg_api', help='Name of ingest type to run (default: gutenburg_api)')
    parser.add_argument('--load_path', type=str, default='/app/data', help='Name of ingest type to run (default: gutenburg_api)')
    parser.add_argument('--limit', type=int, default=10, help='Number of records to return (default: 10)')
    parser.add_argument('--book_id', type=str, help='Book ID to search for')

    # Parse the command-line arguments
    args = parser.parse_args()

    # ingest
    # Can run here, but will run it on docker compose up so its ready to go, and we can just query here
    # ingest(args)

    query(args)



