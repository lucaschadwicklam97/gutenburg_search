from bs4 import BeautifulSoup
import zipfile
from io import BytesIO
import requests
from gutenburgsearch.models.ingestion.base import BaseIngestion
from pyspark.sql.types import StructType, StructField, StringType, ArrayType
from pyspark.sql.functions import udf, explode, col
import pandas as pd
import re


class GutenburgAPIIngestion(BaseIngestion):
    """
    Concrete implementation of BaseIngestion to ingest data from Gutenburg API
    """
    url = 'https://www.gutenberg.org/robot/harvest?filetypes[]=txt'
    records_to_ingest = 30
    
    def __init__(self, spark_session, load_path):
        self.spark = spark_session
        self.load_path = load_path # to replace with S3 or hdfs path

        import os
        if not os.path.exists(self.load_path):
            os.makedirs(self.load_path)

    def extract(self):
        """
        extracts data from Gutenburg API and returns a data frame with schema (book_id, book_url, content)
        """

        # create data frame with schema and empty content column
        df = self.spark.createDataFrame(
            [[book_id, book_url, None] for book_id, book_url in self._get_book_id_url()],
            schema=self.schema)

        # create udf to extract text from url
        extract_text = udf(self._extract_text_from_url, StringType())

        # apply udf on content column
        df = df.withColumn('content', extract_text(df.book_url))

        return df
    
    def transform(self, df):
        """
        transforms data frame by splitting words out, exploding words into its own column, and then doing a groupby and count
        to return a data frame with schema (book_id, word, count)
        """
        
        print("Here is  number of partitions:", df.rdd.getNumPartitions())

        # create udf to split text into words
        split_text_udf = udf(self._split, ArrayType(StringType()))
        df = df.withColumn("words", split_text_udf(col("content")))

        # explode words column
        df = df.select("book_id", explode("words").alias("word"))

        # group by word and count
        result = df.groupBy("book_id", "word").count().orderBy("book_id", "count", ascending=False)

        result.show()

        return result

    
    def load(self, df):
        """
        loads data frame to self.load_path. For now its just a local path, but in the future it will be S3 or hdfs
        """

        df.write.mode('overwrite').parquet(self.load_path)


    def _get_book_id_url(self) -> list[tuple[str, str]]:
        """
        will pick up list of tuples of (book_id, book_url) from Gutenburg API. Number of books picked up is defined by self.records_to_ingest
        If there are 
        """

        # TODO: need to implement a way to get next page of results to get more books (i.e if one page doesnt have enough books, and if we want to get more)
        try: 
            res = requests.get(self.url)
            if res.status_code == 200:
                soup = BeautifulSoup(res.text)
        except Exception as e:
            raise e

        # only get urls that end without "-8.zip" as these are duplicates with different encodings
        # in the future, we wont need to filter it out. we can process it and use the files corresponding encoding
        book_list = []
        book_count = 0

        for book in soup.find_all('a', href=True):
            if book_count >= self.records_to_ingest:
                break
            url = book['href']
            if self.__valid_url(url):
                book_count += 1
                book_id = url.split('/')[-1].split('.')[0]
                book_list.append((book_id, book['href']))

        return book_list

    @property
    def schema(self) -> StructType:
        return StructType([
            StructField('book_id', StringType(), True),
            StructField('book_url', StringType(), True),
            StructField('content', StringType(), True)
        ])
 
    def __valid_url(self, url) -> bool:
        """
        method to filter out urls that we want.
        """
        if not url.endswith('-8.zip') and url.startswith('http://aleph.gutenberg.org/'):
            return True
        return False
    
    @staticmethod
    def _extract_text_from_url(book_url) -> str:
        """
        UDF to extract text all text from a zip file.

        Parameters
        ----------
        bookurl : str  : URL to zip file. 
        """
        book_text = []
        try:
            res = requests.get(book_url)
        
            if res.status_code == 200:
                zip_buffer = BytesIO(res.content)
                with zipfile.ZipFile(zip_buffer) as z:
                    file_list = z.namelist() 
                    for file in file_list:
                        # print("Extracting text from file: ", file)
                        with z.open(file) as f:
                            # TODO: grab start of the book, not the whole text file
                            text = f.read().decode('utf-8', errors='ignore')
                            # print("Showing sample of text: ", text[:100])
                            book_text.append(text)
        except Exception as e:
            raise e
        
        return "\n".join(book_text)

    @staticmethod
    def _split(text: str) -> list:
        """
        method to split text into words. 

        Parameters
        ----------
        text : str  : content to split into words
        """
        
        return re.findall(r'\b\w+\b', text.lower())
