from gutenburgsearch.models.ingestion.gutenburg_api import GutenburgAPIIngestion

def test_extract():
    text = GutenburgAPIIngestion._extract_text_from_url("http://aleph.gutenberg.org/1/2/3/7/12373/12373.zip")
    assert len(text) > 10000
    assert True

def test_id_url():
    spark_context = {}
    ingestion = GutenburgAPIIngestion(spark_context)
    ids = ingestion._get_book_id_url()
    assert len(ids) == 58 # at least for the first page... will need to test functionality on getting exactly the number of books we want
