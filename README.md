# gutenburg_search
PySpark app that indexes books from Gutenburg Project

To run app, spin up app by running:
`docker compose up`

There is a running container called gutenburgsearch-pyspark-app-1 that we can attach to. That way we can run our app in the environment
`docker exec -it gutenburgsearch-pyspark-app-1 {or container id} bash`

We are now in the container environment. Before querying, we need to first perform ingestion on the books.
`python ingest.py`

Now, we can run our queries. Try the following:
`python search_documents.py --book_id 12375 --limit 5`

`python search_words.py --word fish --limit 5`

