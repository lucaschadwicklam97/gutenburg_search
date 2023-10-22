import argparse
from search_documents import ingest


def test_ingest():
    parser = argparse.ArgumentParser(description='Run Gutenburg Search')

    parser.add_argument('--app_name', type=str, default='Gutenburg Search App', help='Name of application')                    
    parser.add_argument('--ingest', type=str, default='gutenburg_api', help='Name of ingest type to run (default: gutenburg_api)')
    parser.add_argument('--load_path', type=str, default='/app/data', help='Name of ingest type to run (default: gutenburg_api)')

    # Parse the command-line arguments
    args = parser.parse_args()

    ingest(args)

    assert False