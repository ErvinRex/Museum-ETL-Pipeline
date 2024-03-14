"""Test functionality of extract python file"""

from unittest.mock import patch, MagicMock

from extract import (download_specific_files,
                     get_bucket_names,
                     get_bucket_objects)


def test_get_bucket_names():
    """Check that the function returns a list of bucket names"""

    mock_client = MagicMock()

    mock_client.list_objects.return_value = {"Buckets: []"}
    names = get_bucket_names(mock_client)
    print(names)
    assert isinstance(names, list)


def test_get_bucket_objects():
    """Check that the function returns a list of bucket objects"""

    mock_client = MagicMock()
    mock_bucket = MagicMock()
    mock_objects = MagicMock()

    mock_client.list_objects.return_value = {"Contents": []}
    mock_objects.return_value = []
    objects = get_bucket_objects(mock_client, mock_bucket)

    assert isinstance(objects, list)


@patch("extract.get_bucket_objects")
def test_download_specific_files_creates_files(mock_get_bucket_objects):
    """download_museum_files() should download the relevant csv files in the current directory."""
    mock_get_bucket_objects.return_value = ["lmnh"]
    mock_bucket = MagicMock()
    mock_client = MagicMock()
    mock_download_file = mock_client.download_file

    download_specific_files(mock_client, mock_bucket, 'lmnh', 'museum_files')

    mock_download_file.call_count == 1
