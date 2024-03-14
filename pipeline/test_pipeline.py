"""Test functionality of extract python file"""

from unittest.mock import patch, MagicMock

from pipeline import (load_kiosk_data)


@patch("pipeline.reader")
@patch("pipeline.open")
def test_load_kiosk_data(mock_open, mock_reader):
    """Test that kiosk data is loaded successfully"""
    mock_open.return_value = []
    mock_reader.return_value = []
    data = load_kiosk_data("museum_files")
    assert isinstance(data, list)


# Replace your_module with the name of your actual module
@patch("pipeline.logging.info")
@patch("builtins.open", new_callable=mock_open, read_data="header\nrow1\nrow2\nrow3")
def test_load_kiosk_data(mock_file, mock_logging):
    """Test that kiosk data is loaded successfully"""
    from pipeline import load_kiosk_data  # Import here to avoid imported things being patched before use

    # Assuming reader is a csv.reader, which yields each row
    mock_reader = MagicMock(return_value=iter(
        [["header"], ["row1"], ["row2"], ["row3"]]))

    with patch('csv.reader', mock_reader):
        data = load_kiosk_data("museum_files")

    assert data == [["row1"], ["row2"], ["row3"]]
    mock_logging.assert_called_with('Kiosk data successfully acquired.')
    mock_file.assert_called_with(
        'museum_files/lmnh_merged_hist_data.csv', 'r', encoding='utf-8')
