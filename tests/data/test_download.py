from avian_biosurveillance.data.download import download_data

def test_download_data() -> None:
    """Test the download_data function."""
    assert download_data("http://example.com/data.csv", "./data.csv") is True
    assert download_data("", "./data.csv") is False
    assert download_data("http://example.com/data.csv", "") is False
