import sqlite3
from pathlib import Path

from tests.integration.create_campaigns import CAMPAIGN_NAMES, DATASET_PATHS, create_campaigns


def test_create_remote_access_campaign_matrix(tmp_path: Path):
    source = Path(__file__).resolve().parents[1] / "testdata"
    data_root = tmp_path / "data"
    campaign_store = tmp_path / "campaigns"

    campaigns = create_campaigns(source, data_root, campaign_store)

    assert [campaign.name for campaign in campaigns] == list(CAMPAIGN_NAMES)
    assert (data_root / "archive/campaign-data.tar").is_file()
    assert (data_root / "archive/campaign-data.tar.idx").is_file()

    for campaign in campaigns:
        with sqlite3.connect(campaign) as connection:
            datasets = connection.execute("select name, fileformat from dataset where deltime = 0").fetchall()
            replicas = connection.execute("select count(*) from replica where deltime = 0").fetchone()
        assert {name for name, _file_format in datasets} == set(DATASET_PATHS)
        assert {file_format for _name, file_format in datasets} == {"ADIOS", "HDF5", "IMAGE", "TEXT"}
        assert replicas is not None and replicas[0] == len(DATASET_PATHS)
