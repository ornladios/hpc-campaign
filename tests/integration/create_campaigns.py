#!/usr/bin/env python3

import argparse
import os
import shutil
import tarfile
from contextlib import contextmanager
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Iterator

from hpc_campaign.manager import Manager
from hpc_campaign.taridx import create_tar_index

DATASET_PATHS = (
    "testdata/heat_10_15.bp",
    "testdata/heat_12_17.bp",
    "testdata/heat_10_15.h5",
    "testdata/heat_12_17.h5",
    "testdata/T_10_15_00000.png",
    "testdata/T_10_15_00001.png",
    "testdata/T_10_15_00002.png",
    "testdata/T_12_17_00000.png",
    "testdata/T_12_17_00001.png",
    "testdata/T_12_17_00002.png",
    "testdata/readme",
)
IMAGE_PATHS = tuple(path for path in DATASET_PATHS if path.endswith(".png"))
DATA_PATHS = tuple(path for path in DATASET_PATHS if path.endswith((".bp", ".h5")))
TEXT_PATHS = tuple(path for path in DATASET_PATHS if path.endswith("readme"))

CAMPAIGN_NAMES = tuple(
    f"testdata-{protocol}-{layout}.aca"
    for protocol in ("local", "ssh", "https", "s3", "xrootd")
    for layout in ("files", "tar")
)

S3_ACCESS_KEY = "HPC_CAMPAIGN_TEST"
S3_SECRET_KEY = "hpc-campaign-test-secret"
S3_BUCKET = "campaign-data"
SSH_LONG_HOSTNAME = "ssh-service.docker.hpc-campaign"
HTTPS_LONG_HOSTNAME = "https-service.docker.hpc-campaign"
S3_LONG_HOSTNAME = "s3-service.docker.hpc-campaign"
S3_ENDPOINT = f"http://{S3_LONG_HOSTNAME}:9000"
XROOTD_LONG_HOSTNAME = "xrootd-service.docker.hpc-campaign"


@contextmanager
def working_directory(path: Path) -> Iterator[None]:
    previous = Path.cwd()
    os.chdir(path)
    try:
        yield
    finally:
        os.chdir(previous)


def prepare_data(source: Path, data_root: Path) -> tuple[Path, Path]:
    source = source.resolve()
    data_root.mkdir(parents=True, exist_ok=True)
    destination = data_root / "testdata"
    if source != destination.resolve():
        if destination.exists():
            shutil.rmtree(destination)
        shutil.copytree(source, destination)

    archive_dir = data_root / "archive"
    archive_dir.mkdir(parents=True, exist_ok=True)
    tar_path = archive_dir / "campaign-data.tar"
    with tarfile.open(tar_path, "w") as archive:
        archive.add(destination, arcname="testdata")
    index_path = archive_dir / "campaign-data.tar.idx"
    create_tar_index(str(tar_path), str(index_path))
    return tar_path, index_path


def add_datasets(manager: Manager) -> list[int]:
    for path in DATA_PATHS:
        manager.data(path)
    for path in IMAGE_PATHS:
        manager.image(path)
    for path in TEXT_PATHS:
        manager.text(path)
    return [int(row[0]) for row in manager.cur.execute("select rowid from replica where deltime = 0").fetchall()]


def create_file_campaign(
    campaign_store: Path,
    name: str,
    hostname: str,
    longhostname: str | None = None,
) -> None:
    # Always ingest locally so BP/HDF5 metadata is embedded. A hostname that is
    # already present in hosts.yaml would otherwise activate metadata-free
    # remote ingestion. Rename the recorded host after ingestion instead.
    manager = Manager(archive=name, hostname="docker", campaign_store=str(campaign_store))
    manager.open(create=True, truncate=True)
    add_datasets(manager)
    if hostname != "docker":
        recorded_longhostname = longhostname if longhostname is not None else hostname
        manager.cur.execute(
            "update host set hostname = ?, longhostname = ? where hostname = 'docker'",
            (hostname, recorded_longhostname),
        )
        manager.con.commit()
    manager.close()


def create_archived_file_campaign(
    campaign_store: Path,
    name: str,
    system: str,
    host: str,
    directory: str,
    longhostname: str,
) -> None:
    manager = Manager(archive=name, hostname="docker", campaign_store=str(campaign_store))
    manager.open(create=True, truncate=True)
    add_datasets(manager)
    _host_id, directory_id, archive_id = manager.add_archival_storage(
        system=system,
        host=host,
        directory=directory,
        longhostname=longhostname,
    )
    for dataset_name in DATASET_PATHS:
        manager.archived_replica(dataset_name, directory_id, archive_id, move=True)
    manager.close()


def create_tar_campaign(
    campaign_store: Path,
    name: str,
    system: str,
    host: str,
    directory: str,
    longhostname: str,
    tar_path: Path,
    index_path: Path,
) -> None:
    manager = Manager(archive=name, hostname="docker", campaign_store=str(campaign_store))
    manager.open(create=True, truncate=True)
    original_replica_ids = add_datasets(manager)
    manager.add_archival_storage(
        system=system,
        host=host,
        directory=directory,
        tarfilename=tar_path.name,
        tarfileidx=str(index_path),
        longhostname=longhostname,
    )
    for replica_id in original_replica_ids:
        manager.delete_replica(replica_id)
    manager.close()


def _create_campaigns(source: Path, data_root: Path, campaign_store: Path) -> None:
    tar_path, index_path = prepare_data(source, data_root)
    campaign_store.mkdir(parents=True, exist_ok=True)

    with working_directory(data_root):
        create_file_campaign(campaign_store, "testdata-local-files.aca", "docker")
        create_tar_campaign(
            campaign_store,
            "testdata-local-tar.aca",
            "fs",
            "docker",
            str(tar_path.parent),
            "",
            tar_path,
            index_path,
        )
        create_file_campaign(campaign_store, "testdata-ssh-files.aca", "docker-ssh", SSH_LONG_HOSTNAME)
        create_tar_campaign(
            campaign_store,
            "testdata-ssh-tar.aca",
            "fs",
            "docker-ssh",
            str(tar_path.parent),
            SSH_LONG_HOSTNAME,
            tar_path,
            index_path,
        )
        create_archived_file_campaign(
            campaign_store,
            "testdata-https-files.aca",
            "https",
            "docker-https",
            "data",
            HTTPS_LONG_HOSTNAME,
        )
        create_tar_campaign(
            campaign_store,
            "testdata-https-tar.aca",
            "https",
            "docker-https",
            "data/archive",
            HTTPS_LONG_HOSTNAME,
            tar_path,
            index_path,
        )
        create_archived_file_campaign(
            campaign_store,
            "testdata-s3-files.aca",
            "S3",
            "docker-rustfs",
            S3_BUCKET,
            S3_LONG_HOSTNAME,
        )
        create_tar_campaign(
            campaign_store,
            "testdata-s3-tar.aca",
            "S3",
            "docker-rustfs",
            S3_BUCKET,
            S3_LONG_HOSTNAME,
            tar_path,
            index_path,
        )
        create_file_campaign(
            campaign_store,
            "testdata-xrootd-files.aca",
            "docker-xrootd",
            XROOTD_LONG_HOSTNAME,
        )
        create_tar_campaign(
            campaign_store,
            "testdata-xrootd-tar.aca",
            "fs",
            "docker-xrootd",
            str(tar_path.parent),
            XROOTD_LONG_HOSTNAME,
            tar_path,
            index_path,
        )


def create_campaigns(source: Path, data_root: Path, campaign_store: Path) -> list[Path]:
    campaign_store.mkdir(parents=True, exist_ok=True)
    with TemporaryDirectory(prefix=".staging-", dir=campaign_store) as staging_directory:
        staging_store = Path(staging_directory)
        _create_campaigns(source, data_root, staging_store)
        for name in CAMPAIGN_NAMES:
            (staging_store / name).replace(campaign_store / name)

    return [campaign_store / name for name in CAMPAIGN_NAMES]


def upload_s3(data_root: Path, endpoint: str) -> None:
    import boto3
    from botocore.config import Config
    from botocore.exceptions import ClientError

    client = boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=S3_ACCESS_KEY,
        aws_secret_access_key=S3_SECRET_KEY,
        region_name="us-east-1",
        config=Config(s3={"addressing_style": "path"}),
    )
    try:
        client.head_bucket(Bucket=S3_BUCKET)
    except ClientError:
        client.create_bucket(Bucket=S3_BUCKET)

    for relative_path in DATASET_PATHS:
        source = data_root / relative_path
        if source.is_dir():
            for file_path in sorted(source.rglob("*")):
                if file_path.is_file():
                    key = file_path.relative_to(data_root).as_posix()
                    client.upload_file(str(file_path), S3_BUCKET, key)
        else:
            client.upload_file(str(source), S3_BUCKET, relative_path)

    client.upload_file(str(data_root / "archive/campaign-data.tar"), S3_BUCKET, "campaign-data.tar")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Create the Docker remote-access campaign matrix",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--source",
        type=Path,
        default=Path("testdata"),
        help="source directory containing the test datasets",
    )
    parser.add_argument(
        "--data-root",
        type=Path,
        default=Path("/srv/data"),
        help="destination where test data and its tar archive are prepared",
    )
    parser.add_argument(
        "--campaign-store",
        type=Path,
        default=Path("/campaigns"),
        help="output directory for the generated .aca campaign files",
    )
    parser.add_argument(
        "--upload-s3",
        action="store_true",
        help="upload the prepared test data and tar archive to S3",
    )
    parser.add_argument(
        "--s3-endpoint",
        default=S3_ENDPOINT,
        help="S3-compatible service URL used with --upload-s3",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    campaigns = create_campaigns(args.source, args.data_root, args.campaign_store)
    if args.upload_s3:
        upload_s3(args.data_root, args.s3_endpoint)
    print("Created campaigns:")
    for campaign in campaigns:
        print(f"  {campaign}")


if __name__ == "__main__":
    main()
