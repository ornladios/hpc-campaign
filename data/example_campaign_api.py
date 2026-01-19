import argparse
from pathlib import Path

from hpc_campaign.info import format_info
from hpc_campaign.manager import Manager

repo_root = Path(__file__).resolve().parents[1]
print(f"repo_root = {repo_root}")
data_dir = repo_root / "data"
print(f"data_dir = {data_dir}")
campaign_store = repo_root
print(f"campaign_store = {repo_root}")

api_archive = repo_root / "example_api.aca"
heat_dataset = data_dir / "heat.bp"
readme_file = data_dir / "readme"
image_files = [
    data_dir / "T00000.png",
    data_dir / "T00001.png",
    data_dir / "T00002.png",
]

info_outputs: dict[str, str] = {}


def build_info_args() -> argparse.Namespace:
    return argparse.Namespace(
        list_replicas=True,
        list_files=True,
        show_deleted=True,
        show_checksum=True,
    )


def main():
    manager = Manager(archive=str(api_archive), campaign_store=str(campaign_store))
    _result = manager.delete_campaign_file()
    manager.create()
    assert api_archive.exists()
    manager.add_dataset([str(heat_dataset)], name="heat")
    manager.add_image(str(image_files[0]), name="T0")
    manager.add_image(str(image_files[1]), name="T1", store=True)
    manager.add_image(str(image_files[2]), name="T2", thumbnail=[64, 64])
    manager.add_text(str(readme_file), name="readme", store=True)

    info_data = manager.info(
        list_replicas=True,
        list_files=True,
        show_deleted=True,
        show_checksum=True,
    )

    output = format_info(info_data, build_info_args())
    print(output)


if __name__ == "__main__":
    main()
