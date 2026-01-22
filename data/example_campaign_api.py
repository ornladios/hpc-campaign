from pathlib import Path

from hpc_campaign.info import format_info
from hpc_campaign.ls import ls
from hpc_campaign.manager import Manager
from hpc_campaign.rm import rm

repo_root = Path(__file__).resolve().parents[1]
campaign_store = repo_root
data_dir = Path("data")

print(f"campaign_store = {repo_root}")
print(f"data_dir = {data_dir}")

api_archive = Path("example_api.aca")  #  will find it in repo_root
heat_dataset = data_dir / "heat.bp"
readme_file = data_dir / "readme"
image_files = [
    data_dir / "T00000.png",
    data_dir / "T00001.png",
    data_dir / "T00002.png",
]

info_outputs: dict[str, str] = {}


def main():
    manager = Manager(archive=str(api_archive), campaign_store=str(campaign_store))
    manager.open(create=True, truncate=True)
    assert repo_root.joinpath(api_archive).exists()
    manager.add_dataset([str(heat_dataset)], name="heat")
    manager.add_image(str(image_files[0]), name="T0")
    manager.add_image(str(image_files[1]), name="T1", store=True)
    manager.add_image(str(image_files[2]), name="T2", thumbnail=[64, 64])
    manager.add_text(str(readme_file), name="readme", store=True)

    info_data = manager.info(True, True, True, True)
    output = format_info(info_data)
    print(output)
    manager.close()

    # ls this aca
    result = ls(str(api_archive), campaign_store=str(campaign_store))
    print(f"ls result: {result}")
    assert len(result) == 1
    assert result[0] == str(api_archive)

    # rm this aca
    result = rm(str(api_archive), campaign_store=str(campaign_store))
    print(f"rm result: {result}")
    assert len(result) == 1
    assert result[0] == str(api_archive)


if __name__ == "__main__":
    main()
