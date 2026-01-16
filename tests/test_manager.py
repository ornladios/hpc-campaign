import argparse
import subprocess
import sys
from pathlib import Path

from hpc_campaign.info import format_info, info
from hpc_campaign.manager import Manager

repo_root = Path(__file__).resolve().parents[1]
data_dir = repo_root / "data"
campaign_store = repo_root

cmdline_archive = data_dir / "test_cmdline.aca"
api_archive = data_dir / "test_api.aca"
heat_dataset = data_dir / "heat.bp"
readme_file = data_dir / "readme"
image_files = [
    data_dir / "T00000.png",
    data_dir / "T00001.png",
    data_dir / "T00002.png",
]

info_outputs: dict[str, str] = {}

def run_manager_command(args: list[str]) -> subprocess.CompletedProcess:
    command = [
        sys.executable,
        "-m",
        "hpc_campaign",
        "manager",
        "--campaign_store",
        str(campaign_store),
    ]
    command.extend([str(entry) for entry in args])
    return subprocess.run(command, check=True, capture_output=True, text=True)


def normalize_info_output(output_text: str) -> str:
    lines = output_text.splitlines()
    if lines and set(lines[0]) == {"="}:
        lines = lines[1:]
    return "\n".join(lines).strip()


def build_info_args() -> argparse.Namespace:
    return argparse.Namespace(
        list_replicas=True,
        list_files=True,
        show_deleted=True,
        show_checksum=True,
    )


def test_01_cleanup():
    # cleanup previous test run to avoid conflicts
    run_manager_command([str(cmdline_archive), "delete", "--campaign"])
    run_manager_command([str(api_archive), "delete", "--campaign"])
    assert not cmdline_archive.exists()
    assert not api_archive.exists()


def test_02_manager_instantiation():
    manager = Manager(archive=str(api_archive), campaign_store=str(campaign_store))
    assert isinstance(manager, Manager)


def test_03_create_cli():
    assert not cmdline_archive.exists()
    run_manager_command([str(cmdline_archive), "create"])
    assert cmdline_archive.exists()


def test_04_create_api():
    assert not api_archive.exists()
    manager = Manager(archive=str(api_archive), campaign_store=str(campaign_store))
    manager.create()
    assert api_archive.exists()


def test_05_dataset_cli():
    run_manager_command([str(cmdline_archive), "dataset", str(heat_dataset), "--name", "heat"])
    assert cmdline_archive.exists()


def test_06_dataset_api():
    manager = Manager(archive=str(api_archive), campaign_store=str(campaign_store))
    manager.add_dataset([str(heat_dataset)], name="heat")
    assert api_archive.exists()


def test_07_image_cli():
    run_manager_command([str(cmdline_archive), "image", str(image_files[0]), "--name", "T0"])
    run_manager_command([str(cmdline_archive), "image", str(image_files[1]), "--name", "T1", "--store"])
    run_manager_command([str(cmdline_archive), "image", str(image_files[2]), "--name", "T2", "--thumbnail", "64", "64"])


def test_08_image_api():
    manager = Manager(archive=str(api_archive), campaign_store=str(campaign_store))
    manager.add_image(str(image_files[0]), name="T0")
    manager.add_image(str(image_files[1]), name="T1", store=True)
    manager.add_image(str(image_files[2]), name="T2", thumbnail=[64, 64])


def test_09_text_cli():
    run_manager_command([str(cmdline_archive), "text", str(readme_file), "--name", "readme", "--store"])


def test_10_text_api():
    manager = Manager(archive=str(api_archive), campaign_store=str(campaign_store))
    manager.add_text(str(readme_file), name="readme", store=True)


def test_11_info_cli():
    result = run_manager_command([str(cmdline_archive), "info", "-rfdc"])
    info_outputs["cli"] = normalize_info_output(result.stdout)
    assert info_outputs["cli"]


def test_12_info_api():
    info_data = info(
        str(cmdline_archive),
        campaign_store=str(campaign_store),
        list_replicas=True,
        list_files=True,
        show_deleted=True,
        show_checksum=True,
    )
    api_output = normalize_info_output(format_info(info_data, build_info_args()))
    assert "cli" in info_outputs
    assert api_output == info_outputs["cli"]


def test_13_delete_cli():
    run_manager_command([str(cmdline_archive), "delete", "--campaign"])
    assert not cmdline_archive.exists()


def test_14_delete_api():
    manager = Manager(archive=str(api_archive), campaign_store=str(campaign_store))
    result = manager.delete_campaign_file()
    assert result == 0
    assert not api_archive.exists()
