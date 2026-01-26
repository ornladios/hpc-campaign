import argparse
import logging
import subprocess
import sys
from pathlib import Path

from hpc_campaign.info import format_info
from hpc_campaign.ls import ls
from hpc_campaign.manager import Manager
from hpc_campaign.rm import rm

LOGGER = logging.getLogger(__name__)

repo_root = Path(__file__).resolve().parents[1]
campaign_store = repo_root

data_dir = Path("data")
cmdline_archive = data_dir / "test_cmdline.aca"
api_archive = data_dir / "test_api.aca"
heat_data = data_dir / "heat.bp"
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


def run_command(cmd: str, args: list[str]) -> subprocess.CompletedProcess:
    command = [
        sys.executable,
        "-m",
        "hpc_campaign",
        cmd,
        "--campaign_store",
        str(campaign_store),
    ]
    command.extend([str(entry) for entry in args])
    return subprocess.run(command, check=True, capture_output=True, text=True)


def normalize_info_output(output_text: str) -> str:
    # remove the first line from CLI output that is like ======..
    lines = output_text.splitlines()
    if lines and lines[0].startswith("=========="):
        lines = lines[1:]
    return "\n".join(lines).strip()


def build_info_args() -> argparse.Namespace:
    return argparse.Namespace(
        list_replicas=True,
        list_files=True,
        show_deleted=True,
        show_checksum=True,
    )


def test_02_manager_instantiation():
    manager = Manager(archive=str(api_archive), campaign_store=str(campaign_store))
    assert isinstance(manager, Manager)


def test_04_create_api():
    # assert not api_archive.exists()
    manager = Manager(archive=str(api_archive), campaign_store=str(campaign_store))
    manager.open(create=True, truncate=True)
    manager.close()
    assert api_archive.exists()


def test_05_data_cli():
    run_manager_command([str(cmdline_archive), "data", str(heat_data), "--name", "heat"])
    assert cmdline_archive.exists()


def test_06_data_api():
    manager = Manager(archive=str(api_archive), campaign_store=str(campaign_store))
    manager.open()
    manager.data([str(heat_data)], name="heat")
    manager.close()
    assert api_archive.exists()


def test_07_image_cli():
    run_manager_command([str(cmdline_archive), "image", str(image_files[0]), "--name", "T0"])
    run_manager_command([str(cmdline_archive), "image", str(image_files[1]), "--name", "T1", "--store"])
    run_manager_command([str(cmdline_archive), "image", str(image_files[2]), "--name", "T2", "--thumbnail", "64", "64"])


def test_08_image_api():
    manager = Manager(archive=str(api_archive), campaign_store=str(campaign_store))
    # leaving out  manager.open()/manager.close() to test it works this way too
    manager.image(str(image_files[0]), name="T0")
    manager.image(str(image_files[1]), name="T1", store=True)
    manager.image(str(image_files[2]), name="T2", thumbnail=[64, 64])


def test_09_text_cli():
    run_manager_command([str(cmdline_archive), "text", str(readme_file), "--name", "readme", "--store"])


def test_10_text_api():
    manager = Manager(archive=str(api_archive), campaign_store=str(campaign_store))
    manager.text(str(readme_file), name="readme", store=True)


def test_11_info_cli():
    result = run_manager_command([str(cmdline_archive), "info", "-rfdc"])
    info_outputs["cli"] = normalize_info_output(result.stdout)
    LOGGER.debug(f"test_11_info_cli info_outputs:\n{info_outputs}")
    assert info_outputs["cli"]


def test_12_info_api():
    # run info on cmdline archive so that we can compare the outputs of cli vs api
    manager = Manager(archive=str(cmdline_archive), campaign_store=str(campaign_store))
    info_data = manager.info(
        list_replicas=True,
        list_files=True,
        show_deleted=True,
        show_checksum=True,
    )
    api_output = normalize_info_output(format_info(info_data))
    LOGGER.debug(f"test_12_info_api info_outputs:\n{info_outputs}")
    assert "cli" in info_outputs
    assert api_output == info_outputs["cli"]


def test_20_ls_cli():
    res = run_command("ls", [str(cmdline_archive)])
    res.check_returncode()
    assert res.stdout == str(cmdline_archive) + "\n"


def test_21_ls_api():
    result = ls(str(api_archive), campaign_store=str(campaign_store))
    assert len(result) == 1
    assert result[0] == str(api_archive)


def test_30_delete_cli():
    run_command("rm", [str(cmdline_archive), "--force"])
    assert not cmdline_archive.exists()


def test_31_delete_api():
    rm(str(api_archive), campaign_store=str(campaign_store), force=True)
