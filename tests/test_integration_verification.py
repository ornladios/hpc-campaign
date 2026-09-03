import argparse
import subprocess
from itertools import product
from pathlib import Path

import pytest

from tests.integration import verify_campaigns, verify_campaigns_incontainer
from tests.integration.create_campaigns import CAMPAIGN_NAMES, DATASET_PATHS


def test_failure_summary_preserves_multiline_adios_exception():
    output = """Traceback (most recent call last):
  File \"verify_campaigns.py\", line 1, in <module>
RuntimeError: [ADIOS2 EXCEPTION] <CampaignReader> : useful details
: iostream error
"""

    assert verify_campaigns.summarize_failure(output, 1) == (
        "RuntimeError: [ADIOS2 EXCEPTION] <CampaignReader> : useful details : iostream error"
    )


def test_failure_summary_uses_last_line_for_other_errors():
    output = "Traceback (most recent call last):\nValueError: useful details\n"

    assert verify_campaigns.summarize_failure(output, 1) == "ValueError: useful details"


def test_failure_summary_handles_empty_output():
    assert verify_campaigns.summarize_failure("", 7) == "child process exited with status 7"


def test_verifier_help_documents_single_read_options(monkeypatch, capsys):
    monkeypatch.setattr("sys.argv", ["verify_campaigns", "--help"])

    with pytest.raises(SystemExit) as exc_info:
        verify_campaigns.parse_args()

    assert exc_info.value.code == 0
    help_text = capsys.readouterr().out
    normalized_help = " ".join(help_text.split())
    assert "--single-campaign CAMPAIGN" in help_text
    assert "--single-dataset DATASET" in help_text
    assert "verify only CAMPAIGN instead of every campaign" in help_text
    assert "read only DATASET or variable from each selected campaign" in normalized_help


@pytest.mark.parametrize(
    ("selection", "dataset_name"),
    (
        ("testdata/readme", "testdata/readme"),
        ("testdata/heat_10_15.bp/T", "testdata/heat_10_15.bp"),
        ("testdata/heat_10_15.h5/T", "testdata/heat_10_15.h5"),
        ("testdata/T_10_15_00000.png/800x800", "testdata/T_10_15_00000.png"),
    ),
)
def test_variable_selection_maps_to_catalog_dataset(selection, dataset_name):
    assert verify_campaigns.campaign_dataset_name(selection) == dataset_name


@pytest.mark.parametrize(
    ("single_campaign", "single_dataset", "expected_campaigns", "expected_datasets"),
    (
        (None, None, CAMPAIGN_NAMES, DATASET_PATHS),
        ("testdata-xrootd-tar.aca", None, ("testdata-xrootd-tar.aca",), DATASET_PATHS),
        (None, "testdata/readme", CAMPAIGN_NAMES, ("testdata/readme",)),
    ),
)
def test_verifier_filters_campaigns_and_datasets_independently(
    monkeypatch,
    capsys,
    single_campaign,
    single_dataset,
    expected_campaigns,
    expected_datasets,
):
    campaign_store = Path("/campaigns")
    metadata_paths = []
    commands = []
    monkeypatch.setattr(
        verify_campaigns,
        "parse_args",
        lambda: argparse.Namespace(
            campaign_store=campaign_store,
            single_campaign=single_campaign,
            single_dataset=single_dataset,
            read_timeout=15,
        ),
    )
    monkeypatch.setattr(verify_campaigns, "verify_metadata", metadata_paths.append)

    def run(command, **_kwargs):
        commands.append(command)
        return subprocess.CompletedProcess(command, 0, stdout="PASS", stderr="")

    monkeypatch.setattr(verify_campaigns.subprocess, "run", run)

    verify_campaigns.main()

    assert metadata_paths == [campaign_store / name for name in expected_campaigns]
    requested_reads = {
        (
            command[command.index("--single-campaign") + 1],
            command[command.index("--single-dataset") + 1],
        )
        for command in commands
    }
    assert requested_reads == set(product(expected_campaigns, expected_datasets))
    expected_total = len(expected_campaigns) * len(expected_datasets)
    assert f"All {expected_total} reads passed" in capsys.readouterr().out


def test_campaign_and_dataset_combination_runs_one_read(monkeypatch):
    campaign_name = "testdata-xrootd-tar.aca"
    dataset_path = "testdata/heat_10_15.bp/T"
    calls = []
    monkeypatch.setattr(
        verify_campaigns,
        "parse_args",
        lambda: argparse.Namespace(
            campaign_store=Path("/campaigns"),
            single_campaign=campaign_name,
            single_dataset=dataset_path,
            read_timeout=15,
        ),
    )
    monkeypatch.setattr(verify_campaigns, "verify_campaign", lambda *args: calls.append(args))

    verify_campaigns.main()

    assert calls == [
        (
            Path("/campaigns") / campaign_name,
            (dataset_path,),
            r"^testdata/heat_10_15\.bp$",
        )
    ]


def test_incontainer_verifier_checks_services_before_reads(monkeypatch):
    calls = []

    monkeypatch.setattr(
        verify_campaigns_incontainer,
        "check_port",
        lambda host, port: calls.append((host, port)),
    )
    monkeypatch.setattr(
        verify_campaigns_incontainer,
        "verify_campaigns",
        lambda: calls.append(("verify", 0)),
    )

    verify_campaigns_incontainer.main()

    assert calls == [
        ("s3-service.docker.hpc-campaign", 9000),
        ("https-service.docker.hpc-campaign", 443),
        ("ssh-service.docker.hpc-campaign", 22),
        ("xrootd-service.docker.hpc-campaign", 8080),
        ("localhost", 30000),
        ("verify", 0),
    ]
