import subprocess
import sys
import uuid
from pathlib import Path

import pytest
from PIL import Image
from prov.model import PROV, ProvActivity, ProvDerivation, ProvEntity, ProvGeneration, ProvUsage

from examples.provenance_hello_world import build_example as build_hello_world
from examples.provenance_hello_world import describe_ancestry
from examples.provenance_workflow import build_example
from hpc_campaign import Manager
from hpc_campaign.prov_mapping import HPC
from tools.benchmark_provenance_scale import Scenario, build_document, measure


def _variable(document, dataset, variable):
    return next(
        record.identifier
        for record in document.get_records(ProvEntity)
        if record.get_attribute(HPC["datasetName"]) == {dataset} and record.get_attribute(HPC["variable"]) == {variable}
    )


def _activities(document):
    return {
        next(iter(record.get_attribute(PROV["label"]))): record.identifier
        for record in document.get_records(ProvActivity)
    }


def test_hello_world_records_reduction_and_visualization(tmp_path: Path, capsys):
    export_path = build_hello_world(tmp_path)

    expected_variables = {
        "simulation.txt": "pressure",
        "reduced.txt": "pressure",
    }
    for filename, variable in expected_variables.items():
        assert (tmp_path / filename).read_text(encoding="utf-8") == (
            f"This is a placeholder for variables:\n- {variable}\n"
        )

    manager = Manager(archive="provenance-hello-world.aca", campaign_store=str(tmp_path))
    manager.open()
    document = manager.prov_document("campaign-provenance")
    asserted_types = {
        asserted_type for record in document.get_records() for asserted_type in record.get_asserted_types()
    }

    assert HPC["Reduction"] in asserted_types
    assert HPC["Visualization"] in asserted_types
    assert HPC["Simulation"] in asserted_types
    images = [record for record in document.get_records(ProvEntity) if HPC["Image"] in record.get_asserted_types()]
    assert len(images) == 1
    assert HPC["LogicalVariable"] not in images[0].get_asserted_types()
    with Image.open(tmp_path / "pressure.png") as image:
        assert image.format == "PNG"
    pressure = _variable(document, "simulation", "pressure")
    reduced_pressure = _variable(document, "reduced", "pressure")
    image = images[0].identifier
    activities = _activities(document)
    assert {(record.args[0], record.args[1]) for record in document.get_records(ProvGeneration)} == {
        (pressure, activities["simulation"]),
        (reduced_pressure, activities["reduction"]),
        (image, activities["visualization"]),
    }
    assert {(record.args[0], record.args[1]) for record in document.get_records(ProvDerivation)} == {
        (reduced_pressure, pressure),
        (image, reduced_pressure),
    }
    expected = (
        "pressure-image\n"
        "  produced by visualization\n"
        "    derived from reduced.pressure\n"
        "      produced by reduction\n"
        "        derived from simulation.pressure\n"
        "          produced by simulation"
    )
    assert describe_ancestry(document, image) == expected
    assert "Image ancestry:\n" + expected in capsys.readouterr().out
    assert export_path.is_file()
    manager.close()


def test_end_to_end_example_builds_a_reopenable_scientific_graph(tmp_path: Path, capsys):
    export_path = build_example(tmp_path)

    payload_dir = tmp_path / "provenance-example-payloads"
    expected_variables = {
        "simulation-output.txt": "P",
        "reduced-products.txt": "P_reduced",
        "qoi-products.txt": "total_pressure",
    }
    for filename, variable in expected_variables.items():
        assert (payload_dir / filename).read_text(encoding="utf-8") == (
            f"This is a placeholder for variables:\n- {variable}\n"
        )

    manager = Manager(archive="provenance-example.aca", campaign_store=str(tmp_path))
    manager.open()
    document = manager.prov_document("campaign-provenance")
    asserted_types = {
        asserted_type for record in document.get_records() for asserted_type in record.get_asserted_types()
    }

    # This protects the example's intended teaching path, not just its syntax:
    # all three processing stages must reach persistent canonical PROV.
    assert len(list(document.get_records(ProvActivity))) == 4
    assert HPC["Simulation"] in asserted_types
    assert HPC["Reduction"] in asserted_types
    assert HPC["QuantityOfInterest"] in asserted_types
    assert HPC["Visualization"] in asserted_types
    assert HPC["Image"] in asserted_types
    pressure = _variable(document, "simulation-output", "P")
    reduced_pressure = _variable(document, "reduced-products", "P_reduced")
    total_pressure = _variable(document, "qoi-products", "total_pressure")
    image = manager.dataset_entity("pressure-image")
    activities = _activities(document)
    assert {(record.args[0], record.args[1]) for record in document.get_records(ProvGeneration)} == {
        (pressure, activities["simulation"]),
        (reduced_pressure, activities["reduction"]),
        (total_pressure, activities["quantity_of_interest"]),
        (image, activities["visualization"]),
    }
    assert {(record.args[0], record.args[1]) for record in document.get_records(ProvDerivation)} == {
        (reduced_pressure, pressure),
        (total_pressure, reduced_pressure),
        (image, reduced_pressure),
        (image, total_pressure),
    }
    # The Fides configuration is recorded as context, but cannot appear among
    # the causal ancestors displayed for the image.
    context = next(
        record.identifier
        for record in document.get_records(ProvEntity)
        if record.get_attribute(PROV["label"]) == {"Fides data model"}
    )
    assert any(record.args[:2] == (activities["visualization"], context) for record in document.get_records(ProvUsage))
    before = document.serialize(format="json")
    expected = (
        "pressure-image\n"
        "  produced by visualization\n"
        "    derived from qoi-products.total_pressure\n"
        "      produced by quantity_of_interest\n"
        "        derived from reduced-products.P_reduced\n"
        "          produced by reduction\n"
        "            derived from simulation-output.P\n"
        "              produced by simulation\n"
        "    derived from reduced-products.P_reduced (already shown)"
    )
    assert describe_ancestry(document, image) == expected
    assert document.serialize(format="json") == before
    assert "Image ancestry:\n" + expected in capsys.readouterr().out
    assert export_path.is_file()
    manager.close()


@pytest.mark.parametrize("module", ["provenance_hello_world", "provenance_workflow"])
@pytest.mark.parametrize("invocation", ["script", "module"])
def test_examples_print_ancestry_from_script_and_module_entrypoints(tmp_path: Path, module, invocation):
    arguments = [f"examples/{module}.py"] if invocation == "script" else ["-m", f"examples.{module}"]
    result = subprocess.run(
        [sys.executable, *arguments, str(tmp_path / "campaign")],
        cwd=Path(__file__).resolve().parents[1],
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr
    assert "Image ancestry:\npressure-image\n  produced by visualization\n" in result.stdout
    assert "produced by simulation" in result.stdout


def test_scale_model_matches_materialized_record_count():
    scenario = Scenario(
        simulations=2,
        variables_per_simulation=3,
        reductions_per_variable=1,
        qois_per_variable=2,
        visualizations_per_simulation=1,
    )
    document = build_document(
        scenario,
        # A fixed UUID makes the benchmark graph deterministic across runs.
        uuid.UUID("8c13a4ce-3662-43cd-a03b-2fdfd53bd65e"),
    )

    assert len(list(document.get_records())) == scenario.expected_records
    assert scenario.products == 26


def test_small_scale_measurement_exercises_storage_round_trip():
    result = measure(
        Scenario(
            simulations=1,
            variables_per_simulation=1,
            reductions_per_variable=1,
            qois_per_variable=1,
            visualizations_per_simulation=0,
        ),
        repeats=1,
    )

    # The benchmark is versioned code, so a fast CI-sized case verifies every
    # measured phase without turning performance numbers into brittle limits.
    assert result.records == result.scenario.expected_records
    assert result.canonical_bytes > result.gzip_bytes > 0
    assert result.sqlite_bytes > 0
    assert result.sqlite_rewrite_seconds >= 0
