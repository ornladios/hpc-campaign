from pathlib import Path

import pytest

from hpc_campaign.manager import Manager
from hpc_campaign.schema import SchemaInterpretationError

# These tests are the campaign-level schema validation tests. They build real
# ACA files with Manager, store __campaign_schema.yaml, register named datasets, and then
# call Manager.validate_schema().
#
# Validation here is metadata-only: schema paths and patterns must resolve to
# live dataset names in the ACA, and file-per-timestep groups may use the
# existing timeseries table for ordering. Extra campaign datasets are allowed.
# If a root schema does not match root-level datasets, it is applied to each
# immediate child directory as a separate run instance. These tests intentionally
# do not open ADIOS/HDF5 payloads or check variables inside data files.
repo_root = Path(__file__).resolve().parents[1]
data_dir = repo_root / "data"
schema_examples_dir = data_dir / "schema_examples"
sample_dataset = data_dir / "onearray.h5"


def schema_path(name: str) -> Path:
    # Each code_*.yaml fixture describes one supported layout shape. Manager
    # stores any selected fixture into the ACA as __campaign_schema.yaml.
    return schema_examples_dir / f"{name}.yaml"


def add_named_datasets(manager: Manager, dataset_names: list[str]):
    # Reuse one small HDF5 file for all logical dataset names. The validation
    # logic only needs ACA metadata names, not the file payload.
    for dataset_name in dataset_names:
        manager.data(sample_dataset, name=dataset_name)


def build_campaign(tmp_path: Path, archive_name: str, schema_name: str, dataset_names: list[str]) -> Manager:
    # Common setup: create an ACA, embed __campaign_schema.yaml, and register the datasets
    # that validation should match against the schema.
    manager = Manager(archive=archive_name, campaign_store=str(tmp_path))
    manager.open(create=True, truncate=True)
    manager.set_schema(schema_path(schema_name))
    add_named_datasets(manager, dataset_names)
    return manager


def build_campaign_from_schema_text(
    tmp_path: Path,
    archive_name: str,
    schema_text: str,
    dataset_names: list[str],
) -> Manager:
    schema_file = tmp_path / f"{archive_name}.__campaign_schema.yaml"
    schema_file.write_text(schema_text, encoding="utf-8")
    manager = Manager(archive=archive_name, campaign_store=str(tmp_path))
    manager.open(create=True, truncate=True)
    manager.set_schema(schema_file)
    add_named_datasets(manager, dataset_names)
    return manager


def test_validate_schema_single_append_allows_extra_campaign_datasets(tmp_path: Path):
    # The schema requires output.bp. extra.bp proves validation is not a closed
    # world check: unrelated campaign datasets are ignored.
    manager = build_campaign(
        tmp_path,
        "single_append.aca",
        "code_single_append",
        ["output.bp", "extra.bp"],
    )

    layout = manager.validate_schema()
    manager.close()

    assert layout["schema_name"] == "code_single_append"
    assert layout["file_groups"]["output"] == {
        "role": "time_series",
        "mode": "append",
        "datasets": ["output.bp"],
        "time": {"variable": "time"},
    }


def test_validate_schema_root_schema_applies_to_each_run_directory(tmp_path: Path):
    # The root schema is written in run-relative terms: path output.bp. Since no
    # root-level output.bp exists, validation applies the schema independently to
    # run1/ and run2/.
    manager = build_campaign(
        tmp_path,
        "multi_run_single_append.aca",
        "code_single_append",
        ["run1/output.bp", "run2/output.bp", "notes.txt"],
    )

    layout = manager.validate_schema()
    manager.close()

    assert sorted(layout["instances"]) == ["run1", "run2"]
    assert layout["instances"]["run1"]["file_groups"]["output"]["datasets"] == ["run1/output.bp"]
    assert layout["instances"]["run2"]["file_groups"]["output"]["datasets"] == ["run2/output.bp"]


def test_validate_schema_multifile_uses_existing_timeseries_order(tmp_path: Path):
    # The schema pattern finds both field datasets. The existing timeseries table
    # supplies the order, and step_from_filename extracts [0, 1].
    manager = build_campaign(
        tmp_path,
        "multifile.aca",
        "code_multifile",
        ["fields.0001.bp", "fields.0000.bp"],
    )
    manager.add_time_series("fields", ["fields.0000.bp", "fields.0001.bp"], replace=True)

    layout = manager.validate_schema()
    manager.close()

    assert layout["schema_name"] == "code_multifile"
    assert layout["file_groups"]["fields"] == {
        "role": "time_series",
        "mode": "file_per_timestep",
        "datasets": ["fields.0000.bp", "fields.0001.bp"],
        "step_indices": [0, 1],
        "time": {"index": "step_index"},
    }


def test_validate_schema_run_directories_use_scoped_timeseries_order(tmp_path: Path):
    # Scoped timeseries names let each run directory define its own ordered list
    # for the same schema file group.
    manager = build_campaign(
        tmp_path,
        "multi_run_multifile.aca",
        "code_multifile",
        [
            "run1/fields.0001.bp",
            "run1/fields.0000.bp",
            "run2/fields.0001.bp",
            "run2/fields.0000.bp",
        ],
    )
    manager.add_time_series("run1/fields", ["run1/fields.0000.bp", "run1/fields.0001.bp"], replace=True)
    manager.add_time_series("run2/fields", ["run2/fields.0000.bp", "run2/fields.0001.bp"], replace=True)

    layout = manager.validate_schema()
    manager.close()

    assert layout["instances"]["run1"]["file_groups"]["fields"] == {
        "role": "time_series",
        "mode": "file_per_timestep",
        "datasets": ["run1/fields.0000.bp", "run1/fields.0001.bp"],
        "step_indices": [0, 1],
        "time": {"index": "step_index"},
    }
    assert layout["instances"]["run2"]["file_groups"]["fields"] == {
        "role": "time_series",
        "mode": "file_per_timestep",
        "datasets": ["run2/fields.0000.bp", "run2/fields.0001.bp"],
        "step_indices": [0, 1],
        "time": {"index": "step_index"},
    }


def test_validate_schema_static_grid_append_fields(tmp_path: Path):
    # Static grid plus append-mode fields: both schema paths must exist in the
    # ACA, and fields keep their association to the grid file group.
    manager = build_campaign(
        tmp_path,
        "static_grid_append.aca",
        "code_static_grid_append",
        ["grid.bp", "fields.bp"],
    )

    layout = manager.validate_schema()
    manager.close()

    assert layout["file_groups"]["grid"] == {
        "role": "static",
        "mode": "none",
        "datasets": ["grid.bp"],
    }
    assert layout["file_groups"]["fields"] == {
        "role": "time_series",
        "mode": "append",
        "datasets": ["fields.bp"],
        "associations": {"grid": "grid"},
        "time": {"variable": "time"},
    }


def test_validate_schema_static_grid_multifile_fields(tmp_path: Path):
    # Static grid plus one-file-per-timestep fields: the grid path is a single
    # registered dataset, while field files are ordered by timeseries metadata.
    manager = build_campaign(
        tmp_path,
        "static_grid_multifile.aca",
        "code_static_grid_multifile",
        ["grid.bp", "fields.0001.bp", "fields.0000.bp"],
    )
    manager.add_time_series("fields", ["fields.0000.bp", "fields.0001.bp"], replace=True)

    layout = manager.validate_schema()
    manager.close()

    assert layout["file_groups"]["grid"] == {
        "role": "static",
        "mode": "none",
        "datasets": ["grid.bp"],
    }
    assert layout["file_groups"]["fields"] == {
        "role": "time_series",
        "mode": "file_per_timestep",
        "datasets": ["fields.0000.bp", "fields.0001.bp"],
        "step_indices": [0, 1],
        "associations": {"grid": "grid"},
        "time": {"index": "step_index"},
    }


def test_validate_schema_append_grid_append_fields(tmp_path: Path):
    # A grid does not have to be static. This validates an append-mode grid
    # associated with append-mode fields.
    manager = build_campaign(
        tmp_path,
        "append_grid_append.aca",
        "code_append_grid_append",
        ["grid.bp", "fields.bp"],
    )

    layout = manager.validate_schema()
    manager.close()

    assert layout["file_groups"]["grid"] == {
        "role": "time_series",
        "mode": "append",
        "datasets": ["grid.bp"],
    }
    assert layout["file_groups"]["fields"]["associations"] == {"grid": "grid"}
    assert layout["file_groups"]["fields"]["time"] == {"variable": "time"}


def test_validate_schema_append_grid_multifile_fields(tmp_path: Path):
    # Mixed layout: append-mode grid data plus fields stored as one dataset per
    # timestep. The field order still comes from the existing timeseries table.
    manager = build_campaign(
        tmp_path,
        "append_grid_multifile.aca",
        "code_append_grid_multifile",
        ["grid.bp", "fields.0001.bp", "fields.0000.bp"],
    )
    manager.add_time_series("fields", ["fields.0000.bp", "fields.0001.bp"], replace=True)

    layout = manager.validate_schema()
    manager.close()

    assert layout["file_groups"]["grid"] == {
        "role": "time_series",
        "mode": "append",
        "datasets": ["grid.bp"],
        "time": {"variable": "time"},
    }
    assert layout["file_groups"]["fields"] == {
        "role": "time_series",
        "mode": "file_per_timestep",
        "datasets": ["fields.0000.bp", "fields.0001.bp"],
        "step_indices": [0, 1],
        "associations": {"grid": "grid"},
    }


def test_validate_schema_root_time_without_file_applies_to_all_time_series_groups(tmp_path: Path):
    schema_text = """
schema_version: 1
name: root_time_default

files:
  grid:
    role: static
    path: grid.bp

  fields:
    role: time_series
    mode: append
    path: fields.bp

  diagnostics:
    role: time_series
    mode: append
    path: diagnostics.bp

time:
  variable: time
"""
    manager = build_campaign_from_schema_text(
        tmp_path,
        "root_time_default.aca",
        schema_text,
        ["grid.bp", "fields.bp", "diagnostics.bp"],
    )

    layout = manager.validate_schema()
    manager.close()

    assert "time" not in layout["file_groups"]["grid"]
    assert layout["file_groups"]["fields"]["time"] == {"variable": "time"}
    assert layout["file_groups"]["diagnostics"]["time"] == {"variable": "time"}


def test_validate_schema_group_time_overrides_root_default(tmp_path: Path):
    schema_text = """
schema_version: 1
name: group_time_override

files:
  fields:
    role: time_series
    mode: append
    path: fields.bp
    time:
      variable: physical_time

  diagnostics:
    role: time_series
    mode: append
    path: diagnostics.bp

time:
  variable: time
"""
    manager = build_campaign_from_schema_text(
        tmp_path,
        "group_time_override.aca",
        schema_text,
        ["fields.bp", "diagnostics.bp"],
    )

    layout = manager.validate_schema()
    manager.close()

    assert layout["file_groups"]["fields"]["time"] == {"variable": "physical_time"}
    assert layout["file_groups"]["diagnostics"]["time"] == {"variable": "time"}


def test_validate_schema_xgc(tmp_path: Path):
    manager = build_campaign(
        tmp_path,
        "xgc.aca",
        "code_xgc",
        [
            "xgc.mesh.bp",
            "xgc.f0.mesh.bp",
            "xgc.3d.00010.bp",
            "xgc.3d.00012.bp",
            "xgc.f3d.00010.bp",
            "xgc.f3d.00012.bp",
            "xgc.fsourcediag.00010.bp",
            "xgc.fsourcediag.00012.bp",
            "xgc.oneddiag.bp",
        ],
    )
    manager.add_time_series("xgc_3d", ["xgc.3d.00010.bp", "xgc.3d.00012.bp"], replace=True)
    manager.add_time_series("xgc_f3d", ["xgc.f3d.00010.bp", "xgc.f3d.00012.bp"], replace=True)
    manager.add_time_series(
        "xgc_fsourcediag",
        ["xgc.fsourcediag.00010.bp", "xgc.fsourcediag.00012.bp"],
        replace=True,
    )

    layout = manager.validate_schema()
    manager.close()

    assert layout["file_groups"]["mesh"] == {
        "role": "static",
        "mode": "none",
        "datasets": ["xgc.mesh.bp"],
    }
    assert layout["file_groups"]["f0_mesh"] == {
        "role": "static",
        "mode": "none",
        "datasets": ["xgc.f0.mesh.bp"],
    }
    assert layout["file_groups"]["xgc_3d"] == {
        "role": "time_series",
        "mode": "file_per_timestep",
        "datasets": ["xgc.3d.00010.bp", "xgc.3d.00012.bp"],
        "step_indices": [10, 12],
        "associations": {"mesh": "mesh"},
        "time": {"variable": "time"},
    }
    assert layout["file_groups"]["xgc_f3d"] == {
        "role": "time_series",
        "mode": "file_per_timestep",
        "datasets": ["xgc.f3d.00010.bp", "xgc.f3d.00012.bp"],
        "step_indices": [10, 12],
        "associations": {"mesh": "mesh", "f0_mesh": "f0_mesh"},
        "time": {"variable": "time"},
    }
    assert layout["file_groups"]["xgc_fsourcediag"] == {
        "role": "time_series",
        "mode": "file_per_timestep",
        "datasets": ["xgc.fsourcediag.00010.bp", "xgc.fsourcediag.00012.bp"],
        "step_indices": [10, 12],
        "associations": {"mesh": "mesh"},
        "time": {"variable": "time"},
    }
    assert layout["file_groups"]["xgc_oneddiag"] == {
        "role": "time_series",
        "mode": "append",
        "datasets": ["xgc.oneddiag.bp"],
        "time": {"variable": "time"},
    }


def test_validate_schema_fails_when_required_path_dataset_is_missing(tmp_path: Path):
    # A schema path is mandatory. code_static_grid_append names grid.bp, so a
    # campaign containing only fields.bp is invalid.
    manager = build_campaign(
        tmp_path,
        "missing_grid.aca",
        "code_static_grid_append",
        ["fields.bp"],
    )

    with pytest.raises(SchemaInterpretationError, match="files.grid.path"):
        manager.validate_schema()
    manager.close()


def test_validate_schema_fails_when_pattern_matches_no_campaign_dataset(tmp_path: Path):
    # A file-per-timestep pattern must match at least one live ACA dataset.
    # Unrelated datasets do not satisfy the file group.
    manager = build_campaign(
        tmp_path,
        "missing_pattern.aca",
        "code_multifile",
        ["other.0000.bp"],
    )

    with pytest.raises(SchemaInterpretationError, match="matched no datasets"):
        manager.validate_schema()
    manager.close()


def test_validate_schema_fails_when_any_run_directory_does_not_match_schema(tmp_path: Path):
    # Every immediate child directory is treated as an instance of the root
    # schema. run2/ is missing output.bp, so the campaign is invalid.
    manager = build_campaign(
        tmp_path,
        "multi_run_missing_output.aca",
        "code_single_append",
        ["run1/output.bp", "run2/not_output.bp"],
    )

    with pytest.raises(SchemaInterpretationError, match="run2: files.output.path"):
        manager.validate_schema()
    manager.close()


def test_validate_schema_fails_when_timeseries_member_does_not_match_pattern(tmp_path: Path):
    # Timeseries membership cannot smuggle unrelated datasets into a file group;
    # every ordered member must still match the schema pattern.
    manager = build_campaign(
        tmp_path,
        "bad_timeseries.aca",
        "code_multifile",
        ["fields.0000.bp", "other.0001.bp"],
    )
    manager.add_time_series("fields", ["fields.0000.bp", "other.0001.bp"], replace=True)

    with pytest.raises(SchemaInterpretationError, match="does not match"):
        manager.validate_schema()
    manager.close()


def test_validate_schema_fails_when_static_group_has_time(tmp_path: Path):
    schema_text = """
schema_version: 1
name: static_group_time

files:
  grid:
    role: static
    path: grid.bp
    time:
      variable: time
"""
    manager = build_campaign_from_schema_text(
        tmp_path,
        "static_group_time.aca",
        schema_text,
        ["grid.bp"],
    )

    with pytest.raises(SchemaInterpretationError, match="files.grid.time is only valid for time_series groups"):
        manager.validate_schema()
    manager.close()


def test_validate_schema_fails_when_group_time_has_no_source(tmp_path: Path):
    schema_text = """
schema_version: 1
name: group_time_no_source

files:
  fields:
    role: time_series
    mode: append
    path: fields.bp
    time: {}
"""
    manager = build_campaign_from_schema_text(
        tmp_path,
        "group_time_no_source.aca",
        schema_text,
        ["fields.bp"],
    )

    with pytest.raises(SchemaInterpretationError, match="files.fields.time requires exactly one"):
        manager.validate_schema()
    manager.close()


def test_validate_schema_fails_when_group_time_has_variable_and_index(tmp_path: Path):
    schema_text = """
schema_version: 1
name: group_time_ambiguous

files:
  fields:
    role: time_series
    mode: append
    path: fields.bp
    time:
      variable: time
      index: step_index
"""
    manager = build_campaign_from_schema_text(
        tmp_path,
        "group_time_ambiguous.aca",
        schema_text,
        ["fields.bp"],
    )

    with pytest.raises(SchemaInterpretationError, match="files.fields.time requires exactly one"):
        manager.validate_schema()
    manager.close()


def test_validate_schema_fails_when_group_time_names_file(tmp_path: Path):
    schema_text = """
schema_version: 1
name: group_time_file

files:
  fields:
    role: time_series
    mode: append
    path: fields.bp
    time:
      file: fields
      variable: time
"""
    manager = build_campaign_from_schema_text(
        tmp_path,
        "group_time_file.aca",
        schema_text,
        ["fields.bp"],
    )

    with pytest.raises(SchemaInterpretationError, match=r"files.fields.time.file is not supported"):
        manager.validate_schema()
    manager.close()


def test_validate_schema_fails_when_root_time_references_unknown_group(tmp_path: Path):
    schema_text = """
schema_version: 1
name: root_time_unknown_group

files:
  fields:
    role: time_series
    mode: append
    path: fields.bp

time:
  file: missing
  variable: time
"""
    manager = build_campaign_from_schema_text(
        tmp_path,
        "root_time_unknown_group.aca",
        schema_text,
        ["fields.bp"],
    )

    with pytest.raises(SchemaInterpretationError, match="time.file references unknown group: missing"):
        manager.validate_schema()
    manager.close()


def test_validate_schema_fails_when_root_time_references_static_group(tmp_path: Path):
    schema_text = """
schema_version: 1
name: root_time_static_group

files:
  grid:
    role: static
    path: grid.bp

time:
  file: grid
  variable: time
"""
    manager = build_campaign_from_schema_text(
        tmp_path,
        "root_time_static_group.aca",
        schema_text,
        ["grid.bp"],
    )

    with pytest.raises(SchemaInterpretationError, match="time.file references non-time_series group: grid"):
        manager.validate_schema()
    manager.close()
