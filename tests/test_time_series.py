"""Create a small BP time series and verify it through the ADIOS campaign reader."""

import adios2
import numpy as np
import pytest

from hpc_campaign.index import Index
from hpc_campaign.manager import Manager


def test_read_bp_time_series(tmp_path, monkeypatch):
    # Relative replica paths are combined with this directory by CampaignReader.
    monkeypatch.chdir(tmp_path)
    filenames = ["fields.0000.bp", "fields.0001.bp"]
    expected = [np.arange(12, dtype=np.float64).reshape(3, 4) + step for step in range(2)]
    for filename, data in zip(filenames, expected, strict=True):
        with adios2.Stream(filename, "w") as stream:
            stream.begin_step()
            stream.write("temperature", data, data.shape, [0, 0], data.shape)
            stream.end_step()

    archive = tmp_path / "timeseries.aca"
    manager = Manager(archive.name, campaign_store=str(tmp_path))
    try:
        manager.open(create=True)
        for filename in filenames:
            manager.data(filename)
        manager.add_time_series("fields", filenames, replace=True)
    finally:
        manager.close()

    adios = adios2.Adios()
    io = adios.declare_io("time-series-check")
    io.set_engine("Campaign")
    io.set_parameters({"CachePath": str(tmp_path / "cache")})
    with adios2.FileReader(io, str(archive)) as reader:
        variable = reader.inquire_variable("fields/temperature")
        assert variable is not None
        assert variable.steps() == 2
        assert variable.shape() == [3, 4]
        for step, data in enumerate(expected):
            np.testing.assert_array_equal(reader.read("fields/temperature", step_selection=[step, 1]), data)

    index = Index(str(tmp_path / "series.acx"), campaign_store=str(tmp_path))
    try:
        index.add([str(archive)])
        rows = index.con.execute("SELECT name, steps, ndim, shape, min, max FROM variables").fetchall()
        assert len(rows) == 1
        assert rows[0][:4] == ("temperature", 2, 2, "3, 4")
        assert tuple(map(float, rows[0][4:])) == (0, 12)
    finally:
        index.close()


def test_index_time_series_membership_and_variable_steps(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    # Register members in a different order from their logical series ordering.
    for filename, count, base in [("second.bp", 3, 100), ("first.bp", 2, -10), ("other.bp", 1, 7)]:
        with adios2.Stream(filename, "w") as stream:
            for step in range(count):
                stream.begin_step()
                data = np.arange(6, dtype=np.int64).reshape(2, 3) + base + step
                stream.write("temperature", data, data.shape, [0, 0], data.shape)
                stream.write("time", np.array(base + step, dtype=np.int64))
                if filename == "second.bp":
                    stream.write("late", data, data.shape, [0, 0], data.shape)
                stream.end_step()
    archive = tmp_path / "series.aca"
    manager = Manager(str(archive), campaign_store=str(tmp_path))
    manager.open(create=True)
    for filename in ["second.bp", "first.bp", "other.bp"]:
        manager.data(filename)
    manager.add_time_series("fields", ["first.bp", "second.bp"])
    manager.add_time_series("other", ["other.bp"])
    manager.add_time_series("empty", [])
    info = manager.info()
    assert [[ds.name for ds in series.datasets.values()] for series in info.time_series.values()] == [
        ["first.bp", "second.bp"],
        ["other.bp"],
        [],
    ]

    index = Index(str(tmp_path / "series.acx"), campaign_store=str(tmp_path))
    try:
        for _ in range(2):
            index.add([str(archive)])
        assert index.con.execute("SELECT name FROM timeseries ORDER BY name").fetchall() == [
            ("empty",),
            ("fields",),
            ("other",),
        ]
        rows = index.con.execute(
            "SELECT d.name, v.name, v.steps, v.ndim, v.shape, v.min, v.max, v.value "
            "FROM variables v JOIN datasets d USING(datasetid) WHERE d.name = 'fields' ORDER BY v.name"
        ).fetchall()
        assert [(r[0], r[1], r[2], r[3], r[4]) for r in rows] == [
            ("fields", "late", 3, 2, "2, 3"),
            ("fields", "temperature", 5, 2, "2, 3"),
            ("fields", "time", 5, 0, ""),
        ]
        assert [(float(r[5]), float(r[6])) for r in rows] == [(100, 107), (-10, 107), (-10, 102)]
        assert rows[2][7] == "[-10, -9, 100, 101, 102]"
        membership = index.con.execute(
            "SELECT t.name, d.name, td.tsorder FROM timeseries t "
            "JOIN timeseries_datasets td USING(tsid) JOIN datasets d ON d.datasetid = td.datasetid "
            "ORDER BY t.name, td.tsorder"
        ).fetchall()
        assert membership == [("fields", "first.bp", 0), ("fields", "second.bp", 1), ("other", "other.bp", 0)]
        mapping = index.con.execute(
            "SELECT v.name, d.name, v.step_start, v.steps FROM timeseries_variable_steps v "
            "JOIN timeseries t USING(tsid) JOIN datasets d ON d.datasetid = v.datasetid "
            "WHERE t.name = 'fields' ORDER BY v.name, v.step_start"
        ).fetchall()
        assert mapping == [
            ("late", "second.bp", 0, 3),
            ("temperature", "first.bp", 0, 2),
            ("temperature", "second.bp", 2, 3),
            ("time", "first.bp", 0, 2),
            ("time", "second.bp", 2, 3),
        ]

        # A failed refresh must leave the previous membership and summaries intact.
        with monkeypatch.context() as patch:

            def fail_refresh(*args):
                raise RuntimeError("failed")

            patch.setattr(index, "_index_time_series", fail_refresh)
            with pytest.raises(RuntimeError, match="failed"):
                index.add([str(archive)])
        assert index.con.execute("SELECT COUNT(*) FROM timeseries_variable_steps").fetchone()[0] == 7

        manager.add_time_series("fields", ["first.bp"], replace=True)
        index.add([str(archive)])
        assert index.con.execute(
            "SELECT v.name, v.steps FROM variables v JOIN datasets d USING(datasetid) "
            "WHERE d.name = 'fields' ORDER BY v.name"
        ).fetchall() == [("temperature", 2), ("time", 2)]
        assert index.con.execute("PRAGMA foreign_key_check").fetchall() == []
        index.remove([])
        for table in ["datasets", "variables", "timeseries", "timeseries_datasets", "timeseries_variable_steps"]:
            assert index.con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0] == 0
    finally:
        index.close()
        manager.close()
