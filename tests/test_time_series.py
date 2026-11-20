"""Create a small BP time series and verify it through the ADIOS campaign reader."""

import adios2
import numpy as np

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
