#!/usr/bin/env python3
"""Create the smallest complete HPC Campaign provenance example."""

from __future__ import annotations

import argparse
from pathlib import Path

from hpc_campaign import Manager, VariableSpec


def build_example(
    campaign_store: Path,
    archive: str = "provenance-hello-world.aca",
) -> Path:
    """Record simulation -> pressure -> reduction -> visualization."""

    campaign_store.mkdir(parents=True, exist_ok=True)
    archive_path = campaign_store / archive
    if archive_path.exists():
        raise FileExistsError(f"refusing to replace existing campaign: {archive_path}")

    manager = Manager(archive=archive, campaign_store=str(campaign_store))
    manager.open(create=True)
    try:
        # Provenance refers to datasets registered in the campaign. These tiny
        # text files stand in for simulation, reduction, and image data.
        datasets = {
            "simulation": ["pressure"],
            "reduced": ["pressure"],
            "visualization": ["pressure.png"],
        }
        for dataset, variables in datasets.items():
            payload = campaign_store / f"{dataset}.txt"
            contents = "This is a placeholder for variables:\n"
            contents += "".join(f"- {variable}\n" for variable in variables)
            payload.write_text(contents, encoding="utf-8")
            manager.text(payload, name=dataset, store=True)

        # The simulation run generates the original pressure variable.
        simulation = manager.add_run("simulation")
        pressure = manager.add_variable(
            run=simulation,
            dataset="simulation",
            variable="pressure",
            definition="pressure",
        )

        # The reduction consumes pressure and generates reduced pressure.
        reduction = manager.add_activity(
            "reduction",
            inputs={"pressure": pressure},
            outputs={
                "reduced_pressure": VariableSpec(
                    run=simulation,
                    dataset="reduced",
                    variable="pressure",
                    definition="pressure",
                )
            },
        )

        # The visualization consumes reduced pressure and generates an image.
        manager.add_activity(
            "visualization",
            inputs={"pressure": reduction.outputs["reduced_pressure"]},
            outputs={
                "image": VariableSpec(
                    run=simulation,
                    dataset="visualization",
                    variable="pressure.png",
                    definition="pressure_visualization",
                )
            },
        )

        export_path = campaign_store / "provenance-hello-world.json"
        manager.export_prov("campaign-provenance", export_path)
        return export_path
    finally:
        manager.close()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("campaign_store", type=Path)
    args = parser.parse_args()
    export_path = build_example(args.campaign_store)
    print(f"PROV-JSON export: {export_path}")


if __name__ == "__main__":
    main()
