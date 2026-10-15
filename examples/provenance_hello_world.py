#!/usr/bin/env python3
"""Create the smallest complete HPC Campaign provenance example."""

from __future__ import annotations

import argparse
from pathlib import Path

from PIL import Image
from prov.model import PROV, ProvDerivation, ProvDocument, ProvGeneration, QualifiedName

from hpc_campaign import Manager
from hpc_campaign.prov_mapping import HPC


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
        # text files stand in for simulation and reduction data.
        datasets = {
            "simulation": ["pressure"],
            "reduced": ["pressure"],
        }
        for dataset, variables in datasets.items():
            payload = campaign_store / f"{dataset}.txt"
            contents = "This is a placeholder for variables:\n"
            contents += "".join(f"- {variable}\n" for variable in variables)
            payload.write_text(contents, encoding="utf-8")
            manager.text(payload, name=dataset, store=True)

        # Declare pressure as a logical variable, then record the simulation
        # Activity that produced it. The simulation has no causal inputs.
        pressure = manager.add_variable(
            dataset="simulation",
            variable="pressure",
            definition="pressure",
        )
        manager.add_activity(
            "simulation",
            name="simulation",
            outputs={"pressure": pressure},
        )

        # Declare reduced pressure, then record its causal source and producer.
        reduced_pressure = manager.add_variable(
            dataset="reduced",
            variable="pressure",
            definition="pressure",
        )
        manager.add_activity(
            "reduction",
            name="reduction",
            inputs={"source": pressure},
            outputs={"reduced_pressure": reduced_pressure},
        )

        # A tiny valid PNG stands in for output from an external renderer.
        # Its dataset Entity is an Image, with no artificial variable wrapper.
        image_path = campaign_store / "pressure.png"
        Image.new("RGB", (16, 16), color="navy").save(image_path)
        manager.image(image_path, name="pressure-image", store=True)
        image = manager.add_dataset_entity("pressure-image")
        manager.add_activity(
            "visualization",
            name="visualization",
            inputs={"color_by": reduced_pressure},
            outputs={"image": image},
        )

        export_path = campaign_store / "provenance-hello-world.json"
        manager.export_prov("campaign-provenance", export_path)

        # Read the persisted graph to inspect the image's complete ancestry.
        document = manager.prov_document("campaign-provenance")
        print("Image ancestry:")
        print(describe_ancestry(document, image))
        return export_path
    finally:
        manager.close()


def describe_ancestry(document: ProvDocument, entity: QualifiedName) -> str:
    """Describe a product's ancestors using recorded Generation and Derivation.

    Usage alone does not establish causality, so Activity context stays out of
    this display. Shared ancestors are expanded once and marked on later paths.
    This small example helper reads a stored PROV document without changing it.
    """

    producers = {record.args[0]: record.args[1] for record in document.get_records(ProvGeneration)}
    parents = {}
    for record in document.get_records(ProvDerivation):
        parents.setdefault(record.args[0], set()).add(record.args[1])

    def label(reference):
        records = document.get_record(reference)
        if not records:
            return str(reference)
        record = records[0]
        labels = record.get_attribute(PROV["label"])
        if labels:
            return sorted(str(value) for value in labels)[0]
        attributes = dict(record.attributes)
        dataset = attributes.get(HPC["datasetName"])
        variable = attributes.get(HPC["variable"])
        if dataset is not None and variable is not None:
            return f"{dataset}.{variable}"
        return str(reference)

    lines = []
    pending = [(entity, 0, "")]
    seen = set()
    while pending:
        current, depth, relationship = pending.pop()
        repeated = current in seen
        suffix = " (already shown)" if repeated else ""
        lines.append("  " * depth + relationship + label(current) + suffix)
        if repeated:
            continue
        seen.add(current)
        producer = producers.get(current)
        if producer is not None:
            lines.append("  " * (depth + 1) + "produced by " + label(producer))
        parent_depth = depth + 2 if producer is not None else depth + 1
        for parent in reversed(sorted(parents.get(current, ()), key=label)):
            pending.append((parent, parent_depth, "derived from "))
    return "\n".join(lines)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("campaign_store", type=Path)
    args = parser.parse_args()
    export_path = build_example(args.campaign_store)
    print(f"PROV-JSON export: {export_path}")


if __name__ == "__main__":
    main()
