import json
import uuid
from pathlib import Path

import pytest
from prov.model import (
    PROV,
    ProvActivity,
    ProvAssociation,
    ProvDerivation,
    ProvDocument,
    ProvEntity,
    ProvGeneration,
    ProvUsage,
)

from hpc_campaign import ActivityResult, Manager, VariableSpec
from hpc_campaign.prov_mapping import HPC, CampaignProvIds


def _new_manager(tmp_path: Path, name: str = "campaign.aca") -> Manager:
    manager = Manager(archive=name, campaign_store=str(tmp_path))
    manager.open(create=True, truncate=True)
    return manager


def _add_text_dataset(manager: Manager, tmp_path: Path, name: str) -> None:
    source = tmp_path / f"{name}.txt"
    source.write_text(f"payload for {name}\n", encoding="utf-8")
    manager.text(source, name=name, store=True)


def _active_document(manager: Manager) -> ProvDocument:
    active = manager.prov_documents(active=True)
    assert len(active) == 1
    return manager.prov_document(active[0].document_id)


def _stored_text(manager: Manager) -> str:
    row = manager.con.execute("SELECT content FROM provenance_document WHERE name = 'campaign-provenance'").fetchone()
    assert row is not None
    return str(row[0])


def _one_record(document: ProvDocument, reference):
    records = document.get_record(reference)
    assert len(records) == 1
    return records[0]


def _campaign_with_pressure(tmp_path: Path, name: str = "campaign.aca"):
    manager = _new_manager(tmp_path, name)
    _add_text_dataset(manager, tmp_path, f"{name}-raw")
    _add_text_dataset(manager, tmp_path, f"{name}-products")
    run = manager.add_run("run-001")
    pressure = manager.add_variable(
        run=run,
        dataset=f"{name}-raw",
        variable="P",
        definition="pressure",
        units="Pa",
    )
    return manager, run, pressure


def _output_spec(run, dataset: str, variable: str, definition: str = "pressure", **kwargs):
    return VariableSpec(
        run=run,
        dataset=dataset,
        variable=variable,
        definition=definition,
        **kwargs,
    )


def test_reduction_records_exact_prov_relations_and_persists(tmp_path: Path):
    manager, run, pressure = _campaign_with_pressure(tmp_path)
    agent = manager.add_agent("software", "MGARD", version="5")
    plan = manager.add_plan("reduction workflow", location="scripts/reduce.py")

    result = manager.add_activity(
        "reduction",
        inputs={"source": pressure},
        outputs={
            "result": _output_spec(
                run,
                "campaign.aca-products",
                "P_reduced",
                units="Pa",
            )
        },
        action_spec={"method": "mgard", "error_bound": 0.001},
        agent=agent,
        plan=plan,
    )
    document_before_close = _active_document(manager)
    campaign_id = manager.campaign_uuid()

    assert isinstance(result, ActivityResult)
    with pytest.raises(TypeError):
        # A frozen result must not expose a mutable output dictionary.
        result.outputs["other"] = pressure  # type: ignore[index]

    activity = _one_record(document_before_close, result.activity)
    output = result.outputs["result"]
    assert isinstance(activity, ProvActivity)
    assert HPC["Reduction"] in activity.get_asserted_types()

    usages = [record for record in document_before_close.get_records(ProvUsage) if record.args[0] == result.activity]
    source_usage = next(record for record in usages if record.args[1] == pressure)
    spec_usage = next(record for record in usages if record.args[1] == result.action_specification)
    assert source_usage.get_attribute(PROV["role"]) == {HPC["source"]}
    assert spec_usage.get_attribute(PROV["role"]) == {HPC["action_specification"]}

    generations = [record for record in document_before_close.get_records(ProvGeneration) if record.args[0] == output]
    assert len(generations) == 1
    assert generations[0].args[1] == result.activity
    assert generations[0].get_attribute(PROV["role"]) == {HPC["result"]}

    derivations = list(document_before_close.get_records(ProvDerivation))
    assert len(derivations) == 1
    # Qualified derivation records the exact Activity, Generation, and Usage,
    # rather than only asserting a loose output-to-input relationship.
    assert derivations[0].args == (
        output,
        pressure,
        result.activity,
        generations[0].identifier,
        source_usage.identifier,
    )

    associations = [
        record for record in document_before_close.get_records(ProvAssociation) if record.args[0] == result.activity
    ]
    assert len(associations) == 1
    assert associations[0].args[1:] == (agent, plan)

    specification = _one_record(document_before_close, result.action_specification)
    expected_json = json.dumps(
        {"method": "mgard", "error_bound": 0.001},
        sort_keys=True,
        separators=(",", ":"),
    )
    assert PROV["Plan"] in specification.get_asserted_types()
    assert HPC["ActionSpecification"] in specification.get_asserted_types()
    assert specification.get_attribute(PROV["value"]) == {expected_json}

    manager.close()
    reopened = Manager(archive="campaign.aca", campaign_store=str(tmp_path))
    reopened.open()
    assert reopened.campaign_uuid() == campaign_id
    assert _active_document(reopened) == document_before_close
    reopened.close()


def test_explicit_derivations_select_parents_for_each_output(tmp_path: Path):
    manager, run, pressure = _campaign_with_pressure(tmp_path)
    temperature = manager.add_variable(
        run=run,
        dataset="campaign.aca-raw",
        variable="T",
        definition="temperature",
        units="eV",
    )

    result = manager.add_activity(
        "quantity_of_interest",
        inputs={"pressure": pressure, "temperature": temperature},
        outputs={
            "field": _output_spec(run, "campaign.aca-products", "flux", "flux"),
            "total": _output_spec(run, "campaign.aca-products", "total_flux", "total_flux"),
        },
        derivations={"field": ["pressure", "temperature"], "total": ["pressure"]},
    )
    derivations = list(_active_document(manager).get_records(ProvDerivation))

    assert len(derivations) == 3
    assert {(record.args[0], record.args[1]) for record in derivations} == {
        (result.outputs["field"], pressure),
        (result.outputs["field"], temperature),
        (result.outputs["total"], pressure),
    }
    manager.close()


def test_derivation_roles_may_contain_underscores(tmp_path: Path):
    # Qualified Derivation identifiers include both the output and input role.
    # Underscores are valid within either token, so validation must use the
    # referenced Generation and Usage instead of splitting on underscores.
    manager, run, magnetic_x = _campaign_with_pressure(tmp_path)
    magnetic_y = manager.add_variable(
        run=run,
        dataset="campaign.aca-raw",
        variable="By",
        definition="magnetic_y",
    )

    result = manager.add_activity(
        "quantity_of_interest",
        inputs={"magnetic_x": magnetic_x, "magnetic_y": magnetic_y},
        outputs={
            "divergence_result": _output_spec(
                run,
                "campaign.aca-products",
                "div_b",
                "magnetic_divergence",
            )
        },
    )
    derivations = list(_active_document(manager).get_records(ProvDerivation))

    assert len(derivations) == 2
    assert {record.args[1] for record in derivations} == {magnetic_x, magnetic_y}
    activity_id = uuid.UUID(hex=result.activity.localpart.removeprefix("activity_"))
    identifiers = CampaignProvIds(manager.campaign_uuid())
    assert {record.identifier for record in derivations} == {
        identifiers.derivation(activity_id, "divergence_result", "magnetic_x"),
        identifiers.derivation(activity_id, "divergence_result", "magnetic_y"),
    }
    assert all(record.args[0] == result.outputs["divergence_result"] for record in derivations)
    manager.close()


def test_default_derivations_are_all_inputs_to_all_outputs(tmp_path: Path):
    manager, run, pressure = _campaign_with_pressure(tmp_path)
    temperature = manager.add_variable(
        run=run,
        dataset="campaign.aca-raw",
        variable="T",
        definition="temperature",
    )

    result = manager.add_activity(
        "projection",
        inputs={"pressure": pressure, "temperature": temperature},
        outputs={
            "first": _output_spec(run, "campaign.aca-products", "projection_1", "projection"),
            "second": _output_spec(run, "campaign.aca-products", "projection_2", "projection"),
        },
    )
    derivations = list(_active_document(manager).get_records(ProvDerivation))

    # With no explicit map, both outputs inherit both scientific inputs.
    assert {(record.args[0], record.args[1]) for record in derivations} == {
        (output, source) for output in result.outputs.values() for source in (pressure, temperature)
    }
    manager.close()


def test_context_is_used_but_is_not_a_lineage_parent(tmp_path: Path):
    manager, run, pressure = _campaign_with_pressure(tmp_path)
    data_model = manager.add_plan("Fides data model", location="vis/fides.json")

    result = manager.add_activity(
        "visualization",
        inputs={"color": pressure},
        context={"data_model": data_model},
        outputs={
            "image": _output_spec(
                run,
                "campaign.aca-products",
                "pressure_image",
                "pressure_visualization",
            )
        },
    )
    document = _active_document(manager)
    usages = [record for record in document.get_records(ProvUsage) if record.args[0] == result.activity]
    derivations = list(document.get_records(ProvDerivation))

    assert {record.args[1] for record in usages} == {pressure, data_model}
    assert {(record.args[0], record.args[1]) for record in derivations} == {(result.outputs["image"], pressure)}
    manager.close()


def test_identical_action_specs_are_deduplicated_by_content(tmp_path: Path):
    manager, run, pressure = _campaign_with_pressure(tmp_path)
    specification = {"method": "mgard", "parameters": {"tolerance": 1e-4}}

    first = manager.add_activity(
        "reduction",
        inputs={"source": pressure},
        outputs={"result": _output_spec(run, "campaign.aca-products", "reduced_1")},
        action_spec=specification,
    )
    second = manager.add_activity(
        "reduction",
        inputs={"source": pressure},
        outputs={"result": _output_spec(run, "campaign.aca-products", "reduced_2")},
        action_spec={"parameters": {"tolerance": 1e-4}, "method": "mgard"},
    )
    document = _active_document(manager)

    assert first.action_specification == second.action_specification
    assert len(document.get_record(first.action_specification)) == 1
    assert (
        len(
            [
                record
                for record in document.get_records(ProvEntity)
                if HPC["ActionSpecification"] in record.get_asserted_types()
            ]
        )
        == 1
    )
    manager.close()


@pytest.mark.parametrize(
    ("action", "activity_type"),
    [
        ("reduction", HPC["Reduction"]),
        ("projection", HPC["Projection"]),
        ("quantity_of_interest", HPC["QuantityOfInterest"]),
        ("visualization", HPC["Visualization"]),
    ],
)
def test_supported_activity_actions(action, activity_type, tmp_path: Path):
    manager, run, pressure = _campaign_with_pressure(tmp_path, f"{action}.aca")
    result = manager.add_activity(
        action,
        inputs={"source": pressure},
        outputs={
            "result": _output_spec(
                run,
                f"{action}.aca-products",
                f"{action}_result",
                "result",
            )
        },
    )

    assert activity_type in _one_record(_active_document(manager), result.activity).get_asserted_types()
    manager.close()


def test_invalid_activity_requests_do_not_change_canonical_prov(tmp_path: Path):
    manager, run, pressure = _campaign_with_pressure(tmp_path)
    base_output = _output_spec(run, "campaign.aca-products", "reduced")
    before = _stored_text(manager)

    invalid_calls = [
        lambda: manager.add_activity(
            "unsupported",
            inputs={"source": pressure},
            outputs={"result": base_output},
        ),
        lambda: manager.add_activity(
            "reduction",
            inputs={"bad-role": pressure},
            outputs={"result": base_output},
        ),
        lambda: manager.add_activity(
            "reduction",
            inputs={"source": pressure},
            outputs={"result": base_output},
            derivations={"result": ["missing"]},
        ),
        lambda: manager.add_activity(
            "reduction",
            inputs={"source": pressure},
            outputs={"result": base_output},
            action_spec={"invalid": {1, 2}},
        ),
        lambda: manager.add_activity(
            "reduction",
            inputs={"source": pressure},
            outputs={"result": _output_spec(run, "missing", "reduced")},
        ),
    ]
    for invalid_call in invalid_calls:
        with pytest.raises((LookupError, TypeError, ValueError)):
            invalid_call()
        # Authoring is transactional at the document boundary: validation or
        # construction failures leave the exact canonical JSON untouched.
        assert _stored_text(manager) == before
    manager.close()


def test_cross_campaign_and_duplicate_output_ids_are_atomic(tmp_path: Path):
    first, run, pressure = _campaign_with_pressure(tmp_path, "first.aca")
    second, _, foreign_pressure = _campaign_with_pressure(tmp_path, "second.aca")
    output_id = uuid.uuid4()
    first.add_activity(
        "reduction",
        inputs={"source": pressure},
        outputs={
            "result": _output_spec(
                run,
                "first.aca-products",
                "reduced",
                variable_id=output_id,
            )
        },
    )
    before = _stored_text(first)

    with pytest.raises(ValueError, match="another campaign"):
        first.add_activity(
            "reduction",
            inputs={"source": foreign_pressure},
            outputs={"result": _output_spec(run, "first.aca-products", "foreign")},
        )
    assert _stored_text(first) == before

    with pytest.raises(ValueError, match="identifier already exists"):
        first.add_activity(
            "reduction",
            inputs={"source": pressure},
            outputs={
                "result": _output_spec(
                    run,
                    "first.aca-products",
                    "duplicate",
                    variable_id=output_id,
                )
            },
        )
    assert _stored_text(first) == before
    first.close()
    second.close()


def test_qualified_relation_identifiers_use_the_activity_uuid(tmp_path: Path):
    manager, run, pressure = _campaign_with_pressure(tmp_path)
    activity_id = uuid.uuid4()
    result = manager.add_activity(
        "reduction",
        activity_id=activity_id,
        inputs={"source": pressure},
        outputs={"result": _output_spec(run, "campaign.aca-products", "reduced")},
    )
    document = _active_document(manager)
    identifiers = CampaignProvIds(manager.campaign_uuid())

    assert result.activity == identifiers.activity(activity_id)
    assert _one_record(document, identifiers.usage(activity_id, "source")).args == (
        result.activity,
        pressure,
        None,
    )
    assert _one_record(document, identifiers.generation(activity_id, "result")).args == (
        result.outputs["result"],
        result.activity,
        None,
    )
    assert _one_record(document, identifiers.derivation(activity_id, "result", "source")).args[:2] == (
        result.outputs["result"],
        pressure,
    )
    manager.close()
