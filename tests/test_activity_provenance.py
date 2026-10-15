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
    ProvGeneration,
    ProvUsage,
)

from hpc_campaign import ActivityResult, Manager
from hpc_campaign.prov_mapping import HPC, CampaignProvIds
from hpc_campaign.prov_validation import ProvenanceValidationError


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
    pressure = manager.add_variable(
        dataset=f"{name}-raw",
        variable="P",
        definition="pressure",
        units="Pa",
    )
    simulation = manager.add_activity("simulation", outputs={"pressure": pressure})
    return manager, simulation, pressure


def _add_variable(manager, dataset: str, variable: str, definition: str = "pressure", **kwargs):
    return manager.add_variable(
        dataset=dataset,
        variable=variable,
        definition=definition,
        **kwargs,
    )


def test_reduction_records_exact_prov_relations_and_persists(tmp_path: Path):
    manager, _simulation, pressure = _campaign_with_pressure(tmp_path)
    agent = manager.add_agent("software", "MGARD", version="5")
    plan = manager.add_plan("reduction workflow", location="scripts/reduce.py")

    result = manager.add_activity(
        "reduction",
        inputs={"source": pressure},
        outputs={
            "result": _add_variable(
                manager,
                "campaign.aca-products",
                "P_reduced",
                units="Pa",
            )
        },
        metadata={"method": "mgard", "error_bound": 0.001},
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
    assert source_usage.get_attribute(PROV["role"]) == {HPC["source"]}
    assert activity.get_attribute(HPC["metadata"]) == {'{"error_bound":0.001,"method":"mgard"}'}

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

    manager.close()
    reopened = Manager(archive="campaign.aca", campaign_store=str(tmp_path))
    reopened.open()
    assert reopened.campaign_uuid() == campaign_id
    assert _active_document(reopened) == document_before_close
    reopened.close()


def test_explicit_derivations_select_parents_for_each_output(tmp_path: Path):
    manager, _simulation, pressure = _campaign_with_pressure(tmp_path)
    temperature = manager.add_variable(
        dataset="campaign.aca-raw",
        variable="T",
        definition="temperature",
        units="eV",
    )

    result = manager.add_activity(
        "quantity_of_interest",
        inputs={"pressure": pressure, "temperature": temperature},
        outputs={
            "field": _add_variable(manager, "campaign.aca-products", "flux", "flux"),
            "total": _add_variable(manager, "campaign.aca-products", "total_flux", "total_flux"),
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
    manager, _simulation, magnetic_x = _campaign_with_pressure(tmp_path)
    magnetic_y = manager.add_variable(
        dataset="campaign.aca-raw",
        variable="By",
        definition="magnetic_y",
    )

    result = manager.add_activity(
        "quantity_of_interest",
        inputs={"magnetic_x": magnetic_x, "magnetic_y": magnetic_y},
        outputs={
            "divergence_result": _add_variable(
                manager,
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
    manager, _simulation, pressure = _campaign_with_pressure(tmp_path)
    temperature = manager.add_variable(
        dataset="campaign.aca-raw",
        variable="T",
        definition="temperature",
    )

    result = manager.add_activity(
        "projection",
        inputs={"pressure": pressure, "temperature": temperature},
        outputs={
            "first": _add_variable(manager, "campaign.aca-products", "projection_1", "projection"),
            "second": _add_variable(manager, "campaign.aca-products", "projection_2", "projection"),
        },
    )
    derivations = list(_active_document(manager).get_records(ProvDerivation))

    # With no explicit map, both outputs inherit both scientific inputs.
    assert {(record.args[0], record.args[1]) for record in derivations} == {
        (output, source) for output in result.outputs.values() for source in (pressure, temperature)
    }
    manager.close()


def test_context_is_used_but_is_not_a_lineage_parent(tmp_path: Path):
    manager, _simulation, pressure = _campaign_with_pressure(tmp_path)
    data_model = manager.add_plan("Fides data model", location="vis/fides.json")

    result = manager.add_activity(
        "visualization",
        inputs={"color": pressure},
        context={"data_model": data_model},
        outputs={
            "image": _add_variable(
                manager,
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


def test_activity_metadata_is_stored_on_each_activity(tmp_path: Path):
    manager, _simulation, pressure = _campaign_with_pressure(tmp_path)
    specification = {"method": "mgard", "parameters": {"tolerance": 1e-4}}

    first = manager.add_activity(
        "reduction",
        inputs={"source": pressure},
        outputs={"result": _add_variable(manager, "campaign.aca-products", "reduced_1")},
        metadata=specification,
    )
    second = manager.add_activity(
        "reduction",
        inputs={"source": pressure},
        outputs={"result": _add_variable(manager, "campaign.aca-products", "reduced_2")},
        metadata={"parameters": {"tolerance": 1e-4}, "method": "mgard"},
    )
    document = _active_document(manager)

    assert first.activity != second.activity
    assert _one_record(document, first.activity).get_attribute(HPC["metadata"]) == {
        '{"method":"mgard","parameters":{"tolerance":0.0001}}'
    }
    assert _one_record(document, second.activity).get_attribute(HPC["metadata"]) == {
        '{"method":"mgard","parameters":{"tolerance":0.0001}}'
    }
    manager.close()


@pytest.mark.parametrize(
    "metadata",
    [
        {1: "top-level integer key"},
        {"parameters": {1: "nested integer key"}},
        {"parameters": {None: "nested null key"}},
        {"stages": [{"parameters": {False: "boolean key inside a list"}}]},
        {"stages": [[{"parameters": {0.5: "numeric key inside nested lists"}}]]},
        {"stages": ({"parameters": {(1, 2): "tuple key inside a tuple"}},)},
    ],
)
def test_nonstring_metadata_keys_are_rejected_at_every_depth_atomically(tmp_path: Path, metadata):
    manager, _simulation, pressure = _campaign_with_pressure(tmp_path)
    output = _add_variable(manager, "campaign.aca-products", "reduced")
    before = tuple(
        manager.con.execute(
            "SELECT content, sha256, active, modtime FROM provenance_document WHERE name = 'campaign-provenance'"
        ).fetchone()
    )

    with pytest.raises(ValueError, match="metadata keys must be strings"):
        manager.add_activity("reduction", inputs={"source": pressure}, outputs={"result": output}, metadata=metadata)

    assert (
        tuple(
            manager.con.execute(
                "SELECT content, sha256, active, modtime FROM provenance_document WHERE name = 'campaign-provenance'"
            ).fetchone()
        )
        == before
    )
    manager.close()


def test_nested_metadata_preserves_string_keys_and_shared_containers(tmp_path: Path):
    manager, _simulation, pressure = _campaign_with_pressure(tmp_path)
    output = _add_variable(manager, "campaign.aca-products", "reduced")
    parameters = {"1": "a string key", "tolerance": 0.001}
    result = manager.add_activity(
        "reduction",
        inputs={"source": pressure},
        outputs={"result": output},
        metadata={"stages": [parameters, {"nested": (parameters,)}]},
    )

    activity = _one_record(_active_document(manager), result.activity)
    stored = next(iter(activity.get_attribute(HPC["metadata"])))
    assert json.loads(stored) == {"stages": [parameters, {"nested": [parameters]}]}
    manager.close()


@pytest.mark.parametrize("container_type", [dict, list])
def test_circular_metadata_still_raises_a_value_error(tmp_path: Path, container_type):
    manager, _simulation, pressure = _campaign_with_pressure(tmp_path)
    output = _add_variable(manager, "campaign.aca-products", "reduced")
    circular = container_type()
    if isinstance(circular, dict):
        circular["self"] = circular
    else:
        circular.append(circular)
    before = _stored_text(manager)

    with pytest.raises(ValueError, match="only JSON-compatible values"):
        manager.add_activity(
            "reduction",
            inputs={"source": pressure},
            outputs={"result": output},
            metadata={"parameters": circular},
        )

    assert _stored_text(manager) == before
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
    manager, _simulation, pressure = _campaign_with_pressure(tmp_path, f"{action}.aca")
    result = manager.add_activity(
        action,
        inputs={"source": pressure},
        outputs={
            "result": _add_variable(
                manager,
                f"{action}.aca-products",
                f"{action}_result",
                "result",
            )
        },
    )

    assert activity_type in _one_record(_active_document(manager), result.activity).get_asserted_types()
    manager.close()


def test_invalid_activity_requests_do_not_change_canonical_prov(tmp_path: Path):
    manager, _simulation, pressure = _campaign_with_pressure(tmp_path)
    base_output = _add_variable(manager, "campaign.aca-products", "reduced")
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
            metadata={"invalid": {1, 2}},
        ),
        lambda: manager.add_activity(
            "reduction",
            inputs={"source": pressure},
            outputs={"result": CampaignProvIds(manager.campaign_uuid()).variable(uuid.uuid4())},
        ),
    ]
    for invalid_call in invalid_calls:
        with pytest.raises((LookupError, TypeError, ValueError)):
            invalid_call()
        # Authoring is transactional at the document boundary: validation or
        # construction failures leave the exact canonical JSON untouched.
        assert _stored_text(manager) == before
    manager.close()


def test_cross_campaign_outputs_and_multiple_producers_are_atomic(tmp_path: Path):
    first, _simulation, pressure = _campaign_with_pressure(tmp_path, "first.aca")
    second, _, foreign_pressure = _campaign_with_pressure(tmp_path, "second.aca")
    output_id = uuid.uuid4()
    first_output = _add_variable(
        first,
        "first.aca-products",
        "reduced",
        variable_id=output_id,
    )
    first.add_activity(
        "reduction",
        inputs={"source": pressure},
        outputs={"result": first_output},
    )
    before = _stored_text(first)

    foreign_output = _add_variable(second, "second.aca-products", "foreign")
    with pytest.raises(ValueError, match="another campaign"):
        first.add_activity(
            "reduction",
            inputs={"source": foreign_pressure},
            outputs={"result": foreign_output},
        )
    assert _stored_text(first) == before

    with pytest.raises(ValueError, match="already has a producing Activity"):
        first.add_activity(
            "reduction",
            inputs={"source": pressure},
            outputs={"result": first_output},
        )
    assert _stored_text(first) == before
    first.close()
    second.close()


def test_qualified_relation_identifiers_use_the_activity_uuid(tmp_path: Path):
    manager, _simulation, pressure = _campaign_with_pressure(tmp_path)
    activity_id = uuid.uuid4()
    result = manager.add_activity(
        "reduction",
        activity_id=activity_id,
        inputs={"source": pressure},
        outputs={"result": _add_variable(manager, "campaign.aca-products", "reduced")},
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


def test_authoring_rejects_a_multi_activity_cycle_atomically(tmp_path: Path):
    manager = _new_manager(tmp_path)
    _add_text_dataset(manager, tmp_path, "fields")
    first = manager.add_variable(dataset="fields", variable="A", definition="a")
    second = manager.add_variable(dataset="fields", variable="B", definition="b")
    manager.add_activity("projection", inputs={"source": first}, outputs={"result": second})
    before = tuple(
        manager.con.execute(
            "SELECT content, sha256, active, modtime FROM provenance_document WHERE name = 'campaign-provenance'"
        ).fetchone()
    )

    with pytest.raises(ProvenanceValidationError, match="derivation cycle"):
        manager.add_activity("projection", inputs={"source": second}, outputs={"result": first})

    assert (
        tuple(
            manager.con.execute(
                "SELECT content, sha256, active, modtime FROM provenance_document WHERE name = 'campaign-provenance'"
            ).fetchone()
        )
        == before
    )
    manager.close()


def test_context_usage_is_not_treated_as_a_causal_edge(tmp_path: Path):
    manager = _new_manager(tmp_path)
    _add_text_dataset(manager, tmp_path, "fields")
    first = manager.add_variable(dataset="fields", variable="A", definition="a")
    second = manager.add_variable(dataset="fields", variable="B", definition="b")
    manager.add_activity("projection", inputs={"source": first}, outputs={"result": second})
    manager.add_activity("simulation", context={"reference": second}, outputs={"result": first})

    assert {(record.args[0], record.args[1]) for record in _active_document(manager).get_records(ProvDerivation)} == {
        (second, first)
    }
    manager.close()


@pytest.mark.parametrize("import_kind", ["active", "inactive", "bundled", "duplicate"])
def test_activity_resolves_products_context_and_associations_from_active_imports(tmp_path: Path, import_kind):
    manager = _new_manager(tmp_path)
    _add_text_dataset(manager, tmp_path, "fields")
    dataset = manager.add_dataset_entity("fields")
    identifiers = CampaignProvIds(manager.campaign_uuid())
    imported = ProvDocument()
    imported.add_namespace(HPC)
    imported.add_namespace(identifiers.namespace)
    target = imported
    if import_kind == "bundled":
        imported.add_namespace("external", "urn:imported:")
        target = imported.bundle("external:records")

    products = {}
    for name in ("source", "output"):
        variable_id = uuid.uuid4()
        reference = identifiers.variable(variable_id)
        target.entity(
            reference,
            [
                (PROV["type"], HPC["LogicalVariable"]),
                (HPC["logicalVariableId"], str(variable_id)),
                (HPC["dataset"], dataset),
                (HPC["datasetName"], "fields"),
                (HPC["variable"], name),
                (HPC["variableDefinition"], name),
                (PROV["location"], "fields.txt"),
            ],
        )
        products[name] = reference
    agent = identifiers.agent(uuid.uuid4())
    plan = identifiers.plan(uuid.uuid4())
    target.agent(agent, [(PROV["type"], PROV["SoftwareAgent"])])
    target.entity(plan, [(PROV["type"], PROV["Plan"])])
    simulation_id = uuid.uuid4()
    simulation = identifiers.activity(simulation_id)
    target.activity(simulation, other_attributes=[(PROV["type"], HPC["Simulation"])])
    target.wasGeneratedBy(
        products["source"],
        simulation,
        identifier=identifiers.generation(simulation_id, "source"),
        other_attributes=[(PROV["role"], HPC["source"])],
    )
    manager.add_prov_document(imported, name="imported", activate=import_kind != "inactive")
    if import_kind == "duplicate":
        manager.add_prov_document(imported, name="duplicate", activate=True)
    imported_before = tuple(
        manager.con.execute(
            "SELECT content, sha256, active, modtime FROM provenance_document WHERE name = 'imported'"
        ).fetchone()
    )
    authored_before = _stored_text(manager)

    if import_kind == "inactive":
        with pytest.raises(LookupError, match="does not resolve"):
            manager.add_activity(
                "reduction", inputs={"source": products["source"]}, outputs={"result": products["output"]}
            )
        assert _stored_text(manager) == authored_before
        manager.close()
        return

    result = manager.add_activity(
        "reduction",
        inputs={"source": products["source"]},
        outputs={"result": products["output"]},
        context={"recipe": plan},
        agent=agent,
        plan=plan,
    )
    authored = manager.prov_document("campaign-provenance")
    assert next(authored.get_records(ProvDerivation)).args[:3] == (
        products["output"],
        products["source"],
        result.activity,
    )
    # Imported declarations stay in their original document, including bundles.
    assert not any(authored.get_record(reference) for reference in (*products.values(), agent, plan, simulation))
    assert (
        tuple(
            manager.con.execute(
                "SELECT content, sha256, active, modtime FROM provenance_document WHERE name = 'imported'"
            ).fetchone()
        )
        == imported_before
    )
    before = _stored_text(manager)
    with pytest.raises(ValueError, match="already has a producing Activity"):
        manager.add_activity("simulation", outputs={"result": products["source"]})
    with pytest.raises(ValueError, match="identifier already exists"):
        manager.add_activity("simulation", outputs={"result": products["source"]}, activity_id=simulation_id)
    assert _stored_text(manager) == before
    manager.close()

    reopened = Manager(archive="campaign.aca", campaign_store=str(tmp_path))
    reopened.open()
    assert reopened.prov_document("campaign-provenance") == authored
    reopened.close()
