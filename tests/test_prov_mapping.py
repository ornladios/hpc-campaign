import uuid

import pytest
from prov.model import (
    PROV,
    ProvAssociation,
    ProvAttribution,
    ProvDerivation,
    ProvDocument,
    ProvGeneration,
    ProvUsage,
)

from hpc_campaign.prov_mapping import HPC, CampaignProvIds, build_pressure_workflow_example

CAMPAIGN_ID = uuid.UUID("f3a547ce-e645-432e-8701-d2edb1764f6c")


def _one_record(document: ProvDocument, identifier):
    """Return one identified record and make accidental ID collisions obvious."""

    records = document.get_record(identifier)
    assert len(records) == 1
    return records[0]


def test_campaign_identifiers_are_stable_and_campaign_qualified():
    # The same persistent UUID must produce the same public PROV identity after
    # a database rebuild, while another campaign must produce a different URI.
    object_id = uuid.UUID("ef221228-4f84-4a83-881c-f5815868f91c")
    identifiers = CampaignProvIds(CAMPAIGN_ID)
    rebuilt = CampaignProvIds(str(CAMPAIGN_ID))
    other_campaign = CampaignProvIds(uuid.UUID("108fd59c-c87d-407d-ac2d-105e6b00c641"))

    assert identifiers.dataset(object_id) == rebuilt.dataset(object_id)
    assert identifiers.dataset(object_id) != other_campaign.dataset(object_id)
    assert str(identifiers.dataset(object_id)) == f"hpcid:dataset_{object_id.hex}"
    assert identifiers.variable(object_id) == rebuilt.variable(object_id)
    assert str(identifiers.variable(object_id)) == f"hpcid:variable_{object_id.hex}"


@pytest.mark.parametrize(
    ("factory", "expected"),
    [
        (lambda: CampaignProvIds("not-a-uuid"), "campaign_id must be a UUID"),
        (lambda: CampaignProvIds(CAMPAIGN_ID).variable("not-a-uuid"), "variable_id must be a UUID"),
        (lambda: CampaignProvIds(CAMPAIGN_ID).usage(uuid.uuid4(), "not-a-role"), "usage role must begin"),
    ],
)
def test_identifier_mapping_rejects_ambiguous_values(factory, expected):
    # Rejecting malformed local-name components keeps generated PROV qualified
    # names portable instead of relying on serializer-specific escaping.
    with pytest.raises(ValueError, match=expected):
        factory()


def test_representative_document_round_trips_through_prov_json():
    graph = build_pressure_workflow_example(CAMPAIGN_ID)

    serialized = graph.document.serialize(format="json")
    assert isinstance(serialized, str)
    loaded = ProvDocument.deserialize(content=serialized, format="json")

    # Equality checks the complete package object model, including namespaces,
    # record identifiers, attributes, and qualified relationship endpoints.
    assert loaded == graph.document

    # A type outside the initial campaign vocabulary remains intact. The
    # campaign profile may warn about it, but import/export must not erase it.
    unknown_activity = _one_record(loaded, graph.records["unknown_activity"])
    assert HPC["DomainSpecificOperation"] in unknown_activity.get_asserted_types()


def test_logical_variables_have_stable_unversioned_entity_identities():
    graph = build_pressure_workflow_example(CAMPAIGN_ID)
    pressure = _one_record(graph.document, graph.records["pressure"])

    # Version one treats a growing time-varying product as one entity. Its
    # identity therefore has no revision suffix or revision attribute; appends
    # to the underlying dataset do not create additional provenance records.
    assert "_r" not in str(pressure.identifier)
    assert pressure.get_attribute(HPC["revision"]) == set()


def test_qualified_derivations_reference_exact_activity_generation_and_usage():
    graph = build_pressure_workflow_example(CAMPAIGN_ID)
    document = graph.document

    visualization_derivations = [
        record for record in document.get_records(ProvDerivation) if record.args[2] == graph.records["visualization"]
    ]

    # The rendered image depends on two scientific inputs. Each derivation
    # names the exact usage and generation records, preserving their roles.
    assert len(visualization_derivations) == 2
    assert {record.args[1] for record in visualization_derivations} == {
        graph.records["reduced_pressure"],
        graph.records["flux"],
    }

    for derivation in visualization_derivations:
        generated_entity, used_entity, activity, generation_id, usage_id = derivation.args
        generation = _one_record(document, generation_id)
        usage = _one_record(document, usage_id)

        assert isinstance(generation, ProvGeneration)
        assert isinstance(usage, ProvUsage)
        assert generated_entity == graph.records["image"]
        assert used_entity in {graph.records["reduced_pressure"], graph.records["flux"]}
        assert activity == graph.records["visualization"]
        assert generation.args[:2] == (graph.records["image"], graph.records["visualization"])
        assert usage.args[:2] == (graph.records["visualization"], used_entity)
        assert generation.get_attribute(PROV["role"]) == {HPC["image"]}
        assert usage.get_attribute(PROV["role"]) in ({HPC["color"]}, {HPC["annotation"]})


def test_activity_context_is_not_silently_promoted_to_scientific_lineage():
    graph = build_pressure_workflow_example(CAMPAIGN_ID)

    # Fides was genuinely used to render the image, so it belongs in activity
    # dataflow/context. It is not automatically treated as source scientific
    # data for an entity-lineage query.
    visualization_usages = [
        record for record in graph.document.get_records(ProvUsage) if record.args[0] == graph.records["visualization"]
    ]
    assert any(record.args[1] == graph.records["fides_model"] for record in visualization_usages)

    image_derivations = [
        record for record in graph.document.get_records(ProvDerivation) if record.args[0] == graph.records["image"]
    ]
    assert all(record.args[1] != graph.records["fides_model"] for record in image_derivations)


def test_investigation_links_ai_activity_human_attribution_and_evidence():
    graph = build_pressure_workflow_example(CAMPAIGN_ID)

    # The comparison is performed by an AI agent, while attribution records the
    # human who accepts responsibility for the resulting campaign conclusion.
    comparison_associations = [
        record
        for record in graph.document.get_records(ProvAssociation)
        if record.args[0] == graph.records["comparison"]
    ]
    assert len(comparison_associations) == 1
    assert comparison_associations[0].args[1] == graph.records["ai_agent"]

    conclusion_attributions = [
        record
        for record in graph.document.get_records(ProvAttribution)
        if record.args[0] == graph.records["conclusion"]
    ]
    assert len(conclusion_attributions) == 1
    assert conclusion_attributions[0].args[1] == graph.records["robert_agent"]

    conclusion_derivations = [
        record for record in graph.document.get_records(ProvDerivation) if record.args[0] == graph.records["conclusion"]
    ]
    assert {record.args[1] for record in conclusion_derivations} == {
        graph.records["question"],
        graph.records["image"],
    }


def test_all_identified_records_resolve_inside_the_campaign_namespace():
    graph = build_pressure_workflow_example(CAMPAIGN_ID)
    campaign_uri = f"urn:hpc-campaign:{CAMPAIGN_ID}:"

    # Associations are allowed to be anonymous in PROV. Every record for which
    # the spike creates an identifier must remain inside this campaign.
    identified_records = [record for record in graph.document.get_records() if record.identifier is not None]
    assert identified_records
    assert all(record.identifier.namespace.uri == campaign_uri for record in identified_records)
