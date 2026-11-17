import pytest

from hpc_campaign.types import VariableType, python_type_to_variable_type


@pytest.mark.parametrize("value", [[], (), {}], ids=["list", "tuple", "dict"])
def test_empty_collection_has_unknown_element_type(value):
    assert python_type_to_variable_type(value) == (0, VariableType.unknown)


def test_nested_empty_collection_has_unknown_element_type():
    assert python_type_to_variable_type([[]]) == (1, VariableType.unknown)
