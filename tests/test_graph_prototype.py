import pytest

from viur.core import errors
from viur.core.prototypes.graph import Graph


def test_graph_handler_name():
    assert Graph.handler == "graph"


def test_normalize_direction_defaults_to_both():
    assert Graph._normalize_direction(None) == "both"


def test_normalize_direction_accepts_known_values():
    assert Graph._normalize_direction("in") == "in"
    assert Graph._normalize_direction("OUT") == "out"
    assert Graph._normalize_direction("both") == "both"


def test_normalize_direction_rejects_unknown_values():
    with pytest.raises(errors.NotAcceptable):
        Graph._normalize_direction("sideways")


def test_as_bool_parsing():
    assert Graph._as_bool(True) is True
    assert Graph._as_bool(False) is False
    assert Graph._as_bool("1") is True
    assert Graph._as_bool("false") is False
    assert Graph._as_bool(None, default=False) is False
