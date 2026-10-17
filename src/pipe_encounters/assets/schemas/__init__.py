"""Schemas of the output tables, as JSON files."""

from importlib.resources import files

from gfw.common.io import json_load


def get_schema(filename: str) -> list[dict]:
    """Returns the schema in ``filename``, a list of BigQuery field dicts."""
    return json_load(files("pipe_encounters.assets.schemas").joinpath(filename))
