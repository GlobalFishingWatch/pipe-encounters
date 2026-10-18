"""Helpers for the source tables given in the CLI."""


def split_id_prefix(source: str) -> tuple[str, str]:
    """Splits a source table, optionally "ID::table", into its seg_id prefix and table.

    The prefix keeps the seg_ids of different sources (e.g. AIS and VMS) distinct.
    """
    if "::" not in source:
        return "", source

    id_prefix, table = source.split("::", 1)
    return f"{id_prefix}:", table
