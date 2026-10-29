"""Transforms that add encounter ids to encounter records."""

import datetime as dt
import hashlib

from typing import Any

from apache_beam import Map, PCollection, PTransform


class AddRawEncounterId(PTransform):
    """Add a raw encounter id built from the two vessel segment ids and start time."""

    def __init__(self, prefix: str = "") -> None:
        """Initialize the transform.

        Args:
            prefix: String prepended to the text hashed into the id.
        """
        self.prefix = prefix

    def add_id(self, x: dict[str, Any]) -> dict[str, Any]:
        """Set the encounter_id of a raw encounter record.

        Args:
            x: Raw encounter record with start_time and vessel segment ids.

        Returns:
            The record with encounter_id added.
        """
        # Ensure that timestamps are naive or in UTC
        start = dt.datetime.fromtimestamp(x["start_time"], tz=dt.timezone.utc)
        text = f"{self.prefix}encounter|{x['vessel_1_seg_id']}|{x['vessel_2_seg_id']}|{start:%Y-%m-%d %H:%M:%S}"
        x["encounter_id"] = hashlib.md5(text.encode("latin-1")).hexdigest()
        return x

    def expand(self, xs: PCollection) -> PCollection:
        """Map add_id over the input collection.

        Args:
            xs: Collection of raw encounter records.

        Returns:
            Collection of records with encounter_id added.
        """
        return xs | Map(self.add_id).with_output_types(Any)


class AddEncounterId(PTransform):
    """Add an encounter id built from the first segment ids of each vessel and start time."""

    def __init__(self, prefix: str = "") -> None:
        """Initialize the transform.

        Args:
            prefix: String prepended to the text hashed into the id.
        """
        self.prefix = prefix

    def add_id(self, x: dict[str, Any]) -> dict[str, Any]:
        """Set the encounter_id of an encounter record.

        Args:
            x: Encounter record with start_time and vessel segment id lists.

        Returns:
            The record with encounter_id added.
        """
        # Ensure that timestamps are naive or in UTC
        start = dt.datetime.fromtimestamp(x["start_time"], tz=dt.timezone.utc)

        text = (
            f"{self.prefix}encounter|{x['vessel_1_seg_ids'][0]}|{x['vessel_2_seg_ids'][0]}|"
            + f"{start:%Y-%m-%d %H:%M:%S}"
        )
        x["encounter_id"] = hashlib.md5(text.encode("latin-1")).hexdigest()
        return x

    def expand(self, xs: PCollection) -> PCollection:
        """Map add_id over the input collection.

        Args:
            xs: Collection of encounter records.

        Returns:
            Collection of records with encounter_id added.
        """
        return xs | Map(self.add_id).with_output_types(Any)
