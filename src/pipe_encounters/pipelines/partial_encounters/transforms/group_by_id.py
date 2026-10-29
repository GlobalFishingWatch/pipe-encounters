"""Grouping of records by vessel id and date."""

from typing import Any, NamedTuple

from apache_beam import GroupByKey, Map, PCollection, PTransform


class GroupByIdAndDate(PTransform):
    """Groups records by vessel id and date."""

    def tag_with_id(self, value: NamedTuple) -> tuple[str, NamedTuple]:
        """Key the record by its id and date."""
        return (f"{value.id}_{value.timestamp.date()}", value)

    def expand(self, xs: PCollection) -> PCollection:
        """Group the records by vessel id and date."""
        return xs | Map(self.tag_with_id).with_output_types(Any) | GroupByKey()
