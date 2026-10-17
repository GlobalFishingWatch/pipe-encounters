"""Applies partial-encounters' transforms in plain Python, without Beam.

Each function does what the transform's expand() does: the same keys to group by, then the
transform's own methods. GroupByKey's order doesn't matter: each method sorts what it needs.
"""

from collections import defaultdict

from pipe_encounters.pipelines.partial_encounters.transforms.compute_adjacency import (
    ComputeAdjacency,
)
from pipe_encounters.pipelines.partial_encounters.transforms.compute_encounters import (
    ComputeEncounters,
)
from pipe_encounters.pipelines.partial_encounters.transforms.group_by_id import GroupByIdAndDate
from pipe_encounters.pipelines.partial_encounters.transforms.resample import Resample
from pipe_encounters.pipelines.partial_encounters.transforms.sort_by_time import SortByTime


def group_by(key, elements):
    groups = defaultdict(list)
    for element in elements:
        k, v = key(element)
        groups[k].append(v)

    return groups.items()


def resample(records, **kwargs):
    """Resample: GroupByIdAndDate, SortByTime, then resample each group."""
    transform = Resample(**kwargs)
    groups = group_by(GroupByIdAndDate().tag_with_id, records)
    return [
        resampled
        for group in groups
        for resampled in transform.resample(SortByTime().sort_and_uniquify_by_time(group))
    ]


def adjacency(resampled, **kwargs):
    """ComputeAdjacency: group by timestamp, then annotate each group."""
    transform = ComputeAdjacency(**kwargs)
    groups = group_by(transform.tag_with_time, resampled)
    return [annotated for group in groups for annotated in transform.annotate_adjacency(group)]


def encounters(annotated, **kwargs):
    """ComputeEncounters: group by id and date, sort by time, then compute each group's."""
    transform = ComputeEncounters(**kwargs)
    groups = group_by(transform.tag_with_id, annotated)
    return [
        encounter
        for group in groups
        for encounter in transform.compute_encounters(transform.sort_by_time(group))
    ]
