"""Applies the encounters pipeline's transforms in plain Python, without Beam.

Like tests/pipelines/partial_encounters/transforms/steps.py: the same keys to group by as the
transform's expand(), then the transform's own methods.
"""

from collections import defaultdict

from pipe_encounters.pipelines.encounters.transforms.merge_encounters import MergeEncounters


def merge(raw_encounters, min_hours_between_encounters=24):
    """MergeEncounters: group by ordered vessel id pair, then merge each group."""
    transform = MergeEncounters(min_hours_between_encounters=min_hours_between_encounters)
    groups = defaultdict(list)
    for raw in raw_encounters:
        key, value = transform.key_by_ordered_id(raw)
        groups[key].append(value)

    return [merged for group in groups.items() for merged in transform.merge_encounters(group)]
