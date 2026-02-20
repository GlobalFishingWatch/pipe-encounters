from .utils import SchemaBuilder

def build():

    builder = SchemaBuilder()
    builder.add("encounter_id", "STRING", description="Unique encounter ID.")
    builder.add("start_time", "TIMESTAMP", description="The start timestamp of the encounter (UTC).")
    builder.add("end_time", "TIMESTAMP", description="The end timestamp of the encounter (UTC).")
    builder.add("mean_latitude", "FLOAT", description="The mean latitude. Calculated in two stages. First these values are calculated for the raw segments when these values are computed as means over the interpolated points. Second take a weighted average of the averages (by duration of the raw encounters that are being merged) to come up with the average lat/lon for the merged encounter. No guarantee the mean lat/lon will occur along the track.")
    builder.add("mean_longitude", "FLOAT", description="The mean longitude. Calculated in two stages. First these values are calculated for the raw segments when these values are computed as means over the interpolated points. Second take a weighted average of the averages (by duration of the raw encounters that are being merged) to come up with the average lat/lon for the merged encounter. No guarantee the mean lat/lon will occur along the track.")
    builder.add("median_distance_km", "FLOAT", description="The median distance, measured in km.")
    builder.add("median_speed_knots", "FLOAT", description="The median speed, measured in knots.")
    builder.add("start_lat", "FLOAT", description="The beginning latitude where the encounter takes place.")
    builder.add("start_lon", "FLOAT", description="The beginning longitude where the encounter takes place.")
    builder.add("end_lat", "FLOAT", description="The end latitude where the encounter finished.")
    builder.add("end_lon", "FLOAT", description="The end longitude where the encounter finished.")
    for v in [1, 2]:
        builder.add(f"vessel_{v}_id", "STRING", description=f"The vessel id of the vessel {v}.")
        builder.add(f"vessel_{v}_seg_ids", "STRING", mode="REPEATED", description=f"The segment id of the vessel {v}.")
        builder.add(f"vessel_{v}_point_count", "INTEGER", description=f"The amount of the points of contact during encounter by vessel {v}.")

    return builder.schema


def build_raw_encounter():

    builder = SchemaBuilder()
    builder.add("encounter_id", "STRING", description="Unique encounter ID.")
    builder.add("start_time", "TIMESTAMP", description="The start timestamp of the encounter (UTC).")
    builder.add("end_time", "TIMESTAMP", description="The end timestamp of the encounter (UTC).")
    builder.add("mean_latitude", "FLOAT", description="The mean latitude. These values are calculated for the raw segments when these values are computed as means over the interpolated points.")
    builder.add("mean_longitude", "FLOAT", description="The mean longitude. These values are calculated for the raw segments when these values are computed as means over the interpolated points.")
    builder.add("median_distance_km", "FLOAT", description="The median distance, measured in km.")
    builder.add("median_speed_knots", "FLOAT", description="The median speed, measured in knots.")
    builder.add("start_lat", "FLOAT", description="The beginning latitude where the encounter takes place.")
    builder.add("start_lon", "FLOAT", description="The beginning longitude where the encounter takes place.")
    builder.add("end_lat", "FLOAT", description="The end latitude where the encounter finished.")
    builder.add("end_lon", "FLOAT", description="The end longitude where the encounter finished.")
    for v in [1, 2]:
        builder.add(f"vessel_{v}_seg_id", "STRING", description=f"The segment id of the vessel {v}.")
        builder.add(f"vessel_{v}_point_count", "INTEGER", description=f"The amount of the points of contact during encounter by vessel {v}.")

    return builder.schema
