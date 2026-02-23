<h1 align="center" style="border-bottom: none;"> pipe-encounters </h1>

<p align="center">
  <a href="https://codecov.io/gh/GlobalFishingWatch/pipe-encounters">
    <img alt="Coverage" src="https://codecov.io/gh/GlobalFishingWatch/pipe-enciounps/graph/badge.svg?token=XS7HTOWWYG">
  </a>
  <a>
    <img alt="Python versions" src="https://img.shields.io/badge/python-3.8-blue">
  </a>
  <a>
    <img alt="Apache Beam version" src="https://img.shields.io/badge/ApacheBeam-2.49.0-orange">
  </a>
  <a>
    <img alt="Last release" src="https://img.shields.io/github/v/release/GlobalFishingWatch/pipe-encounters">
  </a>
</p>

[git workflow documentation]: GITHUB-FLOW.md

This repository contains the encounters pipeline, which finds vessel encounters
based on AIS messages.

# Running

## Dependencies

You just need [docker](https://www.docker.com/) and
[docker-compose](https://docs.docker.com/compose/) in your machine to run the
pipeline. No other dependency is required.

## Git Workflow

Please refer to our [git workflow documentation] to know how to manage branches in this repository.

## Setup

The pipeline reads it's input from BigQuery, so you need to first authenticate
with your google cloud account inside the docker images. To do that, you need
to run this command and follow the instructions:


```
docker compose run --entrypoint gcloud pipe_enocunters auth application-default login
```

## Overview

The pipeline takes `start_date` and `end_date`. The pipeline pads `start_date`
by one day to warm up, reads the data from from `source_table` and computes
encounters over the specified window.
In incremental mode, `start_date` and `end_date` would be on the same date.  The results
of this encounter are *appended* to the specified `raw_sink` table. A second pipeline
is then run over this second table, merging encounters that are close in time into
one long encounter and *replacing* the table specified in `sink` with the merged results.

## CLI

The pipeline includes a CLI that can be used to start both local test runs and
remote full runs. Just run `docker compose run pipe_encounters --help` and follow the
instructions there.

### Examples:

In incremental mode, the form of the command is

        docker-compose run create_raw_encounters \
                --source_table SOURCE_TABLE \
                --start_date DATE \
                --end_date DATE \
                --max_encounter_dist_km DISTANCE \
                --min_encounter_time_minutes TIME \
                --raw_table RAW_TABLE \
                --project world-fishing-827 \
                --temp_location gs://world-fishing-827-dev-ttl30d/scratch/encounters \
                --job_name encounters-pip \
                --max_num_workers 200 \
                --setup_file ./setup.py \
                --requirements_file requirements.txt \
                --runner DataflowRunner \
                --disk_size_gb 100 \
                --region us-central1

The raw encounters are then merged together, removing duplicates and merging across day boundaries:


        docker-compose run merge_encounters \
                --raw_table RAW_TABLE \
                --vessel_id_table SEGMENT_TABLE \
                --sink_table MERGED_TABLE \
                --max_encounter_dist_km 0.5 \
                --min_encounter_time_minutes 120 \
                --start_date 2018-01-01 \
                --end_date 2018-12-31 \
                --project world-fishing-827 \
                --temp_location gs://world-fishing-827-dev-ttl30d/scratch/encounters \
                --job_name encounters-merge-test \
                --max_num_workers 50 \
                --setup_file ./setup.py \
                --requirements_file requirements.txt \
                --runner DataflowRunner \
                --disk_size_gb 100 \
                --region us-central1


Currently, raw encounters are created based on segment id, since this is a stable (static)
identifier. During the merge process, encounters are merged using vessel id, which does a better
job stitching together segments, but is not stable. This is feasible since the merging process
happens later in the pipeline and is run across all time on every day.

Note that raw_table needs to be persistent since it is date sharded and new dates
are added with each run.


        docker-compose run create_raw_encounters \
                --source_table pipe_production_v20201001.position_messages \
                --start_date 2018-01-01 \
                --end_date 2018-01-31 \
                --max_encounter_dist_km 0.5 \
                --min_encounter_time_minutes 60 \
                --raw_table world-fishing-827:machine_learning_dev_ttl_120d.raw_encounters_test_ \
                --project world-fishing-827 \
                --temp_location gs://world-fishing-827-dev-ttl30d/scratch/encounters \
                --job_name encounters-pip \
                --max_num_workers 100 \
                --setup_file ./setup.py \
                --requirements_file requirements.txt \
                --runner DataflowRunner \
                --disk_size_gb 100 \
                --region us-central1


        docker-compose run merge_encounters \
                --raw_table machine_learning_dev_ttl_120d.raw_encounters_test_ \
                --vessel_id_table pipe_production_v20201001.segment_info \
                --sink_table world-fishing-827:machine_learning_dev_ttl_120d.encounters_test_v20210718 \
                --spatial_measures_table world-fishing-827.pipe_static.spatial_measures_20200311 \
                --min_encounter_time_minutes 120 \
                --start_date 2018-01-01 \
                --end_date 2018-01-31 \
                --project world-fishing-827 \
                --temp_location gs://world-fishing-827-dev-ttl30d/scratch/encounters \
                --job_name encounters-merge-test \
                --max_num_workers 50 \
                --setup_file ./setup.py \
                --requirements_file requirements.txt \
                --runner DataflowRunner \
                --disk_size_gb 100 \
                --region us-central1



# License

Copyright 2017 Global Fishing Watch

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
