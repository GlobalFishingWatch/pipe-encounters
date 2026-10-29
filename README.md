<h1 align="center" style="border-bottom: none;"> pipe-encounters </h1>

<p align="center">
  <a href="https://codecov.io/gh/GlobalFishingWatch/pipe-encounters">
    <img alt="Coverage" src="https://codecov.io/gh/GlobalFishingWatch/pipe-encounters/graph/badge.svg?token=XS7HTOWWYG">
  </a>
  <a>
    <img alt="Python versions" src="https://img.shields.io/badge/python-3.12-blue">
  </a>
  <a>
    <img alt="Apache Beam version" src="https://img.shields.io/badge/ApacheBeam-2.76.0-orange">
  </a>
  <a>
    <img alt="Last release" src="https://img.shields.io/github/v/release/GlobalFishingWatch/pipe-encounters">
  </a>
</p>

[contributing guide]: https://github.com/GlobalFishingWatch/python-app-template/blob/main/CONTRIBUTING.md
[preparing the development environment]: https://github.com/GlobalFishingWatch/python-app-template/blob/main/CONTRIBUTING.md#preparing-the-development-environment
[git workflow]: https://github.com/GlobalFishingWatch/python-app-template/blob/main/CONTRIBUTING.md#git-workflow
[python-app-template]: https://github.com/GlobalFishingWatch/python-app-template
[Makefile]: Makefile

This repository contains the encounters pipeline, which finds vessel encounters
based on AIS messages.

# Development

First, clone the repository:

```shell
git clone https://github.com/GlobalFishingWatch/pipe-encounters.git
cd pipe-encounters
```

Then, from the cloned repository, follow the [contributing guide]
shared by the repositories created from [python-app-template]:
[preparing the development environment] (Docker setup, installing dependencies, pre-commit hooks)
and the [git workflow].

The [Makefile] exposes the usual targets:
`make docker-build`, `make docker-gcp`, `make docker-shell`, `make test`, `make format`, `make lint`,
among others -- run `make help` for the full list.
No repository-specific system dependency is required.

# Running

## Overview

The pipeline takes `start_date` and `end_date`. The pipeline pads `start_date`
by one day to warm up, reads the data from from `source_table` and computes
encounters over the specified window.
In incremental mode, `end_date` is the day after `start_date`, since the end date is
exclusive. The partial encounters are *appended* to the `bq_out_partial_encounters` table. The second command,
`encounters`, then joins the pieces that are close in time into one encounter per pair of
vessels, *replacing* the `bq_out_encounters` table with the results.

## CLI

The pipeline includes a CLI that can be used to start both local test runs and
remote full runs. Just run `docker compose run pipeline --help` and follow the
instructions there.

### Examples:

The first step, `partial-encounters`, detects the pieces of encounters for a range of dates:

        docker compose run pipeline partial-encounters \
                --bq-in-positions SOURCE_TABLE \
                --bq-out-partial-encounters RAW_TABLE \
                --start-date DATE \
                --end-date DATE \
                --max-encounter-dist-km DISTANCE \
                --min-encounter-time-minutes TIME \
                --labels environment=development resource_creator=YOUR_NAME \
                --project world-fishing-827 \
                --runner DataflowRunner \
                --region us-central1

or, with a configuration file (command-line arguments take precedence over it):

        docker compose run pipeline partial-encounters -c config/partial-encounters/bq-1-day.yaml

Run `docker compose run pipeline partial-encounters --help` for all its options.

The second step, `encounters`, joins the partial encounters into one row per encounter between
two vessels. It replaces its output table, so it's usually run over the whole history:

        docker compose run pipeline encounters \
                --bq-in-partial-encounters RAW_TABLE \
                --bq-in-segment-info SEGMENT_INFO_TABLE \
                --bq-in-spatial-measures SPATIAL_MEASURES_TABLE \
                --bq-out-encounters ENCOUNTERS_TABLE \
                --start-date FIRST_DATE \
                --end-date DATE \
                --labels environment=development resource_creator=YOUR_NAME \
                --project world-fishing-827 \
                --runner DataflowRunner \
                --region us-central1

or, with a configuration file:

        docker compose run pipeline encounters -c config/encounters/bq-full-history.yaml

Run `docker compose run pipeline encounters --help` for all its options.

Partial encounters are identified by segment id, since it's a stable (static) identifier.
The encounters are joined by vessel id, which does a better job stitching segments together,
but isn't stable. That works because joining happens later in the pipeline and runs over the
whole history every day.

Note that the partial encounters table needs to be persistent, since each run appends new dates.


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
