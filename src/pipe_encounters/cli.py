#! /usr/bin/env python
import logging
import sys


logging.basicConfig(level=logging.INFO)


def exec_create_raw_encounters(args):
    # set import here to avoid collition of same args in PipelineOption setup
    from pipe_encounters.create_raw_encounters import main as run_create_raw_encounters

    run_create_raw_encounters(args)


def exec_merge_encounters(args):
    # set import here to avoid collition of same args in PipelineOption setup
    from pipe_encounters.merge_encounters import main as run_merge_encounters

    run_merge_encounters(args)


SUBCOMMANDS = {
    "create_raw_encounters": exec_create_raw_encounters,
    "merge_encounters": exec_merge_encounters,
}


def main():
    logging.info("Running %s", sys.argv)

    if len(sys.argv) < 2:
        logging.info(
            "No subcommand specified. Run pipeline [SUBCOMMAND], where subcommand is one of %s",
            SUBCOMMANDS.keys(),
        )
        exit(1)

    subcommand = sys.argv[1]
    subcommand_args = sys.argv[2:]

    SUBCOMMANDS[subcommand](subcommand_args)


if __name__ == "__main__":
    main()
