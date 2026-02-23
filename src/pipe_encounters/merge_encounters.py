import sys

from pipe_encounters import merge_pipeline
from pipe_encounters.options.logging_options import LoggingOptions
from pipe_encounters.options.merge_options import MergeOptions
from pipe_encounters.options.validate_options import validate_options


def run(args):
    options = validate_options(args=args, option_classes=[LoggingOptions, MergeOptions])

    options.view_as(LoggingOptions).configure_logging()

    return merge_pipeline.run(options)


def main(args):
    sys.exit(run(args))


if __name__ == "__main__":
    main(sys.argv)
