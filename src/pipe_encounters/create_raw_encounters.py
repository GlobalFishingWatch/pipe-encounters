import sys

from pipe_encounters import create_raw_pipeline
from pipe_encounters.options.create_options import CreateOptions
from pipe_encounters.options.logging_options import LoggingOptions
from pipe_encounters.options.validate_options import validate_options


def run(args):
    options = validate_options(args=args, option_classes=[LoggingOptions, CreateOptions])

    options.view_as(LoggingOptions).configure_logging()

    return create_raw_pipeline.run(options)


def main(args):
    sys.exit(run(args))


if __name__ == "__main__":
    main(sys.argv)
