from __future__ import absolute_import
from apache_beam.options.pipeline_options import PipelineOptions

class CreateOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        # Use add_value_provider_argument for arguments to be templatable
        # Use add_argument as usual for non-templatable arguments

        required = parser.add_argument_group('Required')
        optional = parser.add_argument_group('Optional')

        required.add_argument('--source_table', required=True, action='append', dest='source_tables',
                            help='Table to pull messages from. May be prefixed by "{ID}::" to prefix '
                                 'the seg_id with {ID}. This is useful for applying to multiple sources '
                                 '(e.g., AIS/VMS) while ensuring the seg_ids stay distinct.')
        required.add_argument('--raw_table', required=True,
                            help='Table to write raw (unmerged) encounters to')
        required.add_argument('--start_date', required=True,
                            help="First date to look for entry/exit events.")
        required.add_argument('--end_date', required=True,
                            help="Last date (inclusive) to look for entry/exit events.")
        required.add_argument('--max_encounter_dist_km', required=True, type=float,
                            help="Maximum distance for vessels to be elegible for an encounters")
        required.add_argument('--min_encounter_time_minutes', required=True, type=float,
                            help="Minimum minutes of vessel adjacency before we have an encounter")

        optional.add_argument('--raw_sink_write_disposition', default='WRITE_APPEND',
                            help='How to merge the output of this process with whatever records are already there'
                                 ' in the sink tables. Might be WRITE_TRUNCATE to remove all existing data and write'
                                 ' the new data, or WRITE_APPEND to add the new date without. Defaults to WRITE_APPEND.')
        optional.add_argument('--wait_for_job', action='store_true',
                            help='Wait for Dataflow to complete.')
        optional.add_argument('--ssvid_filter',
                            help='Subquery or list of ssvid to limit processing to.\n'
                                 'If prefixed by @, load from given path')

