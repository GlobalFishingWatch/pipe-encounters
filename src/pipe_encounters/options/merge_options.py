from __future__ import absolute_import
from apache_beam.options.pipeline_options import PipelineOptions

class MergeOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        # Use add_value_provider_argument for arguments to be templatable
        # Use add_argument as usual for non-templatable arguments

        required = parser.add_argument_group('Required')
        optional = parser.add_argument_group('Optional')

        required.add_argument('--raw_table', required=True,
                            help='Table to pull raw (unmerged) encounters to')
        required.add_argument('--vessel_id_table',  required=True, action='append', dest='vessel_id_tables',
                            help='Name of table mapping vessel_id to seg_id (BQ). '
                            'Should have one vessel_id per seg_id, e.g. the `segment_info` table.'
                            ' May be prefixed by "{ID}::" to prefix '
                            'the seg_id with {ID}. This is useful for applying to multiple sources '
                            '(e.g., AIS/VMS) while ensuring the seg_ids stay distinct.')
        required.add_argument('--sink_table', required=True,
                            help='Table to write file, merged and filtered encounters to')
        required.add_argument('--start_date', required=True,
                              help="First date to merge.")
        required.add_argument('--end_date', required=True,
                            help="Last date (inclusive) to merge.")
        required.add_argument('--spatial_measures_table', required=True,
                            help="Table to pull distance from shore and port from.")
        optional.add_argument('--wait_for_job', action='store_true',
                            help='Wait for Dataflow to complete.')
        optional.add_argument('--min_encounter_time_minutes', required=False, type=float,
                            help="Minimum minutes of vessel adjacency before we have an encounter.\n"
                                  "Does not have an effect if lower than value used in the create pipeline")
        optional.add_argument('--min_hours_between_encounters', required=False, type=float, default=4,
                            help="Minimum hours between two encounters before merging them")
        optional.add_argument('--bad_segs_table',
                            help='table of containing segment ids of bad segments')
        optional.add_argument('--ssvid_filter',
                            help='Subquery or list of ssvid to limit processing to.\n'
                                 'If prefixed by @, load from given path')

