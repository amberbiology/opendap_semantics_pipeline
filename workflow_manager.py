import luigi
import glob
import os
import sys
from optparse import OptionParser
from parse_workflow import ParseWorkflow
from identify_workflow import IdentifyWorkflow
# from bagofwords_workflow import BowWorkflow, BowFromXmlWorkflow
from triple_workflow import TripleWorkflow
from triple_workflow import SimpleParliamentWorkflow
# from extract_identifier_workflow import ExtractIdentifierWorkflow


def main():
    op = OptionParser()
    op.add_option('--interval', '-i', default=1000)
    op.add_option('--directory', '-d')
    op.add_option('--config', '-c')
    op.add_option('--start', '-s')
    op.add_option('--end', '-e')
    op.add_option('--workflow', '-w')
    op.add_option('--filetype', '-f', default='json')

    options, arguments = op.parse_args()

    if not options.config:
        op.error('No configuration YAML')
    if not options.directory:
        op.error('No input file directory')

    if not options.workflow:
        op.error('No workflow specified')

    files = glob.glob(
        os.path.join(
            options.directory, '*.{0}'.format(options.filetype)
        )
    )
    if not files:
        op.error(
            'Empty input file directory (no {0})'.format(options.filetype)
        )

    try:
        interval = int(options.interval)
    except:
        op.error('Non-integer interval value')

    try:
        start_index = int(options.start)
    except:
        start_index = 0
    try:
        end_index = int(options.end)
    except:
        end_index = len(files)

    # this only works for the workflows imported above (and they
    # need to be imported, obv). things like the eda, which aren't
    # working on a single file dependency tree, need something else
    try:
        workflow_class = getattr(sys.modules[__name__], options.workflow)
    except AttributeError:
        op.error('Unable to load workflow for {0}'.format(options.workflow))

    for i in range(start_index, end_index, interval):
        w = workflow_class(
            doc_dir=options.directory,
            yaml_file=options.config,
            start_index=i,
            end_index=(i + interval) if (i + interval) < end_index else end_index
        )
        luigi.build([w], local_scheduler=True)

if __name__ == '__main__':
    '''
    a little cli for running the workflows and
    to manage the standardization across those for
    bulk processing from files
    '''
    main()
