import luigi
import glob
import os
from tasks.parse_tasks import TripleTask
from tasks.parliament_tasks import ParliamentTask
from tasks.parliament_tasks import SimpleParliamentTask
from tasks.task_helpers import parse_yaml
from tasks.task_helpers import run_init


class TripleWorkflow(luigi.Task):
    doc_dir = luigi.Parameter()
    yaml_file = luigi.Parameter()

    start_index = luigi.Parameter(default=0)
    end_index = luigi.Parameter(default=1000)

    def requires(self):
        return [
            TripleTask(
                input_file=f,
                yaml_file=self.yaml_file
            ) for f in self._iterator()
        ]

    def output(self):
        return luigi.LocalTarget('log.txt')

    def run(self):
        self._configure()
        print ('running')

    def _configure(self):
        config = parse_yaml(self.yaml_file)
        run_init(config)

    def _iterator(self):
        for f in glob.glob(
                os.path.join(self.doc_dir, '*.json')
        )[self.start_index:self.end_index]:
            yield f


# TODO: there has to be a better way than the duplication
#       as i recall a build order thing in luigi?
class ParliamentWorkflow(luigi.Task):
    doc_dir = luigi.Parameter()
    yaml_file = luigi.Parameter()

    start_index = luigi.Parameter(default=0)
    end_index = luigi.Parameter(default=1000)

    def requires(self):
        return [
            ParliamentTask(
                input_file=f,
                yaml_file=self.yaml_file
            ) for f in self._iterator()
        ]

    def output(self):
        return luigi.LocalTarget('log.txt')

    def run(self):
        self._configure()
        print ('running')

    def _configure(self):
        config = parse_yaml(self.yaml_file)
        run_init(config)

    def _iterator(self):
        for f in glob.glob(
            os.path.join(self.doc_dir, '*.json')
        )[self.start_index:self.end_index]:
            yield f


class SimpleParliamentWorkflow(luigi.Task):
    doc_dir = luigi.Parameter()
    yaml_file = luigi.Parameter()

    start_index = luigi.Parameter(default=0)
    end_index = luigi.Parameter(default=1000)

    def requires(self):
        return [
            SimpleParliamentTask(
                input_file=f,
                yaml_file=self.yaml_file
            ) for f in self._iterator()
        ]

    def output(self):
        return luigi.LocalTarget('log.txt')

    def run(self):
        self._configure()
        print ('running')

    def _configure(self):
        config = parse_yaml(self.yaml_file)
        run_init(config)

    def _iterator(self):
        for f in glob.glob(
            os.path.join(self.doc_dir, '*.ttl')
        )[self.start_index:self.end_index]:
            yield f
