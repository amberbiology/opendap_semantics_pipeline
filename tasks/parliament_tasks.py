import luigi
from tasks.parse_tasks import TripleTask
from tasks.task_helpers import parse_yaml, extract_task_config
from tasks.task_helpers import generate_output_filename
from tasks.task_helpers import read_data
from rdflib.plugins.stores import sparqlstore
from rdflib import Graph, URIRef


def connect(endpoint, named_graph):
    # set up the parliament named graph endpoint
    # TODO: handle auth to the thing
    store = sparqlstore.SPARQLUpdateStore()
    store.open((endpoint, endpoint))
    return Graph(store, URIRef(named_graph))


def insert(conn, filename):
    # load the local grpah
    graph = Graph()
    graph.parse(filename, format="turtle")
    as_nt = graph.serialize(format="nt")
    conn.update("INSERT DATA { %s }" % as_nt)


class SimpleParliamentTask(luigi.Task):
    # to run on previously generated triples
    # and not from the full pipeline flow
    # effectively, no previous task required,
    # just point it to the triples directory
    yaml_file = luigi.Parameter()
    input_file = luigi.Parameter()

    output_path = ''
    params = {}

    def requires(self):
        return []

    def output(self):
        return luigi.LocalTarget(
            generate_output_filename(
                self.input_file,
                self.output_path,
                'post',
                '.txt'
            )
        )

    def run(self):
        '''  '''
        self._configure()

        # f = self.input().path
        # data = read_data(self.input_file, False)
        self.process_response(self.input_file)

        # add a file as a flag basically
        with self.output().open('w') as out_file:
            out_file.write('posted')

    def _configure(self):
        config = parse_yaml(self.yaml_file)
        config = extract_task_config(config, 'SimpleParliament')
        self.output_path = config.get('output_directory', '')
        self.params = config.get('params', {})

    def process_response(self, data):
        # data in this instance is the filename to the turle
        conn = connect(
            self.params.get('endpoint'), self.params.get('named_graph'))
        insert(conn, data)


class ParliamentTask(luigi.Task):
    # the linked post to parliament
    yaml_file = luigi.Parameter()
    input_file = luigi.Parameter()

    output_path = ''
    params = {}

    def requires(self):
        return TripleTask(
            input_file=self.input_file, yaml_file=self.yaml_file)

    def output(self):
        return luigi.LocalTarget(
            generate_output_filename(
                self.input_file,
                self.output_path,
                'post',
                '.txt'
            )
        )

    def run(self):
        '''  '''
        self._configure()

        f = self.input().path
        self.process_response(f)

        # add a file as a flag basically
        with self.output().open('w') as out_file:
            out_file.write('posted')

    def _configure(self):
        config = parse_yaml(self.yaml_file)
        config = extract_task_config(config, 'Parliament')
        self.output_path = config.get('output_directory', '')
        self.params = config.get('params', {})

    def process_response(self, data):
        # data in this instance is the filename to the turle
        conn = connect(
            self.params.get('endpoint'), self.params.get('named_graph'))
        insert(conn, data)
