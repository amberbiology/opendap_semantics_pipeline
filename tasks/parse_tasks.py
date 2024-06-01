import luigi
from semproc.rawresponse import RawResponse
from semproc.parser import Parser
from semproc.identifier import Identify
from semproc.process_router import Router
from semproc.utils import generate_sha, convert_header_list
from semproc.serializers.rdfgraphs import RdfGrapher
import json
import os
import urllib.parse
from tasks.task_helpers import parse_yaml, extract_task_config
from tasks.task_helpers import read_data, generate_output_filename


class ResponseTask(luigi.Task):
    '''
    task to pull the handful of elements from the solr
    response and deal with the encoding/storage issues
    in the xml
    '''
    yaml_file = luigi.Parameter()
    input_file = luigi.Parameter()

    output_path = ''

    def requires(self):
        return []

    def output(self):
        return luigi.LocalTarget(
            generate_output_filename(
                self.input_file,
                self.output_path,
                'cleaned'
            )
        )

    def run(self):
        '''  '''
        self._configure()

        data = read_data(self.input_file)
        self.cleaned = self.process_response(data)
        with self.output().open('w') as out_file:
            out_file.write(json.dumps(self.cleaned, indent=4))

    def _configure(self):
        config = parse_yaml(self.yaml_file)
        config = extract_task_config(config, 'Clean')
        self.output_path = config.get('output_directory', '')

    def process_response(self, data):
        # do the response processing
        source_url = data['url']
        content = data['raw_content']
        # TODO: this is a holdover from prev solr instance
        url_sha = data.get('url_hash', generate_sha(source_url))
        headers = convert_header_list(data.get('response_headers', []))
        content_type = headers.get('content-type', '')

        rr = RawResponse(content, content_type)
        cleaned_text = rr.clean_raw_content()
        datatype = rr.datatype

        # check for the host and add it if missing
        host = data.get('host', '')
        if not host:
            parts = urllib.parse.urlparse(source_url)
            # host = urlparse.urlunparse((
            #     parts.scheme,
            #     parts.netloc,
            #     None, None, None, None
            # ))
            host = parts.netloc

        data.update({
            "content": cleaned_text,
            "sha": url_sha,
            "response_datatype": datatype,
            "host": host
        })
        return data


class ExtractXmlTask(luigi.Task):
    '''
    task to dump just the xml out of the response
    '''
    yaml_file = luigi.Parameter()
    input_file = luigi.Parameter()

    output_path = ''

    def requires(self):
        return ResponseTask(
            input_file=self.input_file, yaml_file=self.yaml_file)

    def output(self):
        return luigi.LocalTarget(
            generate_output_filename(
                self.input_file,
                self.output_path,
                'extracted',
                '.xml'
            )
        )

    def run(self):
        '''  '''
        self._configure()

        data = read_data(self.input_file)
        self.xml = self.process_response(data)
        with self.output().open('w') as out_file:
            out_file.write(self.xml)

    def _configure(self):
        config = parse_yaml(self.yaml_file)
        config = extract_task_config(config, 'ExtractXml')
        self.output_path = config.get('output_directory', '')

    def process_response(self, data):
        # do the response processing
        content = data['content'].encode('unicode_escape')
        parser = Parser(content)
        return parser.to_string()


class IdentifyTask(luigi.Task):
    yaml_file = luigi.Parameter()
    input_file = luigi.Parameter()

    output_path = ''
    identifiers = []

    def requires(self):
        return ResponseTask(
            input_file=self.input_file, yaml_file=self.yaml_file)

    def output(self):
        return luigi.LocalTarget(
            generate_output_filename(
                self.input_file,
                self.output_path,
                'identified'
            )
        )

    def run(self):
        '''  '''
        self._configure()

        f = self.input().open('r')
        data = json.loads(f.read())

        identified = self.process_response(data)

        if identified or self.params.get('save_unknown', False):
            with self.output().open('w') as out_file:
                out_file.write(json.dumps(identified, indent=4))

    def _locate_in_configs(self, filename):
        return os.path.join(
            os.path.abspath(os.path.dirname(__file__)),
            'configs',
            filename
        )

    def _configure(self):
        config = parse_yaml(self.yaml_file)
        config = extract_task_config(config, 'Identify')
        self.output_path = config.get('output_directory', '')
        self.params = config.get('params', {})

        if 'identifiers' in self.params:
            if isinstance(self.params['identifiers'], list):
                identifiers = self.params.get('identifiers', [])
                self.identifiers = [
                    self._locate_in_configs(i) for i in identifiers]
            else:
                # self.identifiers = glob.glob(config['identifiers'])
                raise Exception('identifier file list not found')

    def process_response(self, data):
        content = data['content'].encode('unicode_escape').decode()
        url = data['url']

        identify = Identify(
            self.identifiers,
            content,
            url
        )
        data['identity'] = identify.identify()
        return data


class ParseTask(luigi.Task):
    yaml_file = luigi.Parameter()
    input_file = luigi.Parameter()

    output_path = ''
    params = {}

    def requires(self):
        return IdentifyTask(
            input_file=self.input_file, yaml_file=self.yaml_file)

    def output(self):
        return luigi.LocalTarget(
            generate_output_filename(
                self.input_file,
                self.output_path,
                'parsed'
            )
        )

    def run(self):
        '''  '''
        self._configure()

        f = self.input().open('r')
        data = json.loads(f.read())
        parsed = self.process_response(data)
        if parsed:
            with self.output().open('w') as out_file:
                out_file.write(json.dumps(parsed, indent=4))

    def _configure(self):
        config = parse_yaml(self.yaml_file)
        config = extract_task_config(config, 'Parse')
        self.output_path = config.get('output_directory', '')
        self.params = config.get('params', {})

    def process_response(self, data):
        content = data['content'].encode('unicode_escape')
        url = data['url']
        identity = data['identity']

        to_exclude = self.params.get('exclude', [])
        protocol = next(iter(identity), {}).get('protocol')
        if not protocol or protocol in to_exclude:
            return {}

        harvest_details = {
            "harvest_date": data['tstamp']
        }

        processor = Router(
            identity,
            content,
            url,
            harvest_details,
            **{"parse_as_xml": self.params.get('parse_as_xml', False)}
        )
        if not processor:
            return {}

        description = processor.reader.description

        # drop the source for a decent non-xml embedded in my json file
        del data['content']

        data["service_description"] = description
        return data


class TripleTask(luigi.Task):
    yaml_file = luigi.Parameter()
    input_file = luigi.Parameter()

    output_path = ''
    params = {}

    def requires(self):
        return ParseTask(
            input_file=self.input_file, yaml_file=self.yaml_file)

    def output(self):
        return luigi.LocalTarget(
            generate_output_filename(
                self.input_file,
                self.output_path,
                'triples',
                '.ttl'
            )
        )

    def run(self):
        '''  '''
        self._configure()

        f = self.input().open('r')
        data = json.loads(f.read())
        triples = self.process_response(data)

        if triples:
            with self.output().open('w') as out_file:
                out_file.write(triples)

    def _configure(self):
        config = parse_yaml(self.yaml_file)
        config = extract_task_config(config, 'Triple')
        self.output_path = config.get('output_directory', '')
        self.params = config.get('params', {})

    def process_response(self, data):
        description = data.get('service_description', {})
        if not description:
            return ''
        graph = RdfGrapher(description)
        graph.serialize()
        return graph.emit_format()
