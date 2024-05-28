import json
from optparse import OptionParser
import os
import requests


class SolrBulkJob():
    '''
    not a proper task, just a way to pull, from a
    prebuilt query, some chunk of docs from solr
    that will then be sent to the initializing
    iterator instead of a glob. a remote glob.
    '''
    def __init__(self,
                 solr_connection,
                 solr_query,
                 output_path,
                 start=0,
                 end=1000,
                 interval=100):
        # deal with the solr connection
        self.connection = self._unpack_connection_string(solr_connection)
        self.solr_auth = (
            self.connection.get('User ID'),
            self.connection.get('Password')
        )
        self.solr_query = solr_query
        self.start = start
        self.end = end
        self.interval = interval
        self.output_path = output_path

    def _unpack_connection_string(self, conn_str):
        ''' standard conn string (from postgres)

            structure:
                Provider=[]; User ID=[]; Password=[]; Host=[]; Port=[];
                Database=[]; Collection=[]
            ex:
                Provider=https; User ID=user; Password=password;
                Host=localhost; Port=8080; Database=solr;
                Collection=collection1
        '''
        return dict([tuple(i.strip().split('=')) for i in conn_str.split(';')])

    def _generate_query(self, limit, offset):
        # from the query without pagination,
        # add the pagination. this is to handle
        # multiple requests where i don't want
        # to unpack the query to understand
        return '?' + self.solr_query + \
            '&rows={0}&start={1}'.format(limit, offset)

    def _generate_url(self, query):
        host = '{0}://{1}'.format(
            self.connection.get('Provider'),
            self.connection.get('Host')
        )

        if self.connection.get('Port'):
            host += ':' + self.connection.get('Port')

        return '/'.join([
            host,
            self.connection.get('Database'),
            self.connection.get('Collection'),
            'select',
            query
        ])

    def _query(self, query):
        url = self._generate_url(query)
        req = requests.get(url, auth=self.solr_auth)

        print ('Querying: {0}'.format(url))

        if req.status_code != 200:
            print ( '\tFailed request ({0}): {1}'.format(req.status_code, url))

        return req.json()

    def _parse_contents(self, contents):
        # run through the docs array
        output_pattern = os.path.join(self.output_path, '%s.json')

        for doc in contents.get('response', {}).get('docs', []):
            response_sha = doc.get('url_hash')
            with open(output_pattern % response_sha, 'w') as f:
                    f.write(json.dumps(doc, indent=4))

    def run(self):
        for offset in range(self.start, self.end, self.interval):
            query = self._generate_query(self.interval, offset)
            contents = self._query(query)
            self._parse_contents(contents)


def main():
    op = OptionParser()
    # go get a yaml config file with
    # the query, start, end, offset
    # and output directory for the docs
    op.add_option('--connection', '-c')
    op.add_option('--query', '-q', default='q=*%3A*&wt=json&indent=true')
    op.add_option('--directory', '-d')
    op.add_option('--interval', '-i', default=1000)
    op.add_option('--start', '-s', default=0)
    op.add_option('--end', '-e', default=10)

    options, arguments = op.parse_args()

    if not options.connection:
        op.error('No SOLR Connection provided')

    if not options.directory:
        op.error('No output directory provided')

    if not options.connection:
        op.error('No SOLR Connection provided')

    try:
        interval = int(options.interval)
        start = int(options.start)
        end = int(options.end)
    except Exception as ex:
        op.error('Invalid pagination integer: {0}'.format(ex))

    solr = SolrBulkJob(
        options.connection,
        options.query,
        options.directory,
        start,
        end,
        interval
    )
    solr.run()


if __name__ == '__main__':
    '''
    execute a simple solr pull
    '''
    main()
