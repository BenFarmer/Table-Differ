#!/usr/bin/env python
""" Run from the source code root directory
"""

import os.path
from os.path import dirname, join as pjoin
from pprint import pprint as pp

#import psycopg2

from modules import get_config
from modules.create_diff_table import DiffWriter

EXAMPLE_DIR = pjoin(dirname(dirname(os.path.realpath(__file__))), 'examples')
SCRIPT_DIR = dirname(dirname(os.path.realpath((__file__))))
HOST = 'labrat'
DB = 'etl'
USER = 'kenfar'
PORT = '5432'


class ExampleTools(object):
    """ Test all configs and files in the example directory for this program
    """

    def setup_method(self, method):
        self.cmd = None
        self.db_conn = psycopg2.connect(host=HOST, database=DB, user=USER, port=PORT)


    def _build_model(self,
                     example_number,
                     model_alias):

        create_fqfn = pjoin(EXAMPLE_DIR, f'ex_{example_number}_create_{model_alias}.sql')
        print(create_fqfn)
        with open(create_fqfn, 'r') as inbuf:
            query = inbuf.read()
        print(query)
        cur = self.db_conn.cursor()
        cur.execute(query)
        self.db_conn.commit()

        load_fqfn = pjoin(EXAMPLE_DIR, f'ex_{example_number}_load_{model_alias}.sql')
        with open(load_fqfn, 'r') as inbuf:
            query = inbuf.read()
        print(query)
        cur = self.db_conn.cursor()
        cur.execute(query)
        self.db_conn.commit()


    def run_example_config(self,
                           example_number):

        self._build_model(example_number, 'initial')
        self._build_model(example_number, 'secondary')
        self._build_model(example_number, 'expected_diff')
        cli_args = ['--config-file', f'examples/ex_{example_number}_config.yaml']
        args = get_config.get_config(cli_args)
        tables = DiffWriter(args, self.db_conn)
        tables.create_diff_table()


    def assert_tables_identical(self,
                                example_number):

        def get_query(table_name):
            query = f'''SELECT origin_cust_id, comparison_cust_id,
                               origin_cust_name, comparison_cust_name,
                               origin_status, comparison_status,
                               origin_contract_start_date, comparison_contract_start_date,
                               origin_contract_end_date, comparison_contract_end_date,
                               origin_contract_credits, comparison_contract_credits,
                               origin_contract_balance, comparison_contract_balance,
                               origin_latest_billing_period, comparison_latest_billing_period
                    FROM tabdiff.{table_name}
                    ORDER BY origin_cust_id, comparison_cust_id
                    '''
            return query

        query = get_query('ex_1_expected_diff')
        cur = self.db_conn.cursor()
        cur.execute(query)
        expected_rows = cur.fetchall()

        query = get_query('ex_1_secondary__diff__')
        cur = self.db_conn.cursor()
        cur.execute(query)
        actual_rows = cur.fetchall()

        assert len(expected_rows) == len(actual_rows)
        # Going to assert each row individually to make it easier to see differences:
        for i in range(len(expected_rows)):
            try:
                assert expected_rows[i] == actual_rows[i]
            except:
                print('Expected row that failed: ')
                pp(expected_rows[i])
                print('Actual row that failed: ')
                pp(actual_rows[i])
                raise



class TestExamples(ExampleTools):
    """ Test all configs and files in the example directory for this program
    """
    def test_example_01(self):
        self.run_example_config('1')
        self.assert_tables_identical('1')

