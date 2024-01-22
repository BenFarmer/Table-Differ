#! usr/bin/env python

import sqlite3
import os.path
import pytest
from os.path import dirname, join as pjoin

from pprint import pprint as pp

from modules import get_config
from modules.create_diff_table import DiffWriter

# DB_PATH = 'sqlite:///databases/table_diff_test.db'
DB_PATH = '/home/ben/Envs/san_juan_data/db_tools/table_differ_dev/tests/SqliteTest.db'
EXAMPLE_DIR = pjoin(dirname(dirname(os.path.realpath(__file__))), 'examples_sqlite')


class ExampleTools:
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    def _build_model(self, example_number, model_alias):
        def drop_old_example(table_name):
            drop_query = f"DROP TABLE IF EXISTS {table_name}"
            print(drop_query)
            self.cursor.execute(drop_query)

        table_name = f'ex_{example_number}_{model_alias}'
        drop_old_example(table_name)

        create_fqfn = pjoin(EXAMPLE_DIR, f'ex_{example_number}_create_{model_alias}.sql')
        print(create_fqfn)
        with open(create_fqfn, 'r') as inbuf:
            query = inbuf.read()
        print(query)

        self.cursor.execute(query)
        self.conn.commit()

        load_fqfn = pjoin(EXAMPLE_DIR, f'ex_{example_number}_load_{model_alias}.sql')
        print(load_fqfn)
        with open(load_fqfn, 'r') as inbuf:
            query = inbuf.read()
        print(query)

        self.cursor.execute(query)
        self.conn.commit()


    def run_example_config(self, example_number):
        self._build_model(example_number, 'initial')
        self._build_model(example_number, 'secondary')
        self._build_model(example_number, 'expected_diff')

        cli_args = ['--config-file', f'examples/ex_{example_number}_config.yaml']
        args = get_config.get_config(cli_args)
        tables = DiffWriter(args, self.conn)
        tables.create_diff_table()


    def assert_tables_identical(self, example_number):
        def get_query(table_name):
            query = f"""
                SELECT initial_cust_id, secondary_cust_id,
                        initial_cust_name, secondary_cust_name,
                        initial_status, secondary_status,
                        initial_contract_start_date, secondary_contract_start_date,
                        initial_contract_end_date, secondary_contract_end_date,
                        initial_contract_credits, secondary_contract_credits,
                        initial_contract_balance, secondary_contract_balance,
                        initial_latest_billing_period, secondary_latest_billing_period
                FROM {table_name}
                ORDER BY initial_cust_id, secondary_cust_id
                """
            return query

        query = get_query('ex_1_expected_diff')
        self.cursor.execute(query)
        actual_rows = self.cursor.fetchall()

        query = get_query('ex_1_secondary_diff')
        self.cursor.execute(query)
        actual_rows = self.cursor.fetchall()

        assert len(expected_rows) == len(actual_rows)
        print(len(expected_rows), '==', len(actual_rows))
        for i in range(len(expected_rows)):
            try:
                assert expected_rows[i] == actual_rows[i]
            except:
                print('expected row that failed: ')
                pp(expected_rows[i])
                print('actual row that failed: ')
                pp(actual_rows[i])
                raise


class TestExamples(ExampleTools):
    """ Calls the tests for each specific example
    """
    def test_example_1(self):
        self.run_example_config('1')
        self.assert_tables_identical('1')
    """ Each example/test case needs 6 components
            - ex_n_config.yaml
            - ex_n_create_initial.sql
            - ex_n_create_secondary.sql
            - ex_n_create_expected_diff.sql
            - ex_n_load_initial.sql
            - ex_n_load_secondary.sql
            - ex_n_load_expected_diff.sql

    """
