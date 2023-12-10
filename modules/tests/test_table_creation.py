#!/bin/env python

from pprint import pprint as pp
import pytest
import sys

#from rich import print as rprint
from sqlalchemy.exc import SQLAlchemyError, NoSuchTableError
from sqlalchemy import create_engine, text

sys.path.append('/home/ben/Envs/san_juan_data/table_differ/modules')

import modules.create_diff_table as mod

class TestQueryClauses:

    def test_select_compare_cols(self):
        qc = mod.QueryClauses(table_cols=['col_1', 'col_2', 'col_3', 'col_4', 'col_5', 'col_6'],
                              key_cols=['col_1', 'col_2'],
                              compare_cols=['col_3', 'col_4'],
                              ignore_cols = [],
                              initial_table_alias='TABA',
                              secondary_table_alias='TABB')
        actual = qc.get_select()
        pp('---------- actual ----------')
        pp(actual)
        pp('---------- ------ ----------')
        expected = ('   a.col_1 TABA_col_1, \n'
                    '   b.col_1 TABB_col_1, \n'
                    '   a.col_2 TABA_col_2, \n'
                    '   b.col_2 TABB_col_2, \n'
                    '   a.col_3 TABA_col_3, \n'
                    '   b.col_3 TABB_col_3, \n'
                    '   a.col_4 TABA_col_4, \n'
                    '   b.col_4 TABB_col_4'
        assert actual == expected
                              
    def test_select_compare_cols(self):
        qc = mod.QueryClauses(table_cols=['col_1', 'col_2', 'col_3', 'col_4', 'col_5', 'col_6'],
                              key_cols=['col_1', 'col_2'],
                              compare_cols=[],
                              ignore_cols = ['col_5', 'col_6'],
                              initial_table_alias='TABA',
        actual = qc.get_select()
        pp('---------- actual ----------')
        pp(actual)
        pp('---------- ------ ----------')
        expected = ('   a.col_1 TABA_col_1, \n'
                    '   b.col_1 TABB_col_1, \n'
                    '   a.col_2 TABA_col_2, \n'
                    '   b.col_2 TABB_col_2, \n'
                    '   a.col_3 TABA_col_3, \n'
                    '   b.col_3 TABB_col_3, \n'
                    '   a.col_4 TABA_col_4, \n'
                    '   b.col_4 TABB_col_4'
        assert actual == expected
                              
    def test_join(self):
        qc = mod.QueryClauses(table_cols=['col_1', 'col_2', 'col_3', 'col_4', 'col_5', 'col_6'],
                              key_cols=['col_1', 'col_2'],
                              compare_cols=[],
                              ignore_cols = ['col_5', 'col_6'],
                              initial_table_alias='TABA',
                              secondary_table_alias='TABB')
        actual = qc.get_join()
        pp(actual)
        expected = (' a.col_1 = b.col_1 '
                    ' AND a.col_2 = b.col_2 ')
        assert actual == expected

