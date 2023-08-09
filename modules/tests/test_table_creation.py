#!/bin/env python

import pytest
import sys

from rich import print as rprint
from sqlalchemy.exc import SQLAlchemyError, NoSuchTableError
from sqlalchemy import create_engine, text

sys.path.append('/home/ben/Envs/san_juan_data/table_differ/modules')

from modules import create_diff_table
from create_diff_table import Tables
from create_diff_table import QueryPieces

class TestQueryPieces:
    def __init__(self):
        test_connection = "sqlite:///databases/Sqlite_test.db"
        engine = create_engine(test_connection, echo=False, future=True)
        conn = engine.connect()

    def test_select_args_sqlite(self):
        assert _select_args_sqlite == 'asad'
