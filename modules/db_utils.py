#! /bin/env/python3

class DBFacts:
    def __init__(self, conn):
        self.conn = conn

    def get_cols(self,
                schema_name: str,
                db_type: str,
                table_name: str) -> list[str]:
        if db_type == 'postgres' or db_type == 'mysql':
            cur = self.conn.cursor()
            query = f"""
                    SELECT column_name
                    FROM information_schema.columns
                    WHERE table_schema = '{schema_name}'
                        AND table_name = '{table_name}'
                    """
            cur.execute(query)
            cols = cur.fetchall()
        elif db_type == 'sqlite':
            query = f"""PRAGMA table_info({table_name})"""
            results = self.conn.execute(query)
            cols = []
            for result in results:
                cols.append(result[1])
        else:
            raise ValueError(f'db_type of {db_type} not supported')

        return cols

    def get_common_cols(table_a_cols: list[str],
                        table_b_col: list[str]) -> list[str]:
        return list(set(table_a_cols).intersection(set(table_b_cols)))
