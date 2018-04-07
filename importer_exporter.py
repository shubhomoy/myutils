#!/usr/bin/python3.5

import argparse
import logging
import os

import pandas as pd
import psycopg2
from collections import OrderedDict

from util import run_query


CHUNK_SIZE = 500000


class ImportExporter(object):
    def __init__(self, file_path, schema, table, action, database='datascience'):
        self.action = action
        self.file_path = file_path
        self.schema = schema
        self.table = table
        self.database = database
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.INFO)

        handler = logging.StreamHandler()
        handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(module)s - %(message)s"))
        handler.setLevel(logging.INFO)
        self.logger.addHandler(handler)

        try:
            self.logger.info("Connecting to psql...")
            self.conn = psycopg2.connect(dbname=self.database, user='dell', host='localhost', password='')
            self.logger.info("Connected to database!")
        except Exception as e:
            print(e)

        self.__validate_params()

    def __validate_params(self):
        if self.action not in ('import', 'export'):
            raise ValueError('Invalid action supplied.')

    def __create_table_query(self, columns):
        self.logger.info("Dropping existing table - {schema}.{table_name}".format(schema=self.schema,
                                                                                  table_name=self.table))
        q = "DROP TABLE IF EXISTS {schema}.{table_name};".format(schema=self.schema, table_name=self.table)
        run_query(self.conn, q)

        q = "CREATE SCHEMA IF NOT EXISTS {schema};CREATE TABLE {schema}.{table_name} (".format(schema=self.schema, table_name=self.table)
        for column in columns:
            q += "{column_name} VARCHAR(8000),".format(column_name=column)

        q = q[:-1] + ");"
        return q

    def __import_file(self):
        content = pd.read_csv(self.file_path)
        columns = list(content.columns)
        columns = [c.replace(' ', '_') for c in columns]
        create_table_query = self.__create_table_query(columns)
        self.logger.info("Creating table {schema}.{table_name}".format(schema=self.schema, table_name=self.table))
        run_query(self.conn, create_table_query)

        self.logger.info("Importing file...")
        run_query(self.conn, "  COPY {schema}.{table} FROM '{file_path}' DELIMITER ',' CSV HEADER;"
                  .format(schema=self.schema, table=self.table, file_path=os.path.abspath(self.file_path)))
        self.logger.info("Import Completed!")

    def __export(self):
        self.logger.info("Querying table...")
        q = "SELECT * FROM {schema}.{table};".format(schema=self.schema, table=self.table)
        mode = 'replace'
        curr = self.conn.cursor()
        curr.execute(q)
        columns = [column[0] for column in curr.description]
        result = [OrderedDict(zip(columns, row)) for row in curr.fetchmany(CHUNK_SIZE)]

        result_length = len(result)
        while len(result) > 0:
            self.logger.info("Written {0} records to file".format(result_length))
            df = pd.DataFrame(result)
            if mode == 'replace':
                df.to_csv(self.file_path, header=True, index=False)
            else:
                df.to_csv(self.file_path, mode='a', header=False, index=False)
            result = [OrderedDict(zip(columns, row)) for row in curr.fetchmany(CHUNK_SIZE)]
            result_length += len(result)
            mode = 'append'
        curr.close()
        self.conn.commit()
        self.logger.info('Export completed!')

    def start(self):
        if self.action == 'import':
            self.__import_file()
        else:
            self.__export()


def parse_arg():
    parser = argparse.ArgumentParser()
    parser.add_argument('-f', type=str, required=True)
    parser.add_argument('-d', type=str, required=False, default='datascience')
    parser.add_argument('-t', type=str, required=True)
    parser.add_argument('-x', type=str, required=True)
    parser.add_argument('-a', type=str, required=True, choices=['import', 'export'])

    args = parser.parse_args()
    return args


if __name__ == "__main__":
    args = parse_arg()
    importer = ImportExporter(args.f, args.x, args.t, args.a, args.d)
    importer.start()
