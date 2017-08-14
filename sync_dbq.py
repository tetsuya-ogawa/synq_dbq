# coding:utf-8
from google.cloud import bigquery
from datetime import datetime, timedelta
import dataset
import re

class SyncDbq:
    def __init__(self, user, password, host, db_name, bq):
        self.db = self._connect_db(user, password, host, db_name)
        self.bq = bigquery.Client.from_service_account_json(bq)
        self.db_name = db_name
        self.bq_dataset = self._connect_dataset()

    def sync_to_bq(self, targets):
        for target in targets:
            datas = self._fetch_by_db(target)
            table = self._create_bq_table(target)
            self._insert_to_bq(table, datas)
            print 'ok %s' % table.name

    def _connect_db(self, user, password, host, db_name):
        if password == '' or password is None:
            db = dataset.connect("mysql://{0}@{1}/{2}?charset=utf8".format(user, host, db_name))
        else:
            db = dataset.connect("mysql://{0}:{1}@{2}/{3}?charset=utf8".format(user, password, host, db_name))
        return db

    def _connect_dataset(self):
        bq_dataset = self.bq.dataset(self.db_name)
        if not bq_dataset.exists():
            bq_dataset.create()
        return bq_dataset

    def _fetch_by_db(self, target):
        table_name = target.keys()[0]
        values = target.values()[0]
        columns = []
        datas = []
        data = []
        for value in values:
            columns.append('`%s`.`%s`' % (table_name, value))
        _sql = (',').join(columns)
        sql = 'SELECT %s FROM `%s`' % (_sql, table_name)
        results = self.db.query(sql)
        for result in results:
            for value in values:
                data.append(result[value])
            datas.append(tuple(data))
            data = []
        return datas

    def _insert_to_bq(self, table, datas):
        for data in self._chunked(datas, 10000):
            table.insert_data(data)

    def _get_schema(self, table_name):
        table = dataset.Table(self.db, self.db.get_table(table_name).table)
        schema = dict()
        for c in table.table.c:
            schema[c.name] = self._convert_type(c.type)
        return schema

    def _convert_type(self, data_type):
        data_type = str(data_type)
        if re.match('VARCHAR', data_type) or re.match('TEXT', data_type):
            return 'STRING'
        elif re.match('DATETIME', data_type):
            return 'DATETIME'
        elif re.match('INTEGER', data_type) or re.match('TINYINT\(3\)', data_type):
            return 'INTEGER'
        elif re.match('TINYINT\(1\)', data_type):
            return 'INTEGER'
        else:
            return None

    def _create_bq_table(self, target):
        table_name = target.keys()[0]
        columns = target.values()[0]
        today = (datetime.now() + timedelta(days=-1)).strftime('%Y%m%d')
        table = self.bq_dataset.table('%s_%s' % (table_name, today))
        schema = self._get_schema(table_name)
        _schema = []
        for column in columns:
            _schema.append(bigquery.SchemaField(column, schema[column]))
        table.schema = tuple(_schema)
        table.create()
        return table

    def _chunked(self, list, n):
        return [list[x:x + n] for x in range(0, len(list), n)]
