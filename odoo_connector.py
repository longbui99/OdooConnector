# Init objects
import logging
import json
import xmlrpc.client
from urllib.parse import urlparse
from functools import wraps


_logger = logging.getLogger(__name__)


def error_wrapper(method):
    @wraps(method)
    def wrapper_func(*args, **kwargs):
        try:
            return method(*args, **kwargs)
        except Exception as e:
            print(str(e).replace("\\n", "\n"))
            raise e
    return wrapper_func

class OdooResource:

    @error_wrapper
    def __init__(self, url, database, user, password):
        url.rstrip('/')
        common = xmlrpc.client.ServerProxy('{}/xmlrpc/2/common'.format(url), encoding='utf-8')
        common.version()
        uid = common.authenticate(database, user, password, {})
        model = xmlrpc.client.ServerProxy('{}/xmlrpc/2/object'.format(url), allow_none=True)
        _logger.info(f"Init connection: {url}, {database}, {user}, {password}")
        @error_wrapper
        def env(*args):
            return model.execute_kw(database, uid, password, *args)
        self.connection = env
        self.xmlrpc_connection = model
        self.name = urlparse(url).netloc

    @error_wrapper
    def load(self, *args):
        return self.connection(*args)

    @error_wrapper
    def store(self, recordset, model):
        return recordset.flush_to_odoo_recordset(self.connection, model)

    @error_wrapper
    def recomputing_model(self, recordset, model):
        return recordset.odoo_recompute_model(self.connection, model)

    def close(self):
        return True

import json
from datetime import datetime


class Field:
    def __init__(self, value):
        self.value = value
        self.id = False

    def __eq__(self, __value: object) -> bool:
        return (self.value == __value.value)

    def __lt__(self, __value: object):
        return self.value < __value.value

    def __gt__(self, __value: object):
        return self.value > __value.value

    def __get__(self, instance, owner=None):
        return self.value

    @property
    def fetch(self):
        value = self.value
        if isinstance(self.value, datetime):
            value = self.value.isoformat()
        return value

    def to_dict_value(self):
        return self.fetch

    def set(self, value):
        self.value = value

class Relation(Field):
    def __init__(self, value):
        record_id, record_value = False, ''
        if isinstance(value, (list, tuple)) and len(value):
            if isinstance(value[0], int):
                record_id = value[0]
                record_value = value[1]
            else:
                raise ValueError("The format 1-n and n-n doesn't supported")
        self.value = record_value
        self.id = record_id

    def to_dict_value(self):
        return {'id': self.id, 'value': self.value}

    @property
    def fetch(self):
        return self.id

    def set(self, value):
        if isinstance(value, (list, tuple)):
            if isinstance(self.value, Field):
                self.value = value[1]
            self.id = value[0]

class Record:
    def __init__(self, values, key='id') -> None:
        self.keys = []
        for key, value in values.items():
            if isinstance(value, (list, tuple, set)):
                field_init = Relation
            else:
                field_init = Field
            setattr(self, key, field_init(value))
            self.keys.append(key)
        self.id = values.get('id')
        self.is_merged = False
        self.is_return_line = False
        self.active = True

    def __setitem__(self, key, value):
        self[key].set(value)

    def __getitem__(self, item):
         return getattr(self, item)

    def __str__(self):
        return str(self.to_dict())

    def new(self, key, value):
        if isinstance(value, (list, tuple, set)):
            field_init = Relation
        else:
            field_init = Field
        setattr(self, key, field_init(value))
        self.keys.append(key)

    def update(self, key, field_type):
        setattr(self, key, field_type)
        self.keys.append(key)

    def to_dict(self):
        res = dict()
        for key in self.keys:
            if isinstance(self[key], (Relation, Field)):
                res[key] = self[key].to_dict_value()
            else:
                res[key] = self[key]
        return res

    def to_value_dict(self):
        res = dict()
        for key in self.keys:
            if isinstance(self[key], (Relation, Field)):
                res[key] = self[key].value
            else:
                res[key] = self[key]
        return res

    def to_values(self):
        res = dict()
        for key in self.keys:
            if isinstance(self[key], (Relation, Field)):
                res[key] = self[key].fetch
            else:
                res[key] = self[key]
        return res

    def copy_to_record_style(self, mapping_fields):
        new_record = Record({})
        for key in self.keys:
            new_key = key if key not in mapping_fields else mapping_fields[key]
            new_record[new_key] = self[key]
        return new_record

class Recordset:
    def __init__(self, value_list=[], o="", key='id'):
        self.__data = dict()
        if value_list:
            for index, values in enumerate(value_list):
                if key not in values:
                    record_key = index
                else:
                    record_key = values[key]
                self.__data[record_key] = Record(values)
        self.key = key
        self.object = o
        self.enter_list_records()
        print(repr(self))

    def __getitem__(self, item):
         return self.__data.get(item)

    def __setitem__(self, key, record: Record):
        self.__data[key] = record

    def __iter__(self):
        for value in self.__values:
            yield value

    def __str__(self):
        return json.dumps(self.to_dict(), indent=4)

    def __len__(self):
        return len(self.__values)

    def __repr__(self):
        return f"{self.object}(key={self.key};length={len(self)})"

    def map(self, key='id'):
        records = self.__values
        res = []
        for record in records:
            res.append(record[key].fetch)
        return res

    def get_values(self):
        return self.__values

    def filtered(self, function):
        return list(filter(function, self.__values))

    def sort(self, key='', reverse=False):
        self.__values = sorted(self.__values, key=key, reverse=reverse)

    @property
    def keys(self):
        return list(self.__data.keys())

    def items(self):
        for key, value in self.__data.items():
            yield (key, value)

    def init_key(self, key):
        res = dict()
        setattr(self, key, res)

    def add_key_item(self, key, record, function, merge_function):
        value_key = function(record)
        res = getattr(self, key)
        if value_key in res:
            merge_function(res[value_key], record)
            record.is_merged = True
        else:
            setattr(record, key, value_key)
            res[value_key] = record

    def enter_list_records(self):
        self.__values = list(self.__data.values())
        return self.__values

    def to_dict(self):
        res = dict()
        for k, v in self.__data.items():
            res[k] = v.to_dict()
        return res

    def to_value_list(self):
        res = list()
        for k, v in self.__data.items():
            res.append(v.to_value_dict())
        return res

    def string_list_json_format(self):
        return json.dumps(self.to_value_list(), indent=2)

    def to_records_dict(self):
        res = dict()
        for k, v in self.__data.items():
            res[k] = v.to_dict()
        return res

    def to_value_dict(self):
        res = dict()
        for k, v in self.__data.items():
            res[k] = v.to_value_dict()
        return res

    def string_records_json_format(self):
        return json.dumps(self.to_records_dict(), indent=2)

    def copy_to_recordset_style(self, mapping_fields):
        """
            mapping_fields:
                {'self.key'-> new_key}
        """
        obj = Recordset()
        for key, record in self.items():
            obj[key] = record.copy_to_record_style(mapping_fields)
        return obj

    def flush_to_odoo_recordset(self, odoo_rpc_connection, model):
        value_list = self.to_value_list()
        odoo_rpc_connection(model, 'create', [value_list])
        return value_list

    def flush_to_postgres_recordset(self, postgres_cursor, table, id_key='id'):

        value_list = self.to_value_list()
        if not len(value_list):
            return ""
        key_list = list(value_list[0].keys())
        mogrify = ",".join(map(lambda v: f"%({v})s", key_list))

        keys_str = ",".join(map(lambda key: f'"{key}"', key_list))

        insert_stmt = f"INSERT INTO {table} ({keys_str}) VALUES "
        record_list = []
        for value_dict in value_list:
            value_stmt = postgres_cursor.mogrify(mogrify, value_dict).decode()
            value_stmt = f"({value_stmt})"
            record_list.append(value_stmt)
        value_stmt = ",\n".join(record_list)
        insert_stmt += value_stmt + ";COMMIT;"
        try:
            postgres_cursor.execute(insert_stmt)
        except Exception as e:
            _logger.warning(insert_stmt)
            raise e
        if id_key and id_key in key_list:
            postgres_cursor.execute(f"SELECT setval('{table}_id_seq', max(id)) FROM {table};")
        return insert_stmt

    def flush_revert_to_postgres_recordset(self, postgres_cursor, table, id_key="id"):
        value_list = self.to_value_list()
        if not len(value_list):
            return ""
        if id_key:
            keys = []
            for value_dict in value_list:
                keys.append(value_dict[keys])
            if len(keys):
                query_stmt = f"DELETE FROM {table} WHERE {id_key} in %(keylist)s;COMMIT;"
                postgres_cursor.execute(query_stmt, {'keylist': tuple(keys)})
                postgres_cursor.execute(f"SELECT setval('{table}_id_seq', max(id)) FROM {table};")

    def odoo_recompute_model(self, rpc_connection, model):
        records = self.to_records_dict()
        if len(records):
            ids = list(map(int, records.keys()))
            fields = list(records[ids[0]].keys())
            res = rpc_connection(model, 'search', [[('id', 'in', ids)]])
            rpc_connection(model, 'modified', [ids, fields])
            return res
        return False

