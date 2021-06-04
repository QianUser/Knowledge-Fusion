import csv
import json
import os.path
import re
import sys

import pandas

# ! 耦合性大

def process_data(table_id, field_list, csv_dirname, table_name, encoding='utf-8'):
    """
    处理csv文件，获取实例信息
    """
    data = {'asset_id': str(table_id), 'fieldName': [item['physicsName'] for item in field_list], 'assetData': []}
    fields_len = len(data['fieldName'])
    with open(os.path.join(csv_dirname, table_name + '.csv'), encoding=encoding) as file:
        f_csv = csv.reader(file)
        for row in f_csv:
            if len(row) == fields_len + 1:
                data['assetData'].append([item.rstrip() for item in row[1:]])
            elif len(row) == fields_len:
                data['assetData'].append([item.rstrip() for item in row])
            else:
                raise AssertionError('字段不匹配')
    return data


def get_table_logic_name(filename, sheets):
    """
    添加表的逻辑名
    """
    phy2log = {}
    tables = pandas.read_excel(filename, engine='openpyxl', sheet_name=sheets)
    for i in range(0, len(tables)):
        for row in tables[i].values:
            physics_name = row[0]
            logic_name = row[1]
            if physics_name in phy2log:
                sys.stderr.write('已存在的表格物理名：{}\n'.format(physics_name))
            else:
                phy2log[physics_name] = logic_name
    return phy2log


def process_sql(sql_filename, csv_dirname, encoding='utf-8', linesep='\n'):
    """
    处理sql文件，获取表、字段信息
    """
    fields_dict = {}
    data_list = []
    global table_id
    global exist_tables
    global phy2log
    table_name = ''
    field_list = []
    field_id = 1
    table_pattern = re.compile(r'^\s*CREATE TABLE `(.+)`\s*\(\s*' + linesep + '$')
    field_pattern = re.compile(r'^\s*`(.+)`.+COMMENT\s+\'(.+)\'(\s*,)?' + linesep + '$')
    with open(sql_filename, 'rt', encoding=encoding) as file:
        for line in file:
            fm = field_pattern.match(line)
            if fm:
                field_list.append({'id': str(field_id), 'physicsName': fm.group(1), 'logicName': fm.group(2).strip()})
                field_id += 1
            else:
                tm = table_pattern.match(line)
                if tm:
                    if field_list:
                        table_id += 1
                        if table_name not in exist_tables:
                            fields = {'id': str(table_id), 'physicsName': table_name, 'logicName': phy2log[table_name], 'fieldList': field_list}
                            fields_dict[str(table_id)] = fields
                            data_list.append(process_data(table_id, field_list, csv_dirname, table_name))
                            exist_tables.add(table_name)
                        else:
                            sys.stderr.write('已存在的表格：{}\n'.format(table_name))
                        field_list = []
                        field_id = 1
                    table_name = tm.group(1)
    if field_list:
        table_id += 1
        if table_name not in exist_tables:
            fields = {'id': str(table_id), 'physicsName': table_name, 'logicName': phy2log[table_name], 'fieldList': field_list}
            fields_dict[str(table_id)] = fields
            data_list.append(process_data(table_id, field_list, csv_dirname, table_name))
            exist_tables.add(table_name)
        else:
            sys.stderr.write('已存在的表格：{}\n'.format(table_name))
    return fields_dict, data_list


def gen_data(data_dirname, data, encoding='utf-8'):
    """
    生成用于模拟 /api/asset/data接口的数据的文件
    """
    with open(os.path.join(data_dirname, data['asset_id'] + '.json'), 'xt', encoding=encoding) as file:
        json.dump(data, file)


def gen_fields(fields_filename, data, encoding='utf-8'):
    """
    生成用于模拟/api/asset/fields接口的数据的文件
    """
    with open(fields_filename, 'xt', encoding=encoding) as file:
        json.dump(data, file)


if __name__ == '__main__':
    table_id = 0
    sql_dirname = 'data/sql'
    csv_dirname = 'data/数据csv'
    logic_filename = 'data/表名对应.xlsx'
    fields_filename = 'data/fields.json'
    data_dirname = 'data/data'
    all_fields = {}
    exist_tables = set()
    phy2log = get_table_logic_name(logic_filename, [0, 1])

    if not os.path.exists(data_dirname):
        os.mkdir(data_dirname)

    fields_dict, data_list = process_sql(os.path.join(sql_dirname, '浙江省1-28.sql'), os.path.join(csv_dirname, '浙江省'))
    all_fields.update(fields_dict)
    for data in data_list:
        gen_data(data_dirname, data)

    fields_dict, data_list = process_sql(os.path.join(sql_dirname, '浙江省29-57.sql'), os.path.join(csv_dirname, '浙江省'))
    all_fields.update(fields_dict)
    for data in data_list:
        gen_data(data_dirname, data)

    fields_dict, data_list = process_sql(os.path.join(sql_dirname, '浙江省58-85.sql'), os.path.join(csv_dirname, '浙江省'))
    all_fields.update(fields_dict)
    for data in data_list:
        gen_data(data_dirname, data)

    fields_dict, data_list = process_sql(os.path.join(sql_dirname, '社会54-part1.sql'), os.path.join(csv_dirname, '社会56'))
    all_fields.update(fields_dict)
    for data in data_list:
        gen_data(data_dirname, data)

    fields_dict, data_list = process_sql(os.path.join(sql_dirname, '社会54part2.sql'), os.path.join(csv_dirname, '社会56'))
    all_fields.update(fields_dict)
    for data in data_list:
        gen_data(data_dirname, data)

    gen_fields(fields_filename, all_fields)
