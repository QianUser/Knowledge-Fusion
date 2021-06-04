import configparser

import os

from knowledge_fusion.settings import BASE_DIR
from knowledge_fusion.interface.asset import Asset

# 设计表的物理名的变体用于测试
# ! 效果不是很明显

config = configparser.ConfigParser()
config.read(os.path.join(BASE_DIR, 'config.ini'), encoding='utf-8')
eval_data_path = 'data'
logic_name_var_path = os.path.join(eval_data_path, 'logic_name_var.txt')

if not os.path.exists(eval_data_path):
    os.mkdir(eval_data_path)


def gen_logic_name_var(table_id_list, encoding='utf-8'):
    all_logic_name = set()
    fields_info = Asset._select_fields_info(table_id_list)
    for table in fields_info:
        for field in table['fieldList']:
            all_logic_name.add(field['logicName'])
    with open(logic_name_var_path, 'w', encoding=encoding) as file:
        file.write('\n'.join(all_logic_name))
    # 手动创建词汇变体


if __name__ == '__main__':
    table_id_list = []
    for root, dirs, files in os.walk('../simulated_interface/data/data'):
        for file in files:
            table_id_list.append(file.split('.')[0])
    gen_logic_name_var(table_id_list)