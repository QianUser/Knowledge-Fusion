import configparser
import json
import logging
import os
import random
import time

import requests

from knowledge_fusion.settings import BASE_DIR
from knowledge_fusion.utils.LRUDict import LRUDict
from knowledge_fusion.utils.utils import default_return

logger = logging.getLogger('server_log')

config = configparser.ConfigParser()
config.read(os.path.join(BASE_DIR, 'config.ini'), encoding='utf-8')
post_url = config.get('ip', 'post_url')
get_url = config.get('ip', 'get_url')
result_url = config.get('ip', 'result_url')
page_size = int(config.get('data', 'page_size'))
pool_size = int(config.get('process', 'pool_size'))
post_result_path = os.path.join(BASE_DIR, config.get('path', 'post_result_path'))

# ! 将test放置到这儿也许不是什么好事，因为影响正常运行，增加耦合度
test = True if config.get('mode', 'test').lower() == 'true' else False

if test:
    logic_name_var_path = os.path.join(BASE_DIR, config.get('path', 'eval_data_path'), config.get('path', 'logic_name_var_path'))
    logic_name_var = {}
    with open(logic_name_var_path, encoding='utf-8') as file:
        for line in file:
            words = line.strip().split('\t')
            logic_name_var[words[0]] = words


def request_fields_info(asset_ids):
    """
    根据资产ID列表查询资产表结构信息
    """
    logger.info('Request fields info. assetIds: {}'.format(asset_ids))
    return json.loads(requests.post(post_url, {'assetIds': asset_ids}).text)


def request_data_info(asset_id, limit=None):
    """
    根据资产ID查询资产表数据
    """
    logger.info('Request data info. asset_id: {}'.format(asset_id))
    if limit is None:
        return json.loads(requests.get(get_url, params={'asset_id': asset_id}).text)
    else:
        return json.loads(requests.get(get_url, params={'asset_id': asset_id, 'limit': limit}).text)


def call_back_result(data):
    post_time = time.time()
    result = requests.post(result_url, data)
    logger.info('Call back taskId: {}, pageNum: {}, time:{}'.format(data["taskId"], data["pageNum"], time.time() - post_time))
    return result


def post_result(task_id, result):
    """
    计算结果 数据回调接口
    """
    all_data = []
    for i in range((len(result) + page_size - 1) // page_size):
        start = page_size * i
        end = min(len(result), page_size * (i + 1))
        data = {
            'taskId': task_id,
            'status': 1,
            'pageNum': i + 1,
            'pageSize': end - start,
            'data': json.dumps(result[start:end])
        }
        all_data.append(data)
    if len(all_data) == 0:
        all_data.append({
            'taskId': task_id,
            'status': 1,
            'pageNum': 0,
            'pageSize': 0,
            'data': json.dumps([])
        })
    # pool = Pool(processes=pool_size)
    # response = [pool.apply_async(call_back_result, (x,)) for x in all_data]
    response = [call_back_result(x) for x in all_data]
    # pool.close()
    # pool.join()
    for i in range(0, len(response), page_size):
        logger.info('Page {} - Result {}'.format(i + 1, response[i].text))
    with open(post_result_path, 'w+', encoding='utf-8') as file:
        json.dump(all_data, file)


class Asset:

    """
    通用的关系图谱调用接口
    使用LRU字典缓存最近访问数据；请求完成，数据需要被删除
    对于调用者屏蔽访问细节
    """

    data_dict = LRUDict(int(config.get('data', 'cache_data_size')))
    fields_dict = LRUDict(int(config.get('data', 'cache_fields_size')))

    @classmethod
    @default_return(value=[])
    def get_data_by_id(cls, table_id, field_id):
        all_data = cls.data_dict[table_id]
        if all_data is None:
            cls._update_data(table_id)
            return cls.data_dict[table_id].get(field_id)
        return all_data.get(field_id)

    @classmethod
    @default_return(value='')
    def get_field_logic_name_by_id(cls, table_id, field_id):
        fields = cls.fields_dict[table_id]
        if fields is None:
            cls._update_fields(table_id)
            field = cls.fields_dict[table_id].get(field_id)
            return cls._choose(field[1] if field is not None else None)
        field = fields.get(field_id)
        return cls._choose(field[1] if field is not None else None)

    @classmethod
    @default_return(value=[])
    def get_fields_id_by_table_id(cls, table_id):
        fields = cls.fields_dict[table_id]
        if fields is None:
            cls._update_fields(table_id)
            return [field for field in cls.fields_dict[table_id].keys()]
        return [field for field in fields.keys()]

    @classmethod
    @default_return(value=[])
    def get_fields_by_table_id(cls, table_id):
        fields = cls.fields_dict[table_id]
        if fields is None:
            cls._update_fields(table_id)
            return cls.fields_dict[table_id]
        return fields

    @classmethod
    def _update_fields(cls, table_id):
        cls.fields_dict[table_id] = {}
        fields = cls._select_fields_info([table_id])
        if len(fields) > 0:
            for field in fields[0]['fieldList']:
                cls.fields_dict[table_id][field['id']] = (field['physicsName'], field['logicName'])

    @classmethod
    def _update_data(cls, table_id):
        cls.data_dict[table_id] = {}
        all_data = cls._select_data_info(table_id)
        fields = cls.get_fields_by_table_id(table_id)
        id_dict = {value[0]: key for key, value in fields.items()}
        for field, data in zip(all_data[0], all_data[1]):
            if id_dict.get(field) is not None:
                cls.data_dict[table_id][id_dict.get(field)] = data

    @classmethod
    def _choose(cls, logic_name):
        if not test:
            return logic_name
        else:
            if logic_name is not None:
                temp = logic_name_var.get(logic_name)
                if temp is not None and len(temp) > 0:
                    logic_name = random.choice(temp)
            return logic_name

    @classmethod
    def clear(cls):
        cls.data_dict.clear()
        cls.fields_dict.clear()

    @classmethod
    def _select_fields_info(cls, table_list_id):
        """
        对request_fields_info的进一步封装
        """
        fields_info = request_fields_info(table_list_id)
        if fields_info['message'] != 'SUCCESS':
            logger.info('No fields info exist for assetsIds {}'.format(table_list_id))
            return []
        return fields_info['data']

    @classmethod
    def _select_data_info(cls, table_id):
        """
        对request_data_info的进一步封装
        """
        data_info = request_data_info(table_id)
        if data_info['message'] != 'SUCCESS':
            logger.info('No data info exist for asset_id {}'.format(table_id))
            return [[], []]
        data_info = data_info['data']
        fields = data_info['fieldName']
        data = []
        length = len(fields)
        for i in range(0, length):
            data.append([])
        for item in data_info['assetData']:
            for i in range(0, length):
                data[i].append(item[i])
        return fields, data