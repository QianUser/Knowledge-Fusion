import configparser
import logging
import os
from collections import defaultdict
from functools import wraps

from celery import Celery

from knowledge_fusion.matcher.mapping_extractor import MappingExtractor
from knowledge_fusion.settings import config_dir, BASE_DIR
from knowledge_fusion.interface.asset import post_result, Asset

logger = logging.getLogger('match_log')
config = configparser.ConfigParser()
config.read(config_dir, encoding='utf-8')
mapping_extractor = MappingExtractor()
app = Celery('field_matcher', broker=config.get('celery', 'broker'), backend=config.get('celery', 'backend'))

config.read(os.path.join(BASE_DIR, 'config.ini'), encoding='utf-8')
logic_name_var_path = os.path.join(BASE_DIR, config.get('path', 'eval_data_path'), config.get('path', 'logic_name_var_path'))


def clear_cache(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        result = func(*args, **kwargs)
        mapping_extractor.clear()
        Asset.clear()
        return result
    return wrapper


def convert_match(match):
    data = []
    for key, value in match.items():
        item = []
        for v in value:
            item.append({'assetId': v[0], 'fieldId': v[1]})
        data.append({'assetId': key[0], 'fieldId': key[1], 'relatedFields': item})
    return data


# ! 以下接口没有考虑两个参数重复的情况

@app.task
@clear_cache
def match_all_interrelation(task_id, table_id_list):
    match = defaultdict(list)
    for i in range(0, len(table_id_list)):
        for j in range(i + 1, len(table_id_list)):
            for key, value in mapping_extractor.match(table_id_list[i], table_id_list[j], both=True).items():
                match[key].append(value)
    return post_result(task_id, convert_match(match))


@app.task
@clear_cache
def match_one2all_interrelation(task_id, table_id_src, table_id_list_dest):
    match = defaultdict(list)
    for table_id_dest in table_id_list_dest:
        for key, value in mapping_extractor.match(table_id_src, table_id_dest).items():
            match[key].append(value)
    return post_result(task_id, convert_match(match))


@app.task
@clear_cache
def match_one2one_interrelation(task_id, tabld_id_src, tabld_id_dest):
    match = defaultdict(list)
    for key, value in mapping_extractor.match(tabld_id_src, tabld_id_dest).items():
        match[key].append(value)
    return post_result(task_id, convert_match(match))


@app.task
@clear_cache
def match_some2all_interrelation(task_id, table_id_list_src, table_id_list_dest):
    match = defaultdict(list)
    for table_id_src in table_id_list_src:
        for table_id_dest in table_id_list_dest:
            for key, value in mapping_extractor.match(table_id_src, table_id_dest).items():
                match[key].append(value)
    return post_result(task_id, convert_match(match))
