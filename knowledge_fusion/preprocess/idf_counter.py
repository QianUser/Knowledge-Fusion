import configparser
import json
import math
import os
import sys
from collections import Counter

import jieba

from knowledge_fusion.settings import BASE_DIR, config_dir
from knowledge_fusion.utils.utils import Singleton

config = configparser.ConfigParser()
config.read(config_dir, encoding='utf-8')


class IdfCounter(metaclass=Singleton):
    """
    构建Idf字典
    """
    def __init__(self):
        self.cnt = 0
        self.idf_dict = Counter()
        self.idf_path = os.path.join(BASE_DIR, config.get('path', 'idf_path'))
        self._construct_idf()

    def _file_counter(self, filename, *keys):
        # print('process file: {}'.format(filename))
        with open(filename, encoding='utf-8') as file:
            for line in file:
                words = []
                try:
                    item = json.loads(line)
                    for key, value in item.items():
                        if key in keys:
                            words += jieba.lcut(value)
                except Exception:
                    sys.stderr.write('文件：{}\n\t格式错误：{}'.format(filename, line))
                    continue
                words = set(words)
                for word in words:
                    self.idf_dict[word.lower()] += 1  # 注意要小写化
                self.cnt += 1
                # if self.cnt % 10000 == 0:
                #     print(self.cnt, line, sep=': ', end='')

    # ! 使用爬取的网页数据生成Idf字典，与数据库数据未必匹配
    def _construct_idf(self):
        """
        使用爬取的网页数据生成Idf字典，与数据库数据未必匹配
        """
        if os.path.exists(self.idf_path):
            self.idf_dict = Counter(json.load(open(self.idf_path)))
            return
        base_dir = 'data'
        self._file_counter(os.path.join(base_dir, 'baike_qa_train.json'), 'title', 'desc', 'answer')
        self._file_counter(os.path.join(base_dir, 'baike_qa_valid.json'), 'title', 'desc', 'answer')
        self._file_counter(os.path.join(base_dir, 'news2016zh_train.json'), 'keywords', 'desc', 'title', 'source', 'content')
        self._file_counter(os.path.join(base_dir, 'news2016zh_valid.json'), 'keywords', 'desc', 'title', 'source', 'content')
        for root, dirs, files in os.walk(os.path.join(base_dir, 'wiki_zh')):
            for file in files:
                self._file_counter(os.path.join(root, file), 'text')
        for key, value in self.idf_dict.items():
            self.idf_dict[key] = math.log(self.cnt / (value + 1))
        with open(self.idf_path, 'wt') as file:
            json.dump(dict(self.idf_dict), file)
