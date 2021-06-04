import json
import os

import pandas

from knowledge_fusion.settings import BASE_DIR
from knowledge_fusion.utils.utils import read_json, write_json


class NameHandler:

    base_dir = os.path.join(BASE_DIR, 'knowledge_fusion', 'matcher', 'data')
    corpus_dir = os.path.join(base_dir, 'Chinese-Names-Corpus-master')
    feature_dir = os.path.join(base_dir, 'instance_feature', 'name')

    chinese_first_names_corpus_path = os.path.join(corpus_dir, 'Chinese_Names_Corpus', 'Chinese_Family_Name（1k）.xlsx')
    chinese_names_corpus_path = os.path.join(corpus_dir, 'Chinese_Names_Corpus', 'Chinese_Names_Corpus（120W）.txt')
    chinese_first_names_path = os.path.join(feature_dir, 'chinese_first_names.json')
    chinese_second_names_path = os.path.join(feature_dir, 'chinese_second_names.json')
    chinese_names_path = os.path.join(feature_dir, 'chinese_names.json')
    english_names_corpus_path = os.path.join(corpus_dir, 'English_Names_Corpus', 'English_Names_Corpus（2W）.txt')
    english_names_path = os.path.join(feature_dir, 'english_names.json')

    @classmethod
    def construct_chinese_first_name(cls, sheets):
        first_names = set()
        tables = pandas.read_excel(cls.chinese_first_names_corpus_path, engine='openpyxl', sheet_name=sheets)
        for i in range(0, len(tables)):
            for row in tables[i].values:
                first_names.add(row[0])
        return first_names

    @classmethod
    def construct_chinese_name(cls, first_names, encoding='utf-8'):
        second_names = set()
        names = set()
        with open(cls.chinese_names_corpus_path, 'r', encoding=encoding) as file:
            for i in range(3):
                next(file)
            for line in file:
                line = line.strip()
                if len(line) < 2:
                    continue
                matched = False
                for i in range(len(line) - 1, 0, -1):
                    if line[:i] in first_names:
                        if i == len(line) - 1:
                            names.add(line)
                        else:
                            second_names.add(line[i:])
                        matched = True
                if not matched:
                    first_names.add(line[0])
                    if len(line) == 2:
                        names.add(line)
                    else:
                        second_names.add(line[1:])
        return first_names, second_names, names

    @classmethod
    def construct_english_names(cls, encoding='utf-8'):
        names = set()
        with open(cls.english_names_corpus_path, 'r', encoding=encoding) as file:
            for i in range(3):
                next(file)
            for line in file:
                line = line.strip().lower()
                names.add(line)
        return names

    @classmethod
    def store_chinese_names(cls):
        first_names = cls.construct_chinese_first_name([0])
        first_names, second_names, names = cls.construct_chinese_name(first_names)
        write_json(list(first_names), cls.chinese_first_names_path)
        write_json(list(second_names), cls.chinese_second_names_path)
        write_json(list(names), cls.chinese_names_path)

    @classmethod
    def store_english_names(cls):
        names = cls.construct_english_names()
        write_json(list(names), cls.english_names_path)

    @classmethod
    def get_names(cls, language):
        if language.lower() == 'chinese':
            if not os.path.exists(cls.chinese_first_names_path) or not os.path.exists(cls.chinese_second_names_path) or not os.path.exists(cls.chinese_names_path):
                cls.store_chinese_names()
            return set(read_json(cls.chinese_first_names_path)), set(read_json(cls.chinese_second_names_path)), set(read_json(cls.chinese_names_path))
        elif language.lower() == 'english':
            if not os.path.exists(cls.english_names_path):
                cls.store_english_names()
            return set(read_json(cls.english_names_path))
