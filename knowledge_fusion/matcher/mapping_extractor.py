import configparser

import numpy as np

from knowledge_fusion.matcher.instance_matcher import InstanceMatcher
from knowledge_fusion.matcher.semantic_matcher import SemanticMatcher
from knowledge_fusion.matcher.text_matcher import TextMatcher
from knowledge_fusion.settings import config_dir
from knowledge_fusion.interface.asset import Asset

config = configparser.ConfigParser()
config.read(config_dir, encoding='utf-8')


class MappingExtractor:
    """
    匹配抽取，建立字段的映射关系
    只考虑一对一映射
    """

    def __init__(self):
        self._text_matcher = TextMatcher()
        exec('from knowledge_fusion.model import model')
        self._semantic_matcher = SemanticMatcher(eval(config.get('path', 'model')))
        self._instance_matcher = InstanceMatcher()
        self._punish = float(config.get('parameter', 'punish'))
        self._threshold = float(config.get('parameter', 'threshold'))

    def match(self, table_id_1, table_id_2, both=False):
        fields_id_1 = Asset.get_fields_id_by_table_id(table_id_1)
        fields_id_2 = Asset.get_fields_id_by_table_id(table_id_2)
        if table_id_1 == table_id_2:
            return {(table_id_1, field_id): (table_id_1, field_id) for field_id in fields_id_1}
        matrix = np.zeros([len(fields_id_1), len(fields_id_2)])
        for index_1, field_id_1 in enumerate(fields_id_1):
            for index_2, field_id_2 in enumerate(fields_id_2):
                matrix[index_1][index_2] = self._get_match_score(table_id_1, field_id_1, table_id_2, field_id_2)
        if len(fields_id_1) <= len(fields_id_2):
            matching = self._marriage_matching(matrix)
        else:
            matching = list(map(lambda x: (x[1], x[0]), self._marriage_matching(matrix.T)))
        result = {}
        for i, j in matching:
            result[(table_id_1, fields_id_1[i])] = (table_id_2, fields_id_2[j])
        if both:
            for i, j in matching:
                result[(table_id_2, fields_id_2[j])] = (table_id_1, fields_id_1[i])
        return result

    def _marriage_matching(self, matrix, return_score=False):
        size, n_col = np.shape(matrix)
        matched = np.full(size, -1, dtype=np.int32)
        choose = np.full(n_col, -1, np.int32)
        refused = np.zeros(size, dtype=np.int32)
        order = np.argsort(-matrix)
        cond = False
        while not cond:
            cond = True
            for i in range(0, size):
                if matched[i] != -1:
                    continue
                cond = False
                j = refused[i]
                refused[i] += 1
                o = order[i][j]
                c = choose[o]
                if c == - 1 or matrix[i][o] > matrix[c][o]:
                    matched[i] = o
                    choose[o] = i
                    if c != -1:
                        matched[c] = -1
        result = []
        if return_score:
            for index, value in enumerate(matched):
                if matrix[index][value] >= self._threshold:
                    result.append((index, value, matrix[index][value]))
        else:
            for index, value in enumerate(matched):
                if matrix[index][value] >= self._threshold:
                    result.append((index, value))
        return result

    def _get_match_score(self, table_id_1, field_id_1, table_id_2, field_id_2):
        text_score = self._text_matcher.get_match_score(table_id_1, field_id_1, table_id_2, field_id_2)
        instance_score = self._instance_matcher.get_match_score(table_id_1, field_id_1, table_id_2, field_id_2)
        semantic_score = self._semantic_matcher.get_match_score(table_id_1, field_id_1, table_id_2, field_id_2)
        score = instance_score * instance_score + (1 - instance_score) * text_score
        return score - (self._punish - score) * (1 - semantic_score)
        # score = max(self._text_matcher.get_match_score(table_id_1, field_id_1, table_id_2, field_id_2), self._instance_matcher.get_match_score(table_id_1, field_id_1, table_id_2, field_id_2))
        # return score - (self._punish - score) * (1 - self._semantic_matcher.get_match_score(table_id_1, field_id_1, table_id_2, field_id_2))

    def clear(self):
        self._text_matcher.clear()
        self._instance_matcher.clear()
