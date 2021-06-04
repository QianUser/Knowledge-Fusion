import configparser
from collections import defaultdict

import jieba
import numpy as np

from knowledge_fusion.interface.asset import Asset
from knowledge_fusion.matcher.instance_feature.instance_feature import DefaultType, Feature
from knowledge_fusion.settings import config_dir
from knowledge_fusion.utils.LRUDict import LRUDict

config = configparser.ConfigParser()
config.read(config_dir, encoding='utf-8')


def get_all_classes(model):
    """
    获取父类的所有子类，包括间接子类
    """
    all_subclasses = set()
    for subclass in model.__subclasses__():
        if subclass not in all_subclasses:
            if hasattr(subclass, 'feature_type') and subclass.feature_type is True:
                all_subclasses.add(subclass)
            all_subclasses.update(get_all_classes(subclass))
    return all_subclasses


class FeatureHandler:

    exec('from knowledge_fusion.model import model')
    model = eval(config.get('path', 'model'))()

    feature_list = list(get_all_classes(Feature))

    label_dict = defaultdict(list)
    for feature in feature_list:
        for label in feature.label:
            label_dict[label].append(feature)

    label_list = [item for item in label_dict.keys()]

    category_dict = defaultdict(list)
    for feature in feature_list:
        for category in feature.category:
            category_dict[category].append(feature)

    for category, features in category_dict.items():
        for i in range(0, len(features)):
            for j in range(i + 1, len(features)):
                if features[i].type[0] == features[j].type[0]:
                    raise Exception('{}与{}位于同一category：{}内,且存在两个首选type'.format(features[i], features[j], category))

    temp = defaultdict(set)
    for feature in feature_list:
        for category in feature.category:
            temp[feature] = temp[feature].union(category_dict[category])
    category_dict = temp

    def __init__(self, field):
        self._clear()
        score_dict = {item: -1 for item in self.label_list}
        for word in jieba.cut(field):
            for label in self.label_list:
                score_dict[label] = max(score_dict[label], self.model.predict_score(word, label))
        sorted_label = sorted(zip(score_dict.values(), score_dict.keys()), reverse=True)
        sorted_label = list(map(lambda item: item[1], sorted_label))
        self.order = []
        order_set = set()
        for label in sorted_label:
            for feature in self.label_dict[label]:
                if feature not in order_set:
                    order_set.add(feature)
                    self.order.append(feature)

    def match(self, text):
        visited = set()
        self.cnt += 1
        if DefaultType.match(text):
            self.default_cnt += 1
            return
        for feature in self.order:
            visited.add(feature)
            if feature.match(text):
                self.match_cnt += 1
                self.type_dict[feature.type[0]] += feature.weight
                for other_feature in self.category_dict[feature]:
                    if other_feature not in visited and other_feature.match(text):
                        self.type_dict[other_feature.type[0]] += other_feature.weight
                break
        # ! 目前该语句没什么作用
        Feature.clear()

    def result(self):
        for feature in self.feature_list:
            index = 0
            for i in range(len(feature.type) - 1, -1, -1):
                if self.type_dict[feature.type[i]] != 0:
                    index = i
                    break
            for i in range(0, index):
                self.type_dict[feature.type[index]] += self.type_dict[feature.type[i]]
                self.type_dict[feature.type[i]] = 0
        b = False
        for key, value in self.type_dict.items():
            if value != 0:
                # 这里没有加上权重
                self.type_dict[key] += self.default_cnt
                b = True
        if b:
            self.match_cnt += self.default_cnt
        if self.cnt == 0:
            result = self.type_dict, 0
        else:
            result = self.type_dict, self.match_cnt / self.cnt
        self._clear()
        return result

    def _clear(self):
        self.cnt = 0
        self.match_cnt = 0
        self.default_cnt = 0
        self.type_dict = {}
        for feature in self.feature_list:
            for type_ in feature.type:
                self.type_dict[type_] = 0


class InstanceMatcher:
    """
    实例匹配器
    """

    def __init__(self):
        super().__init__()
        self._semantic_statistics_dict = LRUDict(int(config.get('data', 'cache_instance_statistics')))
        self._epsilon = 1e-8

    def get_match_score(self, table_id_1, field_id_1, table_id_2, field_id_2):
        type_dict_1, ratio_1 = self._get_statistics(table_id_1, field_id_1)
        # print(table_id_1, field_id_1, type_dict_1, ratio_1)
        type_dict_2, ratio_2 = self._get_statistics(table_id_2, field_id_2)
        # print(table_id_2, field_id_2, type_dict_2, ratio_2)
        keys = type_dict_1.keys()
        vec_1 = np.array([type_dict_1[key] for key in keys])
        vec_2 = np.array([type_dict_2[key] for key in keys])
        if ratio_1 == 0 or ratio_2 == 0:
            return 0
        else:
            return self._cosine_similarity(vec_1, vec_2) * (2 * ratio_1 * ratio_2 / (ratio_1 + ratio_2))

    def _cosine_similarity(self, vec_1, vec_2):
        return np.dot(vec_1, vec_2) / (np.linalg.norm(vec_1) * np.linalg.norm(vec_2) + self._epsilon)

    def _get_statistics(self, table_id, field_id):
        statistics = self._semantic_statistics_dict[(table_id, field_id)]
        if statistics is None:
            feature_handler = FeatureHandler(Asset.get_field_logic_name_by_id(table_id, field_id))
            texts = Asset.get_data_by_id(table_id, field_id)
            for text in texts:
                feature_handler.match(text)
            statistics = feature_handler.result()
            self._semantic_statistics_dict[(table_id, field_id)] = statistics
        return statistics

    def clear(self):
        self._semantic_statistics_dict.clear()


if __name__ == '__main__':

    def get_match_score(data_1, data_2):
        type_dict_1, ratio_1 = _get_statistics(data_1)
        type_dict_2, ratio_2 = _get_statistics(data_2)
        keys = type_dict_1.keys()
        vec_1 = np.array([type_dict_1[key] for key in keys])
        vec_2 = np.array([type_dict_2[key] for key in keys])
        return _cosine_similarity(vec_1, vec_2) * (2 * ratio_1 * ratio_2 / (ratio_1 + ratio_2))

    def _cosine_similarity(vec_1, vec_2):
        return np.dot(vec_1, vec_2) / (np.linalg.norm(vec_1) * np.linalg.norm(vec_2) + 1e-8)

    def _get_statistics(data):
        feature_handler = FeatureHandler(logic_name)
        for text in data:
            feature_handler.match(text)
        statistics = feature_handler.result()
        return statistics

    logic_name = '房屋编号'
    data1 = ['52.9', '52', '46', '78', '51.3 ', ' 60.9', '89\n', '2019-2-27', '20188266', '88']
    data2 = ['34', 'hello world', 'but', '姓名', '2019-2-27', '岑静', '55.7', '29']
    print(get_match_score(data1, data2))
    print(get_match_score(data1, data2))
