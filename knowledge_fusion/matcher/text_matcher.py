import configparser
import math
from collections import Counter

import jieba
import numpy as np

from knowledge_fusion.settings import config_dir
from knowledge_fusion.preprocess.idf_counter import IdfCounter
from knowledge_fusion.interface.asset import Asset
from knowledge_fusion.utils.LRUDict import LRUDict

config = configparser.ConfigParser()
config.read(config_dir, encoding='utf-8')


class TextMatcher:

	def __init__(self):
		super().__init__()
		self._num_keywords = int(config.get('parameter', 'num_keywords'))
		self._max_count = int(config.get('data', 'max_text_count'))
		if self._num_keywords > self._max_count:
			raise Exception("max_count is smaller than num_keywords")
		self._idf_counter = IdfCounter()
		self._text_rep_dict = LRUDict(int(config.get('data', 'cache_text_rep_size')))
		self._text_statistics_dict = LRUDict(int(config.get('data', 'cache_text_statistics')))
		self._epsilon = 1e-8

	def get_match_score(self, table_id_1, field_id_1, table_id_2, field_id_2):
		rep_1 = self._get_rep(table_id_1, field_id_1)
		rep_2 = self._get_rep(table_id_2, field_id_2)
		statistics_1 = self._get_statistics(table_id_1, field_id_1)
		statistics_2 = self._get_statistics(table_id_2, field_id_2)
		common_words = rep_1[1].union(rep_2[1])
		vec_1 = np.array([rep_1[0][key] for key in common_words])
		vec_2 = np.array([rep_2[0][key] for key in common_words])
		score = self._cosine_similarity(vec_1, vec_2)
		if statistics_1[2] < 1 or statistics_2[2] < 1:
			return score
		rep_weight = score + (1 - score) * (statistics_1[2] + statistics_2[2]) / (2 * statistics_1[2] * statistics_2[2])
		return score * rep_weight + self._weighted_score(statistics_1, statistics_2) * (1 - rep_weight)

	def _cosine_similarity(self, vec_1, vec_2):
		return np.dot(vec_1, vec_2) / (np.linalg.norm(vec_1) * np.linalg.norm(vec_2) + self._epsilon)

	def _approx(self, n, v):
		return v - self._epsilon < n < v + self._epsilon

	def _get_rep(self, table_id, field_id):
		rep = self._text_rep_dict[(table_id, field_id)]
		if rep is None:
			words_tf = Counter()
			words_tf_idf = Counter()
			texts = Asset.get_data_by_id(table_id, field_id)
			for text in texts:
				for word in jieba.cut(text):
					words_tf[word.lower()] += 1  # 注意要小写化
			for key, value in words_tf.items():
				words_tf_idf[key] = self._idf_counter.idf_dict[key] * value
			if len(words_tf) > self._max_count:
				words_tf = Counter({word: words_tf[word] for word, count in words_tf_idf.most_common(self._max_count)})
			rep = words_tf, set(word for word, count in words_tf_idf.most_common(self._num_keywords))
			self._text_rep_dict[(table_id, field_id)] = rep
		return rep

	def _get_statistics(self, table_id, field_id):
		statistics = self._text_statistics_dict[(table_id, field_id)]
		text_set = set()
		word_set = set()
		text_cnt = 0
		word_cnt = 0
		if statistics is None:
			texts = Asset.get_data_by_id(table_id, field_id)
			for text in texts:
				text_set.add(text)
				text_cnt += 1
				for word in jieba.cut(text):
					word_set.add(word)
					word_cnt += 1
			avg_text_len = self._avg(text_set)
			var_text_len = self._var(text_set, avg_text_len)
			text_kind = self._kind(text_set, text_cnt)
			word_kind = self._kind(word_set, word_cnt)
			statistics = [avg_text_len, var_text_len, text_kind, word_kind]
			self._text_statistics_dict[(table_id, field_id)] = statistics
		return statistics

	def _avg(self, data):
		return sum([len(item) for item in data]) / len(data) if len(data) != 0 else 0

	def _var(self, data, avg):
		return sum([(len(item) - avg) ** 2 for item in data]) / len(data) if len(data) != 0 else 0

	def _kind(self, data, data_cnt):
		if data_cnt in [0, 1]:
			return 0
		len_ = len(data)
		if data_cnt == len_:
			len_ = len_ * 2 - 0.5
			data_cnt = data_cnt * 2
		return math.log2(data_cnt * len_ / (data_cnt - len_) + 1)
		# return math.log2(data_cnt) ** (len(data) / data_cnt) * math.log2(len(data)) if data_cnt != 0 else 0

	def _weighted_score(self, vec_1, vec_2):
		sim = [min(i1, i2) / max(i1, i2) if not self._approx(max(i1, i2), 0) else 1 for i1, i2 in zip(vec_1, vec_2)]
		min_ = min([i for i in sim if not self._approx(i, 0)])
		weight = [1 / math.log2(i / min_ + 1) if not self._approx(i, 0) else 1 for i in sim]
		weight_sum = sum(weight)
		weight = [i / weight_sum for i in weight]
		return sum([i * j for i, j in zip(sim, weight)])

	def clear(self):
		self._text_rep_dict = LRUDict(int(config.get('data', 'cache_text_rep_size')))
		self._text_statistics_dict = LRUDict(int(config.get('data', 'cache_text_statistics')))
