import configparser
import logging
import operator
import os
from collections import defaultdict

import numpy as np

from knowledge_fusion.interface.asset import Asset
from knowledge_fusion.matcher.mapping_extractor import MappingExtractor

logging.getLogger().setLevel(logging.WARNING)
eval_data_path = 'data'
eval_result_path = os.path.join('data', 'result.txt')

if not os.path.exists(eval_data_path):
	os.mkdir(eval_data_path)


class Test:
	table_id_list = []
	all_match = defaultdict(set)
	len_match = 0
	weights = [i / 100 for i in range(101, 501)]
	mapping_extractor = MappingExtractor()
	mapping_extractor._threshold = 0

	@classmethod
	def test(cls):
		best_weight = 1
		best_t = 1
		best_F1 = 0
		best_F1_p = 0
		best_F1_r = 0
		logging.info('Get all match score')
		all_match_score = cls.get_all_match_score()
		logging.info('Measure performance under different weights')
		for weight in cls.weights:
			all_weighted_score = {}
			for score, index in zip(all_match_score[0], all_match_score[1]):
				matrix = score[0] - (weight - score[0]) * (1 - score[1])
				n_row, n_col = np.shape(matrix)
				if n_row <= n_col:
					result = cls.mapping_extractor._marriage_matching(matrix, return_score=True)
				else:
					result = cls.mapping_extractor._marriage_matching(matrix.T, return_score=True)
					result = list(map(lambda x: (x[1], x[0], x[2]), result))
				for item in result:
					all_weighted_score[(index[0], index[1][item[0]], index[2], index[3][item[1]])] = item[2]
			t, max_F1, p, r = cls.measure(all_weighted_score)
			logging.info(
				'weight: {}, threshold: {}, max_F1: {}, precision: {}, recall: {}\n'.format(weight, t, max_F1, p, r))
			with open(eval_result_path, 'a', encoding='utf-8') as file:
				file.write(
					'weight: {}, threshold: {}, max_F1: {}, precision: {}, recall: {}\n'.format(weight, t, max_F1, p, r))
			if max_F1 > best_F1:
				best_weight = weight
				best_t = t
				best_F1 = max_F1
				best_F1_p = p
				best_F1_r = r
		logging.info(
			'best_weight: {}, best_threshold: {}, best_F1: {}, precision: {}, recall: {}\n'.format(best_weight, best_t, best_F1, best_F1_p, best_F1_r))
		with open(eval_result_path, 'a', encoding='utf-8') as file:
			file.write(
				'best_weight: {}, best_threshold: {}, best_F1: {}, precision: {}, recall: {}\n'.format(best_weight, best_t, best_F1, best_F1_p, best_F1_r))

	@classmethod
	def measure(cls, all_weighted_score):
		sorted_score = sorted(all_weighted_score.items(), key=operator.itemgetter(1), reverse=True)
		len_ = len(sorted_score)
		m = 0
		max_F1 = 0
		t = 1
		p = 0
		r = 0
		for i in range(0, len_):
			v1 = tuple(sorted_score[i][0][:2])
			v2 = tuple(sorted_score[i][0][2:])
			if v1 in cls.all_match[v2] or v2 in cls.all_match[v1]:
				m += 1
			F1 = cls.get_F1(m, i + 1, cls.len_match)
			if F1 > max_F1:
				max_F1 = F1
				t = sorted_score[i][1]
				p = m / (i + 1)
				r = m / cls.len_match
		return t, max_F1, p, r

	@classmethod
	def get_F1(cls, m, pred_1, real_1):
		precision = m / pred_1
		recall = m / real_1
		if precision == 0:
			return 0
		else:
			return 2 * precision * recall / (precision + recall)

	@classmethod
	def set_all_match(cls):
		mapping = defaultdict(list)
		fields_info = Asset._select_fields_info(cls.table_id_list)
		for table in fields_info:
			for field in table['fieldList']:
				mapping[field['logicName']].append((table['id'], field['id']))
		for value in mapping.values():
			len_ = len(value)
			for i in range(len_):
				for j in range(i + 1, len_):
					if value[i][0] != value[j][0]:
						cls.all_match[value[i]].add(value[j])
		assert len(cls.all_match) > 0
		for key, value in cls.all_match.items():
			cls.len_match += len(value)

	@classmethod
	def get_all_match_score(cls):
		all_match_score = []
		all_index = []
		for i in range(0, len(cls.table_id_list)):
			for j in range(i + 1, len(cls.table_id_list)):
				logging.info('Get match score of table {} and {}'.format(cls.table_id_list[i], cls.table_id_list[j]))
				scores, fields_id = cls.convert_to_matrix(cls.table_id_list[i], cls.table_id_list[j])
				all_match_score.append(scores)
				all_index.append((cls.table_id_list[i], fields_id[0], cls.table_id_list[j], fields_id[1]))
		return all_match_score, all_index

	@classmethod
	def convert_to_matrix(cls, table_id_1, table_id_2):
		fields_id_1 = Asset.get_fields_id_by_table_id(table_id_1)
		fields_id_2 = Asset.get_fields_id_by_table_id(table_id_2)
		m_data = np.zeros([len(fields_id_1), len(fields_id_2)])
		m_fields = np.zeros([len(fields_id_1), len(fields_id_2)])
		for index_1, item_1 in enumerate(fields_id_1):
			for index_2, item_2 in enumerate(fields_id_2):
				text_score = cls.mapping_extractor._text_matcher.get_match_score(table_id_1, item_1, table_id_2, item_2)
				instance_score = cls.mapping_extractor._instance_matcher.get_match_score(table_id_1, item_1, table_id_2, item_2)
				m_data[index_1][index_2] = instance_score * instance_score + (1 - instance_score) * text_score
				m_fields[index_1, index_2] = cls.mapping_extractor._semantic_matcher.get_match_score(table_id_1, item_1, table_id_2, item_2)
		return (m_data, m_fields), (fields_id_1, fields_id_2)

	@classmethod
	def clear(cls):
		cls.table_id_list = []
		cls.all_match = defaultdict(set)
		cls.len_match = 0


if __name__ == '__main__':
	for root, dirs, files in os.walk('../simulated_interface/data/data'):
		for file in files:
			Test.table_id_list.append(file.split('.')[0])
	Test.set_all_match()
	Test.test()