import csv
import os

from knowledge_fusion.settings import BASE_DIR
from knowledge_fusion.utils.utils import write_json, read_json


class AddressHandler:

	base_dir = os.path.join(BASE_DIR, 'knowledge_fusion', 'matcher', 'data')
	feature_dir = os.path.join(base_dir, 'instance_feature', 'address')
	country_corpus_path = os.path.join(base_dir, 'geo-master', 'country.csv')
	countries_path = os.path.join(feature_dir, 'countries_chinese')
	china_zone_corpus_path = os.path.join(base_dir, 'city-master', 'lib', 'citydata.json')
	china_zone_path = os.path.join(feature_dir, 'china_zone')

	@classmethod
	def construct_countries(cls, encoding='utf-8'):
		countries_chinese = set()
		countries_chinese_all = set()
		countries_english = set()
		countries_english_all = set()
		with open(cls.country_corpus_path, encoding=encoding) as file:
			data = csv.reader(file)
			next(data)
			for row in data:
				countries_chinese.add(row[6])
				countries_chinese_all.add(row[7])
				countries_english.add(row[2].lower())
				countries_english_all.add(row[3].lower())
		return countries_chinese, countries_chinese_all, countries_english, countries_english_all

	@classmethod
	def store_countries(cls):
		countries = cls.construct_countries()
		write_json(list(map(lambda x: list(x), countries)), cls.countries_path)

	@classmethod
	def construct_china_zone(cls, encoding='utf-8'):
		special = ['北京市', '上海市', '天津市', '重庆市', '香港特别行政区', '澳门特别行政区']
		province_suffix = ['省', '特别行政区', '回族自治区', '维吾尔族自治区', '壮族自治区', '自治区', '市']  # 不能调换顺序
		city_suffix = ['市']
		provinces = set()
		cities = set()
		counties = set()
		data = read_json(cls.china_zone_corpus_path, encoding=encoding)
		for province in data:
			if province['name'] == '海外':
				continue
			if province['name'] in special:
				provinces.add(province['name'])
				cities.add(province['name'])
				for county in province['children']:
					counties.add(county['name'])
			else:
				provinces.add(province['name'])
				for city in province['children']:
					cities.add(city['name'])
					for county in city['children']:
						counties.add(county['name'])

		set_ = set()
		for item in provinces:
			for suffix in province_suffix:
				if item.endswith(suffix):
					set_.add(item[:-len(suffix)])
					break
		provinces.update(set_)

		set_ = set()
		for item in cities:
			for suffix in city_suffix:
				if item.endswith(suffix):
					set_.add(item[:-len(suffix)])
					break
		cities.update(set_)

		return provinces, cities, counties

	@classmethod
	def store_china_zone(cls):
		zone = cls.construct_china_zone()
		write_json(list(map(lambda x: list(x), zone)), cls.china_zone_path)

	@classmethod
	def get_countries(cls):
		if not os.path.exists(cls.countries_path):
			cls.store_countries()
		result = read_json(cls.countries_path)
		return list(map(lambda x: set(x), result))

	@classmethod
	def get_china_zone(cls):
		if not os.path.exists(cls.china_zone_path):
			cls.store_china_zone()
		result = read_json(cls.china_zone_path)
		return list(map(lambda x: set(x), result))


if __name__ == '__main__':
	x = AddressHandler.get_china_zone()
	for i in x[1]:
		print(i)