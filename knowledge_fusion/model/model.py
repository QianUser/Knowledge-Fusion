import configparser
import os
from abc import ABCMeta, abstractmethod
from collections import Counter

import jieba
import numpy as np
from gensim.models import KeyedVectors
from sentence_transformers import SentenceTransformer, util

from knowledge_fusion.settings import BASE_DIR
from knowledge_fusion.preprocess.idf_counter import IdfCounter
from knowledge_fusion.utils.LRUDict import LRUDict
from knowledge_fusion.utils.utils import Singleton

config = configparser.ConfigParser()
config.read(os.path.join(BASE_DIR, 'config.ini'), encoding='utf-8')


class Model(metaclass=Singleton):
    """
    用于语义相似度计算的模型
    使用LRU字典缓存最近访问数据
    对于调用者屏蔽访问细节
    """

    def __init__(self):
        self._sentence_score_dict = LRUDict(int(config.get('data', 'cache_embedding_size')))

    def predict_score(self, sen1, sen2):
        raise NotImplementedError()

    def clear(self):
        self._sentence_score_dict.clear()


class Word2VecModel(Model):

    def __init__(self):
        super().__init__()
        self.word2vec = KeyedVectors.load_word2vec_format(os.path.join(BASE_DIR, config.get('path', 'word2vec_path')))
        self._idf_counter = IdfCounter()
        self.epsilon = 1e-7

    def predict_score(self, sen1, sen2):
        embedding1, embedding2 = self._sentence_score_dict[sen1], self._sentence_score_dict[sen2]
        if embedding1 is None:
            words1 = jieba.lcut(sen1)
            word_count1 = Counter(words1)
            weight1 = [self._idf_counter.idf_dict[key] * value for key, value in word_count1.items()]
            embedding1 = np.zeros(self.word2vec.vector_size)
            for word, w in zip(words1, weight1):
                if word in self.word2vec.vocab:
                    embedding1 += self.word2vec.get_vector(word) * w
            self._sentence_score_dict[sen1] = embedding1
        if embedding2 is None:
            words2 = jieba.lcut(sen2)
            word_count2 = Counter(words2)
            weight2 = [self._idf_counter.idf_dict[key] * value for key, value in word_count2.items()]
            embedding2 = np.zeros(self.word2vec.vector_size)
            for word, w in zip(words2, weight2):
                if word in self.word2vec.vocab:
                    embedding2 += self.word2vec.get_vector(word) * w
            self._sentence_score_dict[sen2] = embedding2
        return self._cosine_similarity(embedding1, embedding2)

    def _cosine_similarity(self, vec1, vec2):
        return np.dot(vec1, vec2) / (np.linalg.norm(vec1) * np.linalg.norm(vec2) + self.epsilon)


class SentenceTransformerModel(Model):

    def __init__(self):
        super().__init__()
        self.model = SentenceTransformer(os.path.join(BASE_DIR, config.get('path', 'tuned_sentence_transformer_model_path')))

    def predict_score(self, sen1, sen2):
        embedding1, embedding2 = self._sentence_score_dict[sen1], self._sentence_score_dict[sen2]
        if embedding1 is None:
            embedding1 = self.model.encode([sen1], convert_to_tensor=False)
            self._sentence_score_dict[sen1] = embedding1
        if embedding2 is None:
            embedding2 = self.model.encode([sen2], convert_to_tensor=False)
            self._sentence_score_dict[sen2] = embedding2
        return util.pytorch_cos_sim(embedding1, embedding2)[0][0]
