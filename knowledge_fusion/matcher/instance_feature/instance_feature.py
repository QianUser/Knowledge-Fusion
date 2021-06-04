import re

import jieba

from knowledge_fusion.matcher.instance_feature.address_handler import AddressHandler
from knowledge_fusion.matcher.instance_feature.name_handler import NameHandler


def feature_type(**kwargs):

    def decorate(cls):
        """
        type：文本类型，以列表表示，按照列表先后顺序分配。
        对于某个字段的所有数据项，其类型只能是一类，即列表最后的非零项。
        例如，字段体重记录一个人的体重信息。
        若一个人的体重为56（kg），其可能代表整数，也可能代表小数（小数部分恰好为0，故而省略）。
        令type=['整数', '小数']，则其优先匹配整数。
        若所有体重均没有小数部分，则最终type='整数'
        但是若存在另一个体重值55.2（kg），那么'小数'类型就非零，则该字段代表小数而不是整数。

        category：文本的范畴，范畴可以有多个，以列表表示，无顺序。
        若一个文本匹配一个范畴的feature，则其也可能匹配该范畴内其他的feature，但是如果某个feature没有该范畴，则不匹配该feature。

        label：文本标签，决定feature的匹配顺序，无顺序。
        给定一个字段，建立字段与文本标签的相似度顺序。给定数据项，按照相似度从大到小匹配feature。
        当匹配到一个feature，再匹配所有该feature所属范畴内的所有feature。

        weight：该feature的重要程度
        """
        params = {'type', 'category', 'label', 'weight'}
        for key, value in kwargs.items():
            if key in {'type', 'category', 'label'}:
                if type(value) != list or len(value) == 0:
                    raise Exception('参数必须为至少含有一个元素的列表')
                for item in value:
                    if type(item) != str:
                        raise Exception('参数列表元素必须为字符串类型')
            elif key == 'weight':
                if type(value) != int:
                    raise Exception('参数必须为整型')
            setattr(cls, key, value)
            try:
                params.remove(key)
            except KeyError:
                raise Exception('多余的参数')
        if len(params) != 0:
            raise Exception('缺少参数')
        setattr(cls, 'feature_type', True)  # 一个蹩脚的设计，只是为了遍历所有被feature_type装饰的Feature类
        return cls

    return decorate


class DefaultType:
    # """
    # 不为默认值的字段。
    # 数据库中某个字段可以为空，此时返回默认空值。但是无法自动判断某个字段是否为空，因为对于某些类型，默认值与非默认值的集合存在交叉，此时默认没有默认值
    # 假设默认值为空串，则对于整型空串肯定是一个默认值，其不在_no_default_type中；对于备注，可以为空串，其在_no_default_type中。
    # """
    # no_default_type = set()

    @classmethod
    def match(cls, text):
        return len(text.strip()) == 0


# ! 实现有问题而蜕化为原始操作
class Feature:
    """
    Feature父类，起两个作用：便于获取所有的Feature子类以找到所有Feature；提供公共操作
    子类调用Feature方法前，首先判断结果是否缓存，如果是，则直接返回结果，否则缓存结果
    最后使用clear清除缓存
    但是实现有问题，貌似在clear与其他方法调用期间，缓存被修改了，原因不明
    因此蜕化为原始操作，这样效率会受到影响
    """

    __word__ = None

    @classmethod
    def cut(cls, text):
        # if cls.__word__ is None:
        #     cls.__word__ = jieba.lcut(text)
        # return cls.__word__
        return jieba.lcut(text)

    @classmethod
    def clear(cls):
        # cls.__word__ = None
        pass


@feature_type(type=['整数', '小数'], category=['整数'], label=['整数', '编号', '号码', '编码'], weight=1)
class IntegerFeature(Feature):
    _integer_pattern = re.compile(r'^[+-]?\d+$')

    @classmethod
    def match(cls, text):
        return cls._integer_pattern.match(text.strip()) is not None


@feature_type(type=['小数'], category=['小数'], label=['小数'], weight=1)
class DecimalFeature(Feature):
    _decimal_pattern = re.compile(r'^[+-]?(\d+\.\d*)|(\d*\.\d+)([eE][+-]?\d+)?$')

    @classmethod
    def match(cls, text):
        return cls._decimal_pattern.match(text.strip()) is not None


@feature_type(type=['布尔值'], category=['布尔值'], label=['是否', '有无'], weight=1)
class BoolFeature(Feature):

    @classmethod
    def match(cls, text):
        return text.strip().lower() in {'是', '否', 'true', 'false', 'yes', 'no', '有', '无'}


@feature_type(type=['公民身份号码'], category=['整数'], label=['公民身份号码', '身份证', '身份号码', '身份证号码'], weight=3)
class CitizenshipNumberFeature(Feature):
    _citizenship_number_pattern = re.compile(
        r'^[1-9]\d{5}(18|19|20)\d{2}((0[1-9])|(10|11|12))(([0-2][1-9])|10|20|30|31)\d{3}[0-9Xx]$')

    @classmethod
    def match(cls, text):
        return cls._citizenship_number_pattern.match(text.strip()) is not None


@feature_type(type=['时间'], category=['时间'], label=['时间', '时刻'], weight=3)
class TimeFeature(Feature):
    _time_pattern = re.compile(r'^(([01]?[0-9])|(20)|(21)|(22)|(23)):[0-5][0-9](:[0-5][0-9])?$')

    @classmethod
    def match(cls, text):
        return cls._time_pattern.match(text.strip()) is not None

    @classmethod
    def pattern(cls):
        return cls._time_pattern.pattern


@feature_type(type=['日期'], category=['日期'], label=['日期'], weight=3)
class DateFeature(Feature):
    _date_pattern = re.compile(r'^\d{4}\s*[-/\\.年]\s*((0?[1-9])|10|11|12)\s*[-/\\.月]\s*(([0-2]?[1-9])|10|20|30|31)日?$')

    @classmethod
    def match(cls, text):
        return cls._date_pattern.match(text.strip()) is not None

    @classmethod
    def pattern(cls):
        return cls._date_pattern.pattern


@feature_type(type=['星期'], category=['星期'], label=['星期', '周'], weight=3)
class WeekFeature(Feature):
    _week_pattern = re.compile(r'^(星期|周)[一二三四五六日]$')

    @classmethod
    def match(cls, text):
        return cls._week_pattern.match(text)

    @classmethod
    def pattern(cls):
        return cls._week_pattern.pattern


@feature_type(type=['时间'], category=['日期 时间'], label=['时间', '时刻'], weight=3)
class CombinedTimeFeature(Feature):
    _time_feature = re.compile(TimeFeature.pattern()[1:-1])
    _date_feature = re.compile(DateFeature.pattern()[1:-1])
    _week_feature = re.compile(WeekFeature.pattern()[1:-1])

    @classmethod
    def match(cls, text):
        if cls._time_feature.search(text) and (cls._week_feature.search(text) or cls._date_feature.search(text)):
            return True
        return False


@feature_type(type=['日期'], category=['日期 星期'], label=['星期', '日期'], weight=3)
class CombinedDateFeature(Feature):
    _time_feature = re.compile(TimeFeature.pattern()[1:-1])
    _date_feature = re.compile(DateFeature.pattern()[1:-1])
    _week_feature = re.compile(WeekFeature.pattern()[1:-1])

    @classmethod
    def match(cls, text):
        if cls._week_feature.search(text) and cls._date_feature.search(text) and not cls._time_feature.search(text):
            return True
        return False


@feature_type(type=['汉语姓名'], category=['字符串'], label=['姓名', '名字'], weight=3)
class ChineseNameFeature(Feature):
    chinese_first_names, chinese_second_names, chinese_names = NameHandler.get_names('chinese')
    max_first_names = max([len(first_name) for first_name in chinese_first_names])

    @classmethod
    def match(cls, text):
        if text in cls.chinese_names:
            return True
        for i in range(1, min(cls.max_first_names, len(text))):
            if text[:i] in cls.chinese_first_names and text[i:] in cls.chinese_second_names:
                return True
        return False


@feature_type(type=['英语姓名'], category=['字符串'], label=['姓名', '名字'], weight=3)
class EnglishNameFeature(Feature):
    english_names = NameHandler.get_names('english')

    @classmethod
    def match(cls, text):
        names = text.split()
        if len(names) < 2:
            return False
        for name in names:
            if name.lower() not in cls.english_names:
                return False
        return True


@feature_type(type=['联系方式'], category=['整数'], label=['联系方式', '手机号码', '电话号码'], weight=3)
class PhoneNumberFeature(Feature):
    # 只考虑通常的国内11位手机号码
    # _phone_number_pattern = re.compile(r'^(\+86)?\s*1([358][0-9]|4[579]|66|7[0135678]|9[89])[0-9]{8}$')
    _phone_number_pattern = re.compile(r'^1(?:3\d|4[4-9]|5[0-35-9]|6[67]|7[013-8]|8\d|9\d)\d{8}$')

    @classmethod
    def match(cls, text):
        return cls._phone_number_pattern.match(text.strip()) is not None


@feature_type(type=['性别'], category=['性别'], label=['性别'], weight=3)
class GenderFeature(Feature):

    @classmethod
    def match(cls, text):
        return text.strip().lower() in ['男', '女', '女改男', '男改女']

# ! 字符串范围太大了，应该缩小

@feature_type(type=['国家'], category=['字符串'], label=['国家', '国籍'], weight=2)
class CountryFeature(Feature):
    _countries = AddressHandler.get_countries()

    @classmethod
    def match(cls, text):
        words = cls.cut(text)
        for set_ in cls._countries:
            for word in words:
                if word.lower() in set_:
                    return True
        return False


class ZoneFeature(Feature):
    _provinces, _cities, _counties = AddressHandler.get_china_zone()


@feature_type(type=['省级行政区'], category=['字符串'], label=['省', '省级行政区', '省份'], weight=2)
class ProvinceFeature(ZoneFeature):

    @classmethod
    def match(cls, text):
        words = cls.cut(text)
        for word in words:
            if word in cls._provinces:
                return True
        return False


@feature_type(type=['城市'], category=['字符串'], label=['城市'], weight=2)
class CityFeature(ZoneFeature):

    @classmethod
    def match(cls, text):
        words = cls.cut(text)
        for word in words:
            if word in cls._cities:
                return True
        return False


@feature_type(type=['县区'], category=['字符串'], label=['县', '区', '县区', '县级市'], weight=3)
class CountyFeature(ZoneFeature):

    @classmethod
    def match(cls, text):
        words = cls.cut(text)
        for word in words:
            if word in cls._counties:
                return True
        return False

