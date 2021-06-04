from knowledge_fusion.interface.asset import Asset


# ! 如果没有物理名或物理名为空怎么样？
class SemanticMatcher:
    """
    语义匹配器
    """

    def __init__(self, model):
        super().__init__()
        self.model = model()

    def get_match_score(self, table_id_1, field_id_1, table_id_2, field_id_2):
        text_1 = Asset.get_field_logic_name_by_id(table_id_1, field_id_1)
        text_2 = Asset.get_field_logic_name_by_id(table_id_2, field_id_2)
        score = self.model.predict_score(text_1, text_2)
        return score
