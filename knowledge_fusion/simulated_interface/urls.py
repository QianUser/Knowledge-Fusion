from django.urls import path

from knowledge_fusion.simulated_interface import interface

urlpatterns = [
    path('api/asset/fields/', interface.get_fields_info, name='get_fields_info'),
    path('api/asset/data/', interface.get_data_info, name='get_data_info'),
    path('api/asset_relation/related_fields/', interface.get_result, name='get_result')
]
