import configparser
import json
import os.path
import uuid

from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt

from knowledge_fusion.settings import config_dir, BASE_DIR

config = configparser.ConfigParser()
config.read(config_dir, encoding='utf-8')
fields_path = os.path.join(BASE_DIR, config.get('path', 'simulated_fields_path'))
data_path = os.path.join(BASE_DIR, config.get('path', 'simulated_data_path'))
result_path = os.path.join(BASE_DIR, config.get('path', 'simulated_result_path'))

if not os.path.exists(result_path):
    os.mkdir(result_path)

with open(fields_path, encoding='utf-8') as file:
    all_fields = json.load(file)


def response(**kwargs):
    return JsonResponse({str(key): value for key, value in kwargs.items()}, json_dumps_params={'ensure_ascii': False}, safe=False)


@csrf_exempt
def get_fields_info(request):
    """
    模拟的/api/asset/fields接口
    """
    if request.method != 'POST':
        return response(message='仅支持POST访问')
    try:
        asset_ids = request.POST.getlist('assetIds')
    except Exception:
        return response(code='1000002', message='参数错误', data=[])
    try:
        result = []
        for asset_id in asset_ids:
            result.append(all_fields[asset_id])
        return response(code='1000000', message='SUCCESS', data=result, asset_ids=asset_ids)
    except KeyError:
        return response(code='4200004', message='无法获取到表格信息', data=[])
    except Exception:
        return response(code='1000002', message='未知错误', data=[])


def get_data_info(request):
    """
    模拟的/api/asset/data接口
    """
    if request.method != 'GET':
        return response(message='仅支持GET访问')
    try:
        asset_id = request.GET.get('asset_id')
    except Exception:
        return response(code='1000002', message='参数错误', data={})
    try:
        limit = int(request.GET.get('limit', '-1'))
        with open(os.path.join(data_path, asset_id + '.json')) as file:
            result = json.load(file)
            del result['asset_id']
            if limit > 0:
                result['assetData'] = result['assetData'][:limit]
            return response(code='1000000', message='SUCCESS', data=result)
    except FileNotFoundError:
        return response(code='4200004', message='无法获取到表格数据', data={})
    except Exception:
        return response(code='1000002', message='未知错误', data={})


@csrf_exempt
def get_result(request):
    if request.method != 'POST':
        return response(message='仅支持POST访问')
    try:
        task_id = request.POST.get('taskId')
        status = request.POST.get('status')
        page_num = request.POST.get('pageNum')
        page_size = request.POST.get('pageSize')
        data = json.loads(request.POST.get('data'))
    except Exception:
        return response(code='1000002', message='参数错误', data='false')
    try:
        with open(os.path.join(result_path, str(uuid.uuid1()) + '.json'), 'w') as file:
            json.dump({'taskId': task_id, 'status': status, 'pageNum': page_num, 'pageSize': page_size, 'data': data}, file)
        return response(code='1000000', message='SUCCESS', data='true')
    except Exception:
        return response(code='4200001', message='文件写入错误', data='false')
