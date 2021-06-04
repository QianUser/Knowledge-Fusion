import configparser
import logging
import os
import traceback

from celery import Celery
from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt

from knowledge_fusion.interface.mapping import match_all_interrelation, match_one2all_interrelation, match_one2one_interrelation, match_some2all_interrelation
from knowledge_fusion.settings import BASE_DIR, config_dir
from knowledge_fusion.interface.task_db import TaskDB
from knowledge_fusion.utils.utils import get_cur_time_rep

config = configparser.ConfigParser()
config.read(config_dir, encoding='utf-8')
logger = logging.getLogger('server_log')
task_db = TaskDB(os.path.join(BASE_DIR, config.get('path', 'task_db_path')))
task_db.create_table('process', task_id='varchar(100)', pid='varchar(100)', create_time='date')
app = Celery('source', broker=config.get('celery', 'broker'), backend=config.get('celery', 'backend'))


def response(**kwargs):
    return JsonResponse({str(key): value for key, value in kwargs.items()}, json_dumps_params={'ensure_ascii': False}, safe=False)


@csrf_exempt
def test_api(request):
    if request.method not in ['GET', 'POST']:
        return response(message='仅支持GET或POST访问')
    return response(code=200, msg='接口运行正常')


# ! 貌似总是返回1或-1
@csrf_exempt
def task_state(request):
    """
    任务状态查询接口
    """
    logger.info('**********Access task_state api.**********')
    if request.method != 'POST':
        return response(message='仅支持POST访问')
    try:
        task_id = request.POST.get('taskId')
        logger.info('The request taskId: %s', task_id)
    except Exception:
        logger.error('Error parsing parameters: %s')
        return response(message='参数错误')
    try:
        pid = task_db.select_pid_by_task_id(task_id)
        if pid is None:
            return response(taskId=int(task_id), status=-1, message="taskId不存在")
        result = app.AsyncResult(pid)
        logger.info(result.state + '-------------------------')
        status = 1 if result.state == 'SUCCESS' else -1 if result.state == 'FAILURE' or result.state == 'REVOKED' else 0
        info = str(result.info).replace("'", '')
        if isinstance(result.info, Exception):
            return response(taskId=int(task_id), status=status, message=result.info.__class__.__name__ + ': ' + info)
        else:
            return response(taskId=int(task_id),status=status, message=info)
    except Exception:
        logger.error(traceback.format_exc())
        return response(taskId=task_id, status=-1, message='查询状态时发生错误')


@csrf_exempt
def delete_cache(request):
    """
    不太清楚这个功能想要做什么
    不用调用该接口，每次访问接口结束即删除所有缓存的表数据，否则表变化后查询到的是旧数据
    如果调用该接口，则清空数据库TaskDB
    """
    task_db.clear()
    return response(status=True)


@csrf_exempt
def all_interrelation(request):
    """
    所有资产相互间的关系运算
    """
    logger.info('**********Access all_interrelation api.**********')
    if request.method != 'POST':
        return response(state=False, message='仅支持POST访问')
    try:
        # data = json.loads(request.body)
        # task_id = str(data['taskId'])
        # table_id_list = data['tableIdList']
        task_id = request.POST.get('taskId')
        table_id_list = request.POST.getlist('tableIdList')
        logger.info('Request taskId: %s', task_id)
    except Exception:
        logger.error('Error parsing parameters')
        return response(state=False, message='参数错误')
    try:
        result = match_all_interrelation.delay(task_id, table_id_list)
        pid = result.id
        task_db.insert(task_id, pid, get_cur_time_rep())
        return response(state=True)
    except Exception:
        logger.error('Error when matching or inserting info into database')
        return response(state=False)


@csrf_exempt
def one2all_interrelation(request):
    """
    一个资产和现有所有资产的关系运算
    """
    logger.info('**********Access one2all_interrelation api.**********')
    if request.method != 'POST':
        return response(message='仅支持POST访问')
    try:
        task_id = request.POST.get('taskId')
        table_id_src = request.POST.get('tableIdSrc')
        table_id_list_dest = request.POST.getlist('tableIdListDest')
        logger.info('Request taskId: %s', task_id)
    except Exception:
        logger.error('Error parsing parameters: %s')
        return response(state=False, message='参数错误')
    try:
        result = match_one2all_interrelation.delay(task_id, table_id_src, table_id_list_dest)
        pid = result.id
        task_db.insert(task_id, pid, get_cur_time_rep())
        return response(state=True, table_id_list_dest=table_id_list_dest)
    except Exception:
        logger.error('Error when matching or inserting info into database')
        return response(state=False)


@csrf_exempt
def one2one_interrelation(request):
    """
    两个指定资产进行关系运算
    """
    logger.info('**********Access one2one_interrelation api.**********')
    if request.method != 'POST':
        return response(message='仅支持POST访问')
    try:
        task_id = request.POST.get('taskId')
        table_id_src = request.POST.get('tableIdSrc')
        table_id_dest = request.POST.get('tableIdDest')
        logger.info('Request taskId: %s', task_id)
    except Exception:
        logger.error('Error parsing parameters: %s')
        return response(message='参数错误')
    try:
        result = match_one2one_interrelation.delay(task_id, table_id_src, table_id_dest)
        pid = result.id
        task_db.insert(task_id, pid, get_cur_time_rep())
        return response(state=True)
    except Exception:
        logger.error('Error when matching or inserting info into database')
        return response(state=False)


@csrf_exempt
def some2all_interrelation(request):
    """
    一批资产和现有所有资产的关系运算
    """
    logger.info('**********Access some2all_interrelation api.**********')
    if request.method != 'POST':
        return response(message='仅支持POST访问')
    try:
        task_id = request.POST.get('taskId')
        table_id_list_src = request.POST.getlist('tableIdListSrc')
        table_id_list_dest = request.POST.getlist('tableIdListDest')
        logger.info('Request taskId: %s', task_id)
    except Exception:
        logger.error('Error parsing parameters: %s')
        return response(message='参数错误')
    try:
        result = match_some2all_interrelation.delay(task_id, table_id_list_src, table_id_list_dest)
        pid = result.id
        task_db.insert(task_id, pid, get_cur_time_rep())
        return response(state=True)
    except Exception:
        logger.error('Error when matching or inserting info into database')
        return response(state=False)
