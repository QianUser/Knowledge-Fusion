import configparser
import logging
import sqlite3

from knowledge_fusion.settings import config_dir

logger = logging.getLogger('database_log')
config = configparser.ConfigParser()
config.read(config_dir, encoding='utf-8')


class TaskDB:
    def __init__(self, con_str):
        self._con_str = con_str
        self._con = None
        self.new_state(ClosedTaskDB)
        self.open()

    def new_state(self, new_state):
        self.__class__ = new_state

    def open(self):
        """
        连接数据库
        """
        raise NotImplementedError()

    def create_table(self, table='process', **kwargs):
        """
        创建表格，table为表名，kwargs为所有字段及其类型
        """
        raise NotImplementedError()

    def select_pid_by_task_id(self, task_id, table='process'):
        """
        根据任务ID查询进程ID
        """
        raise NotImplementedError()

    def insert(self, *args, table='process', **kwargs):
        """
        插入数据
        """
        raise NotImplementedError()

    def delete_by_task_id(self, task_id, table='process'):
        """
        根据任务ID删除进程ID
        """
        raise NotImplementedError()

    def clear(self, table='process'):
        raise NotImplementedError()

    def close(self):
        """
        关闭数据库连接
        """
        raise NotImplementedError()


class OpenTaskDB(TaskDB):

    def open(self):
        raise RuntimeError('Database already open')

    def create_table(self, table='process', **kwargs):
        try:
            logger.info('Try to create table {}'.format(table))
            cursor = self._con.cursor()
            # 确保已创建的表格的正确性，否则会被错误使用
            sql = 'create table if not exists ' + table + '(' + 'id integer primary key autoincrement'
            sql = sql + ''.join([', ' + key + ' ' + value for key, value in kwargs.items()] + [');'])
            cursor.execute(sql)
            cursor.close()
        except Exception:
            logger.error('Create table {} failure'.format(table))

    def select_pid_by_task_id(self, task_id, table='process'):
        try:
            logger.info('Query pid by task_id {}'.format(task_id))
            cursor = self._con.cursor()
            sql = "select pid from '" + table + "' where task_id='" + task_id + "';"
            cursor.execute(sql)
            r = cursor.fetchone()
            cursor.close()
            return None if r is None or len(r) == 0 else r[0]
        except Exception:
            logger.error('Query pid by task_id {} failure'.format(table, task_id))

    def insert(self, *args, table='process', **kwargs):
        if len(args) != 0 and len(kwargs) == 0:
            logger.info('Insert {} to {}'.format(args, table))
            self._insert_with_args(table, *args)
        elif len(args) == 0 and len(kwargs) != 0:
            logger.info('Insert {} to {}'.format(kwargs, table))
            self._insert_with_kwargs(table, **kwargs)
        else:
            logger.error('Wrong parameter. table: {}, parameter: {}, {}'.format(table, args, kwargs))

    def _insert_with_args(self, table='process', *args):
        if len(args) == 0:
            return
        placeholder = '?, ' * (len(args) - 1) + '?'
        try:
            cursor = self._con.cursor()
            sql = 'insert into ' + table + ' values (NULL, ' + placeholder + ');'
            cursor.execute(sql, args)
            cursor.close()
            self._con.commit()
        except Exception:
            logger.error('Insert failure. table: {}, parameter: {}'.format(table, args))

    def _insert_with_kwargs(self, table, **kwargs):
        if len(kwargs) == 0:
            return
        fields = ', '.join(kwargs.keys())
        placeholder = '?, ' * (len(kwargs) - 1) + '?'
        try:
            cursor = self._con.cursor()
            sql = 'insert into ' + table + ' (' + fields + ') values (' + placeholder + ');'
            cursor.execute(sql, tuple(kwargs.values()))
            cursor.close()
            self._con.commit()
        except Exception:
            logger.error('Insert failure. table: {}, parameter: {}'.format(table, kwargs))

    def delete_by_task_id(self, task_id, table='process'):
        try:
            logger.info('Delete task_id {}'.format(task_id))
            cursor = self._con.cursor()
            sql = "delete from '" + table + "' where task_id='" + task_id + "';"
            cursor.execute(sql)
            cursor.close()
            self._con.commit()
        except Exception:
            logger.error('Delete task_id {} failure'.format(task_id))

    def clear(self, table='process'):
        try:
            logger.info('Clear table {}'.format(table))
            cursor = self._con.cursor()
            cursor.execute("delete from '" + table + "';")
            cursor.execute("update sqlite_sequence set seq=0 where name='" + table + "';")
            cursor.close()
            self._con.commit()
            self._con.execute('vacuum;')  # 返回磁盘空间
        except Exception:
            logger.error('Clear table {} failure'.format(table))

    def close(self):
        try:
            logger.info('Close connection')
            self._con.close()
        except Exception:
            logger.error('Close connection failure')
            return
        self.new_state(ClosedTaskDB)


class ClosedTaskDB(TaskDB):

    def open(self):
        try:
            logger.info('Open connection')
            self._con = sqlite3.connect(self._con_str, check_same_thread = False)
        except Exception:
            logger.error('Open connection failure')
            return
        self.new_state(OpenTaskDB)

    def create_table(self, table='process', **kwargs):
        raise RuntimeError('Database not open')

    def select_pid_by_task_id(self, task_id, table='process'):
        raise RuntimeError('Database not open')

    def insert(self, *args, table='process', **kwargs):
        raise RuntimeError('Database not open')

    def delete_by_task_id(self, task_id, table='process'):
        raise RuntimeError('Database not open')

    def clear(self, table='process'):
        raise RuntimeError('Database not open')

    def close(self):
        raise RuntimeError('Database already closed')
