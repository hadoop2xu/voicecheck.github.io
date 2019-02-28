# coding=utf-8
"""
-------------------------------------------------
 Author      :   hadoop.xu
 Date        ：  2018/8/08
 Description :  将chat和语音打分后的结果跟其他表连接，查询出相关字段后重新插入另一张表中，供查询接口使用
-------------------------------------------------
"""

import argparse
import datetime
import os
import sys

sys.path.append('.')
sys.path.append('..')
sys.path.append('../..')
from utils import log
from utils import dbops

logger = log.get_logger()


def parse_args():
    """
    命令行解析
    :return parser: 返回参数解析器对象
    """
    parser = argparse.ArgumentParser()
    parser.add_argument('--prediction_date', type=str, default=(datetime.datetime.now() - datetime.timedelta(days=1)).strftime("%Y-%m-%d"))
    return parser.parse_args()


if __name__ == '__main__':
    args = parse_args()
    cur_path = os.getcwd()
    join_insert_sql = os.path.dirname(os.path.dirname(cur_path)) + os.sep + 'sql' + os.sep + 'qualitycheck' + os.sep + 'join_insert.sql'

    if not os.path.exists(join_insert_sql):
        logger.error('The sql of join insert not exist！')
        sys.exit(0)
    insert_mysql_object = dbops.Mysql('f_fuwu_dm')
    insert_sql_content = dbops.get_sql_content(join_insert_sql)
    insert_sql_content = insert_sql_content % (args.prediction_date, args.prediction_date, args.prediction_date, args.prediction_date)
    try:
        insert_mysql_object.execute(insert_sql_content)
    except Exception as e:
        print(e)
    finally:
        insert_mysql_object.close()
