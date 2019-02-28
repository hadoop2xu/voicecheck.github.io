# coding=utf-8
"""
-------------------------------------------------
 Author      :   hadoop.xu
 Date        ：  2018/8/08
 Description :   工单流程质检
-------------------------------------------------
"""

import os
import sys
import argparse
import datetime
import json

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
    parser.add_argument('--flowcheck_date', type=str, default=(datetime.datetime.now() - datetime.timedelta(days=1)).strftime("%Y-%m-%d"))
    parser.add_argument('--begin_date', type=str, default=(datetime.datetime.now() - datetime.timedelta(days=1)).strftime("%Y-%m-%d"))
    parser.add_argument('--end_date', type=str, default=(datetime.datetime.now()).strftime("%Y-%m-%d"))
    parser.add_argument('--is_keyword', type=bool, default=True)
    return parser.parse_args()


if __name__ == '__main__':
    args = parse_args()
    cur_path = os.getcwd()
    issue_info_sql = os.path.dirname(os.path.dirname(cur_path)) + os.sep + 'sql' + os.sep + 'qualitycheck' + os.sep + 'query_flowcheck_gongdan_info.sql'
    filter_item_sql = os.path.dirname(os.path.dirname(cur_path)) + os.sep + 'sql' + os.sep + 'qualitycheck' + os.sep + 'query_flowcheck_filter_item.sql'
    chat_speech_sql = os.path.dirname(os.path.dirname(cur_path)) + os.sep + 'sql' + os.sep + 'qualitycheck' + os.sep + 'query_flowcheck_chat_speech.sql'
    flowcheck_result_insert_sql = os.path.dirname(os.path.dirname(cur_path)) + os.sep + 'sql' + os.sep + 'qualitycheck' + os.sep + 'flowcheck_result_insert.sql'
    flowcheck_join_insert_sql = os.path.dirname(os.path.dirname(cur_path)) + os.sep + 'sql' + os.sep + 'qualitycheck' + os.sep + 'flowcheck_join_insert.sql'

    # 读取sql内容
    issue_info_sql_content = dbops.get_sql_content(issue_info_sql)
    filter_item_sql_content = dbops.get_sql_content(filter_item_sql)
    chat_speech_sql_content = dbops.get_sql_content(chat_speech_sql)

    # 从数据库中取出过滤条件
    query_mysql_object = dbops.Mysql("f_fuwu_dw")
    filter_item_records = query_mysql_object.query(filter_item_sql_content)
    query_mysql_object.close()

    filter_items = {}
    for record in filter_item_records:
        size = len(record)
        if size != 3:
            logger.error("The data format is err!")
            sys.exit(0)
        try:
            question_type = record[0]
            caller_threshold = float(record[1])
            key_words = record[2].split("#")
            filter_items[question_type] = (caller_threshold, key_words)
        except Exception as e:
            print(e)

    # 从数据库中查询给定过滤条件的工单信息
    flowcheck_date = args.flowcheck_date

    question_types = ""
    for item in filter_items.keys():  # 拼接查询条件
        question_types = question_types + "\'" + item + "\',"
    question_types = question_types.strip(',').strip('\'')

    issue_info_sql_content = issue_info_sql_content.replace("flowcheck_date", flowcheck_date).replace("question_type", question_types)
    query_mysql_object = dbops.Mysql("f_fuwu_dw")
    issue_info_records = query_mysql_object.query(issue_info_sql_content)
    query_mysql_object.close()

    issue_infos = []
    order_question_type = {}  # 订单与问题类型映射

    for record in issue_info_records:
        size = len(record)
        if size != 5:
            logger.error("The data format is err!")
            sys.exit(0)

        try:
            v_date = record[0].strftime("%Y-%m-%d")
            issue_no = record[1]
            order_no = record[2]
            problem_config_no_3 = record[3]
            caller_num = int(record[4])
            order_question_type[order_no] = problem_config_no_3
            # 根据外呼阈值过滤工单
            threshold = filter_items[problem_config_no_3][0]
            if caller_num >= threshold:
                issue_infos.append((v_date, issue_no, order_no, caller_num))
        except Exception as e:
            print(e)

    # 从数据库中查询语音和chat记录，并根据过滤条件过滤
    order_nos = ""
    for e in issue_infos:  # 拼接查询条件
        order_nos = order_nos + "\'" + e[2] + "\',"
    order_nos = order_nos.strip(',').strip('\'')

    chat_speech_sql_content = chat_speech_sql_content.replace("flowcheck_date", v_date).replace("flowcheck_order_no", order_nos).replace("begin_date", args.begin_date).replace("end_date", args.end_date)
    query_mysql_object = dbops.Mysql("f_fuwu_dw")
    records = query_mysql_object.query(chat_speech_sql_content)
    query_mysql_object.close()

    for record in records:
        size = len(record)
        if size != 5:
            logger.error("The data format is err!")
            sys.exit(0)

        v_date = record[0].strftime('%Y-%m-%d')  # 计算日期
        source = str(record[1])  # 来源，1代表chat，2代表语音
        order_no = record[2]  # 订单号
        task_no = record[3]  # 语音任务号,
        result = record[4]  # 通话语义

        # 过滤关键字
        key_word = filter_items[order_question_type[order_no]][1]
        try:
            content = json.loads(result)  # 通话语义
        except Exception as e:
            content = None
            print(e)
        if content:
            sentences = None
            if source == "1":
                sentences = content
            elif source == "2":
                sentences = content['sentenceListData']
            else:
                pass
            if sentences:
                for sentence in sentences:
                    text = None
                    if source == "1":
                        text = sentence['digest']
                    elif source == "2":
                        text = sentence['text']
                    else:
                        pass
                    if text:
                        flag = False

                        if args.is_keyword:  # 有关键词过滤
                            for kw in key_word:
                                if kw in text:  # 包含关键字
                                    flag = True
                                    # 结果插入数据库
                                    insert_mysql_object = dbops.Mysql("f_fuwu_dm")
                                    flowcheck_result_insert_content = dbops.get_sql_content(flowcheck_result_insert_sql)
                                    flowcheck_result_insert_content = flowcheck_result_insert_content % (v_date, int(source), task_no, kw, text)
                                    # logger.info(flowcheck_result_insert_content)
                                    try:
                                        insert_mysql_object.execute(flowcheck_result_insert_content)
                                    except Exception as e:
                                        print(e)
                                    finally:
                                        insert_mysql_object.close()
                                    break
                        else:  # 无关键词过滤
                            flag = True
                            # 结果插入数据库
                            insert_mysql_object = dbops.Mysql("f_fuwu_dm")
                            flowcheck_result_insert_content = dbops.get_sql_content(flowcheck_result_insert_sql)
                            flowcheck_result_insert_content = flowcheck_result_insert_content % (v_date, int(source), task_no, 'null', text)
                            # logger.info(flowcheck_result_insert_content)
                            try:
                                insert_mysql_object.execute(flowcheck_result_insert_content)
                            except Exception as e:
                                print(e)
                            finally:
                                insert_mysql_object.close()
                        if flag:
                            break

    # 汇总结果，插入流程质检总表
    insert_mysql_object = dbops.Mysql("f_fuwu_dm")
    flowcheck_join_insert_content = dbops.get_sql_content(flowcheck_join_insert_sql).replace("question_type", question_types)
    flowcheck_join_insert_content = flowcheck_join_insert_content % (flowcheck_date, flowcheck_date, flowcheck_date, flowcheck_date)
    # logger.info(flowcheck_join_insert_content)
    try:
        insert_mysql_object.execute(flowcheck_join_insert_content)
    except Exception as e:
        print(e)
    finally:
        insert_mysql_object.close()
