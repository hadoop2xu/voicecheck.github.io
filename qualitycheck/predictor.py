# coding=utf-8
"""
-------------------------------------------------
 Author      :   hadoop.xu
 Date        ：  2018/8/02
 Description :   舆情预测
-------------------------------------------------
"""
import json
import os
import sys

import jieba
import numpy as np
import xgboost as xgb

from model.qualitycheck import parser
from utils import dbops, data_processor
from utils import log
import datetime
from multiprocessing import Pool

logger = log.get_logger()
cur_path = os.getcwd()
chat_query_sql = os.path.dirname(os.path.dirname(cur_path)) + os.sep + 'sql' + os.sep + 'qualitycheck' + os.sep + 'chat_query.sql'
speech_query_sql = os.path.dirname(os.path.dirname(cur_path)) + os.sep + 'sql' + os.sep + 'qualitycheck' + os.sep + 'speech_query.sql'
speech_query_with_emotion_sql = os.path.dirname(os.path.dirname(cur_path)) + os.sep + 'sql' + os.sep + 'qualitycheck' + os.sep + 'speech_query_with_emotion.sql'
result_insert_sql = os.path.dirname(os.path.dirname(cur_path)) + os.sep + 'sql' + os.sep + 'qualitycheck' + os.sep + 'result_insert.sql'
sensitive_word_path = cur_path + os.sep + 'data' + os.sep + 'sensitive_word.txt'

# 判断敏感词路径是否存在
if not os.path.exists(sensitive_word_path):
    logger.error('The path of sensitive word not exist！')
    sys.exit(0)

# 加载敏感词
sensitive_word = []
reader = open(sensitive_word_path, 'r', encoding='utf-8')
line = reader.readline()
while line:
    sensitive_word.append(line.strip('\r\n'))
    line = reader.readline()


def predict(content, doc2vec_dm_model, doc2vec_dbow_model, model):
    """
    分类预测，主要预测类别
    :param content: 待预测的文本
    :param doc2vec_dm_model: doc2vec dm模型
    :param doc2vec_dbow_model: doc2vec dbow模型
    :param model: 回归模型，用来预测
    :return: 返回预测的标签
    """
    dm_vec = doc2vec_dm_model.infer_vector(doc_words=content, steps=1000, alpha=0.025)
    dbow_vec = doc2vec_dbow_model.infer_vector(doc_words=content, steps=1000, alpha=0.025)
    content_vec = np.hstack((dm_vec, dbow_vec)).reshape(1, -1)
    label = model.predict(content_vec)[0]  # 预测标签
    return label


def predict_prob(content, doc2vec_dm_model, doc2vec_dbow_model, *models):
    """
    回归预测，主要预测概率
    :param content: 待预测的文本
    :param doc2vec_dm_model: doc2vec dm模型
    :param doc2vec_dbow_model: doc2vec dbow模型
    :param models: 回归模型，用来预测
    :return: 返回预测的概率
    """
    dm_vec = doc2vec_dm_model.infer_vector(doc_words=content, steps=1000, alpha=0.025)
    dbow_vec = doc2vec_dbow_model.infer_vector(doc_words=content, steps=1000, alpha=0.025)
    content_vec = np.hstack((dm_vec, dbow_vec)).reshape(1, -1)
    prob = 0.0
    for model in models:
        prob += model.predict_proba(content_vec)[0][0]  # 回归预测为负面的概率值
    return prob / len(models)


def svr_predict_prob(content, doc2vec_dm_model, doc2vec_dbow_model, model):
    """
    回归预测，主要预测概率
    :param content: 待预测的文本
    :param doc2vec_dm_model: doc2vec dm模型
    :param doc2vec_dbow_model: doc2vec dbow模型
    :param model: 回归模型，用来预测
    :return: 返回预测的概率
    """
    dm_vec = doc2vec_dm_model.infer_vector(doc_words=content, steps=1000, alpha=0.025)
    dbow_vec = doc2vec_dbow_model.infer_vector(doc_words=content, steps=1000, alpha=0.025)
    content_vec = np.hstack((dm_vec, dbow_vec)).reshape(1, -1)
    prob = model.predict(content_vec)  # 回归预测为负面的概率值
    return prob


def chat_record_neg_analyze(result_with_parms):
    """
    chat记录负面情绪分析
    :param result_with_parms: chat记录以及参数，元组类型
    """
    if len(result_with_parms) != 5:
        logger.error("The result with params format is err!")
        sys.exit(0)

    record = result_with_parms[0]
    doc2vec_dm_model = result_with_parms[1]
    doc2vec_dbow_model = result_with_parms[2]
    args = result_with_parms[3]
    models = result_with_parms[4]

    size = len(record)
    if size != 10:
        logger.error("The data format is err!")
        sys.exit(0)

    try:
        chat_records = json.loads(record[2])  # 聊天记录，有多条
    except Exception as e:
        chat_records = None
        print(e)

    if chat_records:
        create_time = record[1]  # 创建时间
        session_id = record[3]  # 聊天记录对应的session id
        # time_long = record[4]  # 聊天记录时长
        user_id = record[5]  # 用户的id号
        agent_id = record[6]  # 客服的id号
        # begin_time = record[7]  # 聊天开始时间
        # end_time = record[8]  # 聊天的结束时间
        user_say, kefu_say = parser.chat_record_parser(chat_records, user_id, agent_id)  # 用户和客服说话内容
        user_say_highest_score = (0.0, '')

        for content in user_say:  # 对用户说的每句话进行打分
            precess_content = list(jieba.cut(content, cut_all=False))  # 结巴分词
            precess_content = data_processor.single_content_clean(precess_content, args)  # 清洗数据
            neg_prob = predict_prob(precess_content, doc2vec_dm_model, doc2vec_dbow_model, *models)  # 调用模型对该句内容进行打分
            if precess_content and neg_prob > args.chat_filter_score and neg_prob > user_say_highest_score[0] \
                    and data_processor.has_sensitive_word(precess_content, sensitive_word):
                user_say_highest_score = (neg_prob, content)

        kefu_say_highest_score = (0.0, '')
        for content in kefu_say:  # 对客服说的每句话进行打分
            precess_content = list(jieba.cut(content, cut_all=False))  # 结巴分词
            precess_content = data_processor.single_content_clean(precess_content, args)  # 清洗数据
            neg_prob = predict_prob(precess_content, doc2vec_dm_model, doc2vec_dbow_model, *models)  # 调用模型对该句内容进行打分
            if precess_content and neg_prob > args.chat_filter_score and neg_prob > kefu_say_highest_score[0] \
                    and data_processor.has_sensitive_word(precess_content, sensitive_word):
                kefu_say_highest_score = (neg_prob, content)

        # 取用户、客服负面情绪分最高的那条记录
        if user_say_highest_score[0] > kefu_say_highest_score[0] and user_say_highest_score[1]:
            score_content = user_say_highest_score
        elif user_say_highest_score[0] <= kefu_say_highest_score[0] and kefu_say_highest_score[1]:
            score_content = user_say_highest_score
        else:
            score_content = None
        if score_content and score_content[0] >= args.chat_filter_score:
            score = score_content[0]  # 该记录总分
            problem_result = score_content[1]  # 该记录有问题的内容
            source = 1  # 1代表chat,2代表语音

            # 结果插入数据库
            insert_mysql_object = dbops.Mysql("f_fuwu_dm")
            insert_sql_content = dbops.get_sql_content(result_insert_sql)
            insert_sql_content = insert_sql_content % (create_time, source, session_id, problem_result, score)
            # print(insert_sql_content)
            try:
                insert_mysql_object.execute(insert_sql_content)
            except Exception as e:
                print(e)
            finally:
                insert_mysql_object.close()


def chat_record_predict(db_namespace='f_fuwu_dw', doc2vec_dm_model=None, doc2vec_dbow_model=None, args=None, *models):
    """
    读取mysql指定记录表中的数据，根据预测模型预测舆情分数值
    :param db_namespace: 数据库名称
    :param doc2vec_dm_model: doc2vec dm模型
    :param doc2vec_dbow_model: doc2vec dbow模型
    :param args: 参数配置
    :param models: 回归模型
    """
    # 生成查询时间范围，如 2017-12-26 00:00:00 - 2017-12-26 23:59:59
    predict_date = args.prediction_date
    start_predict_date = predict_date + ' 00:00:00'
    end_predict_date = predict_date + ' 23:59:59'

    if not os.path.exists(chat_query_sql):
        logger.error('The sql of chat not exist！')
        sys.exit(0)

    # 连接数据库，查询结果
    query_mysql_object = dbops.Mysql(db_namespace)
    query_sql_content = dbops.get_sql_content(chat_query_sql)
    query_sql_content = query_sql_content % (start_predict_date, end_predict_date)
    result = query_mysql_object.query(query_sql_content)
    query_mysql_object.close()

    # 对查询结果，即聊天记录舆情分析
    result_with_parms = []
    for r in result:
        rp = (r, doc2vec_dm_model, doc2vec_dbow_model, args, models)
        result_with_parms.append(rp)

    # 对查询结果，即聊天记录舆情分析，使用并行计算
    pool = Pool(2)
    pool.map(chat_record_neg_analyze, result_with_parms)
    # pool.join()
    pool.close()


def speech_record_neg_analyze(result_with_parms):
    """
    语音记录负面情绪分析
    :param result_with_parms: 语音记录以及参数，元组类型
    """
    if len(result_with_parms) != 5:
        logger.error("The result with params format is err!")
        sys.exit(0)

    record = result_with_parms[0]
    doc2vec_dm_model = result_with_parms[1]
    doc2vec_dbow_model = result_with_parms[2]
    args = result_with_parms[3]
    models = result_with_parms[4]

    size = len(record)
    if size != 13:
        logger.error("The data format is err!")
        sys.exit(0)

    """下面是不带情感得分的查询解析结果"""
    # v_date = record[0]  # 计算日期
    # task_no = record[1]  # 任务号
    # transfer_way = record[2]  # 转换方式
    # status = record[3]  # 状态
    # create_time = record[4]  # 创建时间
    # result = record[5] # 通话语义

    """下面是带情感得分的查询解析结果"""
    task_no = record[0]  # 语音任务号,
    # business_no = record[1]  # 通话sid
    # order_no = record[2]  # 订单号
    score_qingxu = record[3]  # 情绪得分
    # score_caozuo = record[4]  # 处理得分
    # voice_address = record[5]  # 录音地址,
    # caller_role = record[6]
    # dialed_role = record[7]
    # creator_ucaccount = record[8]  # 电话操作人
    # start_time = record[9]  # 接听时间
    # stop_time = record[10]  # 结束时间
    result = record[11]  # 通话语义
    v_date = record[12].strftime('%Y-%m-%d')  # 计算日期

    try:
        speech_records = json.loads(result)  # 通话语义
    except Exception as e:
        speech_records = None
        print(e)
    if speech_records:
        speech_contents = parser.speech_record_parser(speech_records)
        speech_highest_content_score = (0.0, '')

        # 对语音每条记录进行打分
        for speechContent in speech_contents:
            precess_content = data_processor.single_content_clean(speechContent, args)  # 清洗数据
            neg_prob = predict_prob(precess_content, doc2vec_dm_model, doc2vec_dbow_model, *models)  # 调用模型对该句内容进行打分
            neg_prob = neg_prob * 0.8 + float(score_qingxu) * 0.2  # 加入情绪分占比
            if precess_content and neg_prob >= args.speech_filter_score and neg_prob > speech_highest_content_score[0] \
                    and data_processor.has_sensitive_word(precess_content, sensitive_word):
                speech_highest_content_score = (neg_prob, speechContent)

        if speech_highest_content_score and speech_highest_content_score[0] >= args.speech_filter_score:
            score = speech_highest_content_score[0]  # 该记录总分
            problem_result = speech_highest_content_score[1]  # 该记录有问题的内容
            source = 2  # 1代表chat,2代表语音

            # 结果插入数据库
            insert_mysql_object = dbops.Mysql("f_fuwu_dm")
            insert_sql_content = dbops.get_sql_content(result_insert_sql)
            insert_sql_content = insert_sql_content % (v_date, source, task_no, ' '.join(problem_result), score)
            # print(insert_sql_content)
            try:
                insert_mysql_object.execute(insert_sql_content)
            except Exception as e:
                print(e)
            finally:
                insert_mysql_object.close()


def speech_record_predict(db_namespace='f_fuwu_dm', doc2vec_dm_model=None, doc2vec_dbow_model=None, args=None, *models):
    """
    读取mysql指定记录表中的数据，根据预测模型预测舆情分数值
    :param db_namespace: 数据库名称
    :param doc2vec_dm_model: doc2vec dm模型
    :param doc2vec_dbow_model: doc2vec dbow模型
    :param args: 参数配置
    :param models: 回归模型
    """
    # 查询时间
    predict_date = args.prediction_date

    if not os.path.exists(speech_query_with_emotion_sql):
        logger.error('The sql of speech not exist！')
        sys.exit(0)

    # 连接数据库，查询结果
    query_mysql_object = dbops.Mysql(db_namespace)
    query_sql_content = dbops.get_sql_content(speech_query_with_emotion_sql)
    query_sql_content = query_sql_content % (predict_date, predict_date)
    result = query_mysql_object.query(query_sql_content)
    query_mysql_object.close()
    result_with_parms = []
    for r in result:
        rp = (r, doc2vec_dm_model, doc2vec_dbow_model, args, models)
        result_with_parms.append(rp)

    # 对查询结果，即聊天记录舆情分析，使用并行计算
    pool = Pool(10)
    pool.map(speech_record_neg_analyze, result_with_parms)
    # pool.join()
    pool.close()

