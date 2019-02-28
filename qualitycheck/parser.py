# coding=utf-8
"""
-------------------------------------------------
 Author      :   hadoop.xu
 Date        ：  2018/7/25
 Description :  解析数据库读取的json数据
-------------------------------------------------
"""

from utils import log, data_processor
import jieba

logger = log.get_logger()


def chat_record_parser(chat_records, user_id, agent_id):
    """
    人工聊天记录json数据解析
    :param chat_records: 聊天记录
    :param user_id: 用户名id
    :param agent_id: 客服id
    :return: 分别返回用户说话内容和客服说话内容
    """
    user_say = []
    kefu_say = []
    for chatRecord in chat_records:
        if len(chatRecord) == 8:
            # userName = chatRecord['userName'] # 每条记录中的用户名
            detail_user_id = chatRecord['userId']  # 每条记录中的用户id，如果是客服，则等于上面的agentId，如果是用户，则等于上面的userId
            digest = chatRecord['digest']  # 每条记录中的聊天内容
            # content = chatRecord['content'] # 其他的内容，无关紧要
            # time = chatRecord['time'] # 聊天记录时间，无关紧要
            # msgType = chatRecord['msgType'] # 聊天记录的消息类型，无关紧要
            # userFaceUrl = chatRecord['userFaceUrl'] # url, 无关紧要
            # imuserid = chatRecord['imuserid'] # 其他，无关紧要
            if detail_user_id == user_id and not data_processor.is_fixed_sentence(digest):  # 用户说的话
                user_say.append(digest)
                # print("User say : " + digest)
            elif detail_user_id == agent_id and not data_processor.is_fixed_sentence(digest):  # 客服说的话
                kefu_say.append(digest)
                # print("KeFu say : " + digest)
            else:  # 机器人说的话
                pass
    return user_say, kefu_say


def speech_record_parser(speech_records):
    """
    人工语音记录json数据解析
    :param speech_records: 语音记录
    :return: 分别返回语音转换后的文字信息
    """
    speech_contents = []
    for speechRecord in speech_records['sentenceListData']:
        text = speechRecord['text']
        word_list = speechRecord['wordList']
        if len(word_list) >= 5:
            # word_list = list(jieba.cut(text, cut_all=False))
            speech_contents.append(word_list)
    return speech_contents
