# coding=utf-8
"""
-------------------------------------------------
 Author      :   hadoop.xu
 Date        ：  2018/7/26
 Description :  评估训练的模型
-------------------------------------------------
"""
from sklearn.metrics import roc_curve, auc
import numpy as np


def accuracy_score(model, test_data, test_label):
    """
    测试模型的精度
    :param model: 训练的模型
    :param test_data: 测试数据集
    :param test_label: 测试数据集真正的标签
    :return: 返回模型精度
    """
    predict_label = np.asarray(model.predict(test_data)) >= 0.5
    predict_label = list(map(lambda e: 1.0 if e >= 0.5 else 0.0, predict_label))

    counter = 0
    for b in np.equal(np.asarray(test_label), np.asarray(predict_label)):
        if b:
            counter += 1
    sum_size = float(len(predict_label))
    return counter / sum_size


def get_auc(model, test_data, test_label):
    """
    测试训练模型的AUC值
    :param model: 训练的模型
    :param test_data: 测试数据集
    :param test_label: 测试数据集真正的标签
    :return: 返回AUC值
    """
    predict_label = model.predict(test_data)
    fpr, tpr, _ = roc_curve(test_label, predict_label)
    auc_value = auc(fpr, tpr)
    return auc_value
