# coding=utf-8
"""
-------------------------------------------------
 Author      :   hadoop.xu
 Date        ：  2018/8/05
 Description :  训练模型
-------------------------------------------------
"""
import sys

import gensim
import numpy as np
# import tensorflow as tf
from sklearn.ensemble import GradientBoostingClassifier
from sklearn.ensemble import RandomForestClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.svm import SVR

from utils import log

logger = log.get_logger()


def doc2vec_trainer(labeled_pos, labeled_neg, vec_size=400, iters=500):
    """
    doc2vec模型训练
    :param labeled_pos: 标签化的正面情感数据
    :param labeled_neg: 标签化的负面情感数据
    :param vec_size: doc2vec后vector维度
    :param iters: 迭代次数
    :return: doc2vec的dm和dbow模型
    """
    if not labeled_pos or not labeled_neg:
        logger.error('The data of labeled_pos and labeled_neg are need!')
        sys.exit(0)

    # 实例doc2vec模型
    doc2vec_dm_model = gensim.models.Doc2Vec(min_count=1, window=10, size=vec_size, sample=1e-3, negative=5, workers=8,
                                             iter=iters)
    doc2vec_dbow_model = gensim.models.Doc2Vec(min_count=1, window=10, size=vec_size, sample=1e-3, negative=5, dm=0,
                                               workers=8, iter=iters)
    # 使用所有的数据建立词典
    train_data = []
    for p in labeled_pos:
        train_data.append(p)
    for n in labeled_neg:
        train_data.append(n)

    doc2vec_dm_model.build_vocab(train_data)
    doc2vec_dbow_model.build_vocab(train_data)

    # 训练doc2vec模型
    np.random.shuffle(train_data)
    doc2vec_dm_model.train(train_data, total_examples=doc2vec_dm_model.corpus_count, epochs=doc2vec_dm_model.iter)
    np.random.shuffle(train_data)
    doc2vec_dbow_model.train(train_data, total_examples=doc2vec_dbow_model.corpus_count, epochs=doc2vec_dbow_model.iter)
    return doc2vec_dm_model, doc2vec_dbow_model


def lr(train_data, train_label):
    """
    训练逻辑回归模型
    :param train_data: 训练数据集
    :param train_label: 训练标签
    :return lr: 返回逻辑回归模型
    """
    lr_model = LogisticRegression(class_weight={0: 0.5, 1: 0.5}, max_iter=5000)
    lr_model.fit(train_data, train_label)
    return lr_model


def gbdt(train_data, train_label):
    """
    训练gbdt模型
    :param train_data: 训练数据集
    :param train_label: 训练标签
    :return gbdt: 返回gbdt模型
    """
    gbdt_model = GradientBoostingClassifier(n_estimators=300, max_depth=50, learning_rate=0.1)
    gbdt_model.fit(train_data, train_label)
    return gbdt_model


def rf(train_data, train_label):
    """
    训练随机森林模型
    :param train_data: 训练数据集
    :param train_label: 训练标签
    :return RF: 返回随机森林模型
    """
    rf_model = RandomForestClassifier(n_estimators=300, max_depth=50, class_weight={0: 0.5, 1: 0.5})
    rf_model.fit(train_data, train_label)
    return rf_model


def svr(train_data, train_label):
    """
    支持向量回归模型
    :param train_data: 训练数据
    :param train_label: 训练数据的标签
    :return: 支持向量回归模型
    """
    svr_model = SVR(max_iter=1500)  # kernel:RBF,Linear,Poly,Sigmoid,默认的是"RBF"
    svr_model.fit(train_data, train_label)
    return svr_model


"""
def tf_lr(train_data, train_label, test_data, test_label):

    # tensorflow版逻辑回归
    # :param train_data: 训练数据集
    # :param train_label: 训练标签
    # :return lr: 返回逻辑回归模型


    data_shape = len(train_data[0])
    # 定义两个占位符，一个是训练数据占位符，一个是训练标签占位符
    x = tf.placeholder(shape=[None, data_shape], dtype=tf.float32)  # 训练数据占位符
    y = tf.placeholder(shape=[None, 1], dtype=tf.float32)  # 训练标签占位符

    # 初始化参数
    w = tf.Variable(tf.truncated_normal((data_shape, 1), stddev=0.01))
    b = tf.Variable(tf.zeros([1]))

    # 神经网络前向运行产生的实际值
    y_ = tf.sigmoid(tf.matmul(x, w) + b)

    # 定义交叉熵损失函数
    loss = tf.reduce_mean(-tf.reduce_sum(y * tf.log(y_) + (1 - y) * tf.log(1 - y_), reduction_indices=[1]))

    # 初始化优化器，这里使用动量因子优化器
    trainer = tf.train.MomentumOptimizer(learning_rate=0.0001, momentum=0.9).minimize(loss)

    # 测试精度
    predict = y_ > 0.5
    accracy = tf.reduce_mean(tf.cast(predict, 'float'))

    # 初始化变量和session
    init = tf.global_variables_initializer()
    session = tf.Session()
    session.run(init)

    # 迭代训练
    for step in range(10000):
        session.run(trainer, feed_dict={x: train_data, y: train_label})
        if step % 100 == 0:
            print('The accuracy of iteration %f is %f' % (step, session.run(accracy, feed_dict={x: test_data, y: test_label})))

"""
