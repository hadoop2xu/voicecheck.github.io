# coding=utf-8
"""
-------------------------------------------------
 Author      :   hadoop.xu
 Date        ：  2018/8/08
 Description :  程序运行入口
-------------------------------------------------
"""
import argparse
import datetime
import os
import pickle as plk
import shutil
import jieba
import sys
import gensim
import time

sys.path.append('.')
sys.path.append('..')
sys.path.append('../..')
from model.qualitycheck import trainer
from model.qualitycheck import evaluator
from model.qualitycheck import predictor
from utils import log
from utils import data_processor, dbops

logger = log.get_logger()


def parse_args():
    """
    命令行解析
    :return parser: 返回参数解析器对象
    """
    parser = argparse.ArgumentParser()
    parser.add_argument('--data_set', type=str, default='data' + os.sep + 'trainset')
    parser.add_argument('--num_iters', type=int, default=3000)
    parser.add_argument('--filter_char', type=str, default=""" “”.,?!~！*；。：（）{}【】、-+=？...:;(){}[]，\n//""")
    parser.add_argument('--vector_size', type=int, default=400)
    parser.add_argument('--pos_label', type=str, default='POS')
    parser.add_argument('--neg_label', type=str, default='NEG')
    parser.add_argument('--is_training', type=bool, default=False)
    parser.add_argument('--models_dir', type=str, default='data' + os.sep + 'models')
    parser.add_argument('--chat_filter_score', type=float, default=0.980)
    parser.add_argument('--speech_filter_score', type=float, default=0.600)
    parser.add_argument('--prediction_date', type=str, default=(datetime.datetime.now() - datetime.timedelta(days=1)).strftime("%Y-%m-%d"))
    parser.add_argument('--query_type', type=str, default='speech', choices=['speech', 'chat'])
    return parser.parse_args()


if __name__ == '__main__':
    args = parse_args()
    current_dir = os.getcwd()
    data_set = current_dir + os.sep + args.data_set  # 训练数据集所在目录
    models_dir = current_dir + os.sep + args.models_dir  # 模型所在目录
    # 模型文件所在的位置
    doc2vec_dm_dir = models_dir + os.sep + 'doc2vec_dm.model'  # doc2vec dm 模型
    doc2vec_dbow_dir = models_dir + os.sep + 'doc2vec_dbow.model'  # doc2vec dbow 模型
    lr_dir = models_dir + os.sep + 'lr.model'  # 逻辑回归模型
    gbdt_dir = models_dir + os.sep + 'gbdt.model'  # gbdt模型
    rf_dir = models_dir + os.sep + 'rf.model'  # 随机森林模型
    svr_dir = models_dir + os.sep + 'svr.model'  # 支持向量机模型

    is_training = args.is_training
    if is_training:  # 训练分类模型

        # 读取原始训练数据
        pos, neg = data_processor.read_from_file(data_set)

        # 训练数据做清理处理
        cleaned_pos = data_processor.clean(pos, args)
        cleaned_neg = data_processor.clean(neg, args)

        # 标签化数据
        labeled_pos = data_processor.labeled_data(cleaned_pos, args.pos_label)
        labeled_neg = data_processor.labeled_data(cleaned_neg, args.neg_label)

        # 训练doc2vec的模型
        doc2vec_dm_model, doc2vec_dbow_model = trainer.doc2vec_trainer(labeled_pos, labeled_neg, args.vector_size, args.num_iters)

        # 向量化后的训练数据集
        pos_vec = data_processor.get_vectors(doc2vec_dm_model, doc2vec_dbow_model, labeled_pos, args.vector_size)
        neg_vec = data_processor.get_vectors(doc2vec_dm_model, doc2vec_dbow_model, labeled_neg, args.vector_size)

        # 生成训练集和测试集
        train_data, test_data, train_label, test_label = data_processor.get_train_test_dataset(pos_vec, neg_vec)

        # 训练逻辑回归、gbdt和随机森林模型
        LR = trainer.lr(train_data, train_label)
        # GBDT = trainer.gbdt(train_data, train_label)
        RF = trainer.rf(train_data, train_label)
        # SVR = trainer.svr(train_data, train_label)

        # 测试模型精度
        LR_accuracy = evaluator.accuracy_score(LR, test_data, test_label)
        # GBDT_accuracy = evaluator.accuracy_score(GBDT, test_data, test_label)
        RF_accuracy = evaluator.accuracy_score(RF, test_data, test_label)
        # SVR_accuracy = evaluator.accuracy_score(SVR, test_data, test_label)

        print('The accuracy of LR is : ' + repr(LR_accuracy))
        # print('The accuracy of GBDT is : ' + repr(GBDT_accuracy))
        print('The accuracy of RF is : ' + repr(RF_accuracy))
        # print('The accuracy of SVR is : ' + repr(SVR_accuracy))

        # 保存模型
        postfix = datetime.datetime.now().strftime('%Y%m%d-%H%M') + '.bak'
        if os.path.exists(doc2vec_dm_dir):
            shutil.move(doc2vec_dm_dir, doc2vec_dm_dir + '_' + postfix)
        if os.path.exists(doc2vec_dbow_dir):
            shutil.move(doc2vec_dbow_dir, doc2vec_dbow_dir + '_' + postfix)
        if os.path.exists(lr_dir):
            shutil.move(lr_dir, lr_dir + '_' + postfix)
        """
        if os.path.exists(gbdt_dir):
            shutil.move(gbdt_dir, gbdt_dir + '_' + postfix)
        """
        if os.path.exists(rf_dir):
            shutil.move(rf_dir, rf_dir + '_' + postfix)
        """
        if os.path.exists(svr_dir):
            shutil.move(svr_dir, svr_dir + '_' + postfix)
        """

        doc2vec_dm_model.save(doc2vec_dm_dir)
        doc2vec_dbow_model.save(doc2vec_dbow_dir)
        plk.dump(LR, open(lr_dir, 'wb'))
        # plk.dump(GBDT, open(gbdt_dir, 'wb'))
        plk.dump(RF, open(rf_dir, 'wb'))
        # plk.dump(SVR, open(svr_dir, 'wb'))
    else:  # 根据分类模型进行预测
        doc2vec_dm_model = gensim.models.Doc2Vec.load(doc2vec_dm_dir)
        doc2vec_dbow_model = gensim.models.Doc2Vec.load(doc2vec_dbow_dir)
        model_lr = plk.load(open(lr_dir, 'rb'))
        # model_gbdt = plk.load(open(gbdt_dir, 'rb'))
        model_rf = plk.load(open(rf_dir, 'rb'))
        # model_svr = plk.load(open(svr_dir, 'rb'))

        """
        # 测试效果
        content = u'太坑了吧 直接跟航空公司定也不这样'
        # content = u'信任京东！物流快！服务好！货品质量有保证！'
        # content = u'我不退了,我要是去机场你不给我打两个票你就等着接律师函吧'
        content = list(jieba.cut(content, cut_all=False))
        content = data_processor.single_content_clean(content, args)
        lr_prob = predictor.predict_prob(content, doc2vec_dm_model, doc2vec_dbow_model, model_lr)
        # gbdt_prob = predictor.predict_prob(content, doc2vec_dm_model, doc2vec_dbow_model, model_gbdt)
        rf_prob = predictor.predict_prob(content, doc2vec_dm_model, doc2vec_dbow_model, model_rf)
        # svr_prob = predictor.svr_predict_prob(content, doc2vec_dm_model, doc2vec_dbow_model, model_svr)

        print("The prediction result of lr is " + repr(lr_prob))
        # print("The prediction result of gbdt is " + repr(gbdt_prob))
        print("The prediction result of rf is " + repr(rf_prob))
        # print("The prediction result of svr is " + repr(svr_prob))
        """

        # 对每天增量数据进行预测
        if args.query_type == 'chat':
            begin = time.time()
            predictor.chat_record_predict('f_fuwu_dw', doc2vec_dm_model, doc2vec_dbow_model, args, model_lr)  # chat质检
            end = time.time()
            st = end - begin
            logger.info("chat need time is %s" % repr(st))
        elif args.query_type == 'speech':
            begin = time.time()
            predictor.speech_record_predict('f_fuwu_dm', doc2vec_dm_model, doc2vec_dbow_model, args, model_lr)  # 语音质检
            end = time.time()
            st = end - begin
            logger.info("speech need time is %s" % repr(st))
        else:
            pass
