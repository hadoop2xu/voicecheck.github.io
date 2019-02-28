package com.qunar.flight.fuwu.data.cf.desrecommend

/**
  * Created by hadoop.xu on 2018/5/28.
  */
object DRConstant {

  val ES_CLUSTER = "l-qesaasdatanodessd13.ops.cn2.qunar.com, l-qesaasdatanodessd49.ops.cn2.qunar.com, " +
    "l-qesaasdatanodessd22.ops.cn2.qunar.com, l-qesaasdatanodessd33.ops.cn2.qunar.com," +
    " l-qesaasdatanodessd29.ops.cn2.qunar.com, l-qesaasdatanodessd31.ops.cn2.qunar.com"

  val ES_PORT = "17302"
  val LABEL_NAME = "避暑,草原,城市风光,地质,动植物园,购物,观光,海,红色旅游,户外,建筑,江河湖泊,教育,历史文化," +
    "美食,民俗,森林,沙漠,山,赏花,摄影," +
    "世界遗产,水文,天文,艺术,游乐园,娱乐,园林,运动,宗教,综合自然景观"
  val CITY_LABEL_SCHEMA = "arr_city,避暑,草原,城市风光,地质,动植物园,购物,观光," +
    "海,红色旅游,户外,建筑,江河湖泊,教育,历史文化,美食,民俗,森林,沙漠,山,赏花," +
    "摄影,世界遗产,水文,天文,艺术,游乐园,娱乐,园林,运动,宗教,综合自然景观"

  val USER_MEAN_SQL = "label1,label2,label3,label4,label5,label6," +
    "label7,label8,label9,label10,label11,label12,label13,label14,label15,label16,label17,label18," +
    "label19,label20,label21,label22,label23,label24,label25,label26,label27,label28,label29,label30,label31"

  val DOM_SUM_LABEL_SCORE = "select username,multiplicative_2(label1,score) as label1," +
    "multiplicative_2(label2,score) as label2,multiplicative_2(label3,score) as label3," +
    "multiplicative_2(label4,score) as label4,multiplicative_2(label5,score) as label5," +
    "multiplicative_2(label6,score) as label6,multiplicative_2(label7,score) as label7," +
    "multiplicative_2(label8,score) as label8,multiplicative_2(label9,score) as label9," +
    "multiplicative_2(label10,score) as label10,multiplicative_2(label11,score) as label11," +
    "multiplicative_2(label12,score) as label12,multiplicative_2(label13,score) as label13," +
    "multiplicative_2(label14,score) as label14,multiplicative_2(label15,score) as label15," +
    "multiplicative_2(label16,score) as label16,multiplicative_2(label17,score) as label17," +
    "multiplicative_2(label18,score) as label18,multiplicative_2(label19,score) as label19," +
    "multiplicative_2(label20,score) as label20,multiplicative_2(label21,score) as label21," +
    "multiplicative_2(label22,score) as label22,multiplicative_2(label23,score) as label23," +
    "multiplicative_2(label24,score) as label24,multiplicative_2(label25,score) as label25," +
    "multiplicative_2(label26,score) as label26,multiplicative_2(label27,score) as label27," +
    "multiplicative_2(label28,score) as label28,multiplicative_2(label29,score) as label29," +
    "multiplicative_2(label30,score) as label30,multiplicative_2(label31,score) as label31 from dom_city_label_score"
  val INTER_SUM_LABEL_SCORE = "select username,multiplicative_2(label1,score) as label1," +
    "multiplicative_2(label2,score) as label2,multiplicative_2(label3,score) as label3," +
    "multiplicative_2(label4,score) as label4,multiplicative_2(label5,score) as label5," +
    "multiplicative_2(label6,score) as label6,multiplicative_2(label7,score) as label7," +
    "multiplicative_2(label8,score) as label8,multiplicative_2(label9,score) as label9," +
    "multiplicative_2(label10,score) as label10,multiplicative_2(label11,score) as label11," +
    "multiplicative_2(label12,score) as label12,multiplicative_2(label13,score) as label13," +
    "multiplicative_2(label14,score) as label14,multiplicative_2(label15,score) as label15," +
    "multiplicative_2(label16,score) as label16,multiplicative_2(label17,score) as label17," +
    "multiplicative_2(label18,score) as label18,multiplicative_2(label19,score) as label19," +
    "multiplicative_2(label20,score) as label20,multiplicative_2(label21,score) as label21," +
    "multiplicative_2(label22,score) as label22,multiplicative_2(label23,score) as label23," +
    "multiplicative_2(label24,score) as label24,multiplicative_2(label25,score) as label25," +
    "multiplicative_2(label26,score) as label26,multiplicative_2(label27,score) as label27," +
    "multiplicative_2(label28,score) as label28,multiplicative_2(label29,score) as label29," +
    "multiplicative_2(label30,score) as label30,multiplicative_2(label31,score) as label31 from inter_city_label_score"

  val MEAN_SQL = "avg(label1),avg(label2),avg(label3),avg(label4),avg(label5),avg(label6)," +
    "avg(label7),avg(label8),avg(label9),avg(label10),avg(label11),avg(label12),avg(label13),avg(label14),avg(label15)" +
    ",avg(label16),avg(label17),avg(label18),avg(label19),avg(label20),avg(label21),avg(label22)," +
    "avg(label23),avg(label24),avg(label25),avg(label26),avg(label27),avg(label28),avg(label29),avg(label30),avg(label31)"

}
