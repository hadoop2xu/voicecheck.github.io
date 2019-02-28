package com.qunar.flight.fuwu.data.cf.desrecommend

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.elasticsearch.hadoop.cfg.ConfigurationOptions
import org.elasticsearch.spark.sql.EsSparkSQL

/**
  * Created by hadoop.xu on 2018/5/24.
  */
object RefineRecommendScore {

  def main(args: Array[String]) {
    //spark全局设置
    val sparkConf = new SparkConf()
      .set(ConfigurationOptions.ES_INDEX_AUTO_CREATE, "true")
      .set(ConfigurationOptions.ES_NODES, DRConstant.ES_CLUSTER)
      .set(ConfigurationOptions.ES_PORT, DRConstant.ES_PORT)
      .set(ConfigurationOptions.ES_MAPPING_ID, "mapping_id")
      .set(ConfigurationOptions.ES_WRITE_OPERATION, "upsert")

    val spark = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()
    refineRecommend(spark)

  }

  def refineRecommend(spark: SparkSession) {

    def SCI(positive: Long, n: Long): Double = {
      val p = positive.toDouble / n
      val z = 4.0
      val sci = (p + z / (2 * n) + z * math.sqrt((p * (1 - p) + z / (4 * n)) / n)) / (1 + z / n)
      sci
    }
    val userActionFmoile = spark.sql("select city_name,hit_price,want_to,entry_sub_page,share,c5 " +
      "from f_fuwu.usban_user_action_fmobile t1 inner join f_fuwu.urban_city_info t2 on t1.arr_city_id = t2.city_id ")
    userActionFmoile.createTempView("user_action_fmobile")
    userActionFmoile.sqlContext.udf.register("col_sum", (e1: Long, e2: Long, e3: Long, e4: Long) => {
      e1 + e2 + e3 + e4
    })
    userActionFmoile.sqlContext.udf.register("sci", (positive: Long, n: Long) => {
      SCI(positive, n)
    })
    val sql = "select * from (select t2.city_name,sci(t2.sumValue,t2.count) as sci from (select t1.city_name,t1.count," +
      " col_sum(t1.hit_price,t1.want_to,t1.entry_sub_page,t1.share) as sumValue  from " +
      "(select city_name,count(hit_price) as hit_price,3 * count(want_to) as want_to,count(entry_sub_page) as " +
      "entry_sub_page,5 * count(share) as share,sum(c5) as count from user_action_fmobile  group by city_name) t1) t2)t3 " +
      "where t3.sci != 'NaN'"
    val userActionFmobileAgg = userActionFmoile.sqlContext.sql(sql)
    val mappingDF = userActionFmobileAgg.withColumn("mapping_id", userActionFmobileAgg("city_name"))

    EsSparkSQL.saveToEs(mappingDF, "destination_recommend/refineRecommend_city_sci_info")
  }
}
