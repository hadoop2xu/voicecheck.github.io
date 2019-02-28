package com.qunar.flight.fuwu.data.cf.desrecommend

import java.util

import com.qunar.flight.fuwu.data.commons.ParamUtils
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.mllib.linalg.DenseVector
import org.apache.spark.mllib.linalg.distributed._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.elasticsearch.hadoop.cfg.ConfigurationOptions
import org.elasticsearch.spark.sql.EsSparkSQL
import org.slf4j.LoggerFactory

import scala.collection.immutable.HashMap

/**
  * 低价机票推荐一期
  *
  * @author hadoop.xu
  */
object DesRecommendMain {

  private val logger = LoggerFactory.getLogger(DesRecommendMain.getClass)
  //国内数据
  private var domCityMap = new HashMap[Long, String]()
  private var domCityLabelScoreDF: DataFrame = null

  //不区分国内外
  private var labelMap = new HashMap[Long, String]()
  private var userMap = new HashMap[Long, String]()

  //国外数据
  private var interCityMap = new HashMap[Long, String]()
  private var interCityLabelScoreDF: DataFrame = null

  def main(args: Array[String]) {
    //spark全局设置
    val sparkConf = new SparkConf()
      .set(ConfigurationOptions.ES_INDEX_AUTO_CREATE, "true")
      .set(ConfigurationOptions.ES_NODES, DRConstant.ES_CLUSTER)
      .set(ConfigurationOptions.ES_PORT, DRConstant.ES_PORT)
      .set(ConfigurationOptions.ES_MAPPING_ID, "mapping_id")
      .set(ConfigurationOptions.ES_WRITE_OPERATION, "upsert")
    val spark = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()

    val searchDate = extractParam(args)
    val startTime = System.currentTimeMillis()
    //获取所有数据
    val drBean = getAllData(spark, searchDate)
    logger.info("getAllData cost :{}", System.currentTimeMillis() - startTime)
    //记录全局变量
    rememberData(drBean.getInterLabelScore, drBean.getDomLabelScore)
    logger.info("rememberData cost :{}", System.currentTimeMillis() - startTime)

    //计算标签相似度矩阵
    val domCityLabelCosineMatrix = cosineSimilarity(drBean.getDomFlightSearchInfo, drBean.getDomLabelScore)
    logger.info("dom cosineSimilarity cost :{}", System.currentTimeMillis() - startTime)
    val interCityLabelCosineMatrix = cosineSimilarity(drBean.getInterFlightSearchInfo,
      drBean.getInterLabelScore, domestic = false)
    logger.info("inter cosineSimilarity cost :{}", System.currentTimeMillis() - startTime)

    //计算用户-标签得分矩阵
    val userHistoryLabelMatrix = getUserLabelScoreMatrix(drBean).transpose().toIndexedRowMatrix()
    logger.info("userHistroyLabelMatrix cost :{}", System.currentTimeMillis() - startTime)

    //与 labelCosineMatrix 矩阵相乘  结果类型 31 * usercount
    val (domResultDF, interResultDF) = matrixMultiply(domCityLabelCosineMatrix, interCityLabelCosineMatrix,
      userHistoryLabelMatrix, spark)
    logger.info("matrixMultiply cost :{}", System.currentTimeMillis() - startTime)

    //保存结果到es
    saveToES(domResultDF, interResultDF, spark.sqlContext)
    logger.info("saveToES cost :{}", System.currentTimeMillis() - startTime)
  }

  def extractParam(args: Array[String]): String = {

    logger.info("args:{}", args)
    val arg = ParamUtils.getArgs(args)
    val date = arg.get("search_date")
    if (null == date) {
      logger.error("没有传入日期参数！")
      return null
    }
    //参数校验  yyyyMMdd格式
    val datePattern = "/^[1-9]\\d{3}(0[1-9]|1[0-2])(0[1-9]|[1-2][0-9]|3[0-1])$/".r
    if (!datePattern.pattern.matcher(date).matches()) {
      logger.error("传入日期参数格式错误！date:{}", date)
      return null
    }

    date
  }

  /**
    * 计算城市标签 考虑流行度得分
    *
    * @param cityPopular    城市流行度得分 city | popular
    * @param cityLabelScore 城市标签得分  clit | label1 .... label31
    * @param cityColumn     城市列名称
    * @return 城市标签得分
    */
  def getLabelWithPopularScore(cityPopular: DataFrame, cityLabelScore: DataFrame, cityColumn: String): DataFrame = {

    //这里采用 left_outer是因为如果城市没有搜索过,那么这个城市在后面的标签得分中就是原始的得分
    val cityLabel = cityLabelScore.join(cityPopular, cityLabelScore(cityColumn).equalTo(
      cityPopular(cityColumn)), "left_outer").drop(cityColumn)
    //labelScore 中最后一列为城市索引值
    val labelScore = cityLabel.rdd.zipWithIndex().map(f => {
      val row = f._1
      val labelScore = new Array[Double](row.size)
      if (null == row.get(row.size - 1)) {
        for (i <- 0 until row.size - 1) {
          labelScore(i) = row.getDouble(i)
        }
      } else {
        val pop = row.getDouble(row.size - 1)
        for (i <- 0 until (row.size - 1)) {
          labelScore(i) = row.getDouble(i) * pop
        }
      }
      labelScore(row.size - 1) = f._2
      Row.fromSeq(labelScore)
    })
    val labelScorePopular = cityLabelScore.sqlContext.createDataFrame(labelScore, StructType(cityLabel.schema))

    labelScorePopular
  }

  /**
    * 计算城市流行度
    *
    * @param flightSearchInfo 城市搜索表
    * @param cityColumn       城市列名称
    * @param domestic         是否国内
    * @return 城市流行度得分
    */
  def getCityPopular(flightSearchInfo: DataFrame, cityColumn: String, domestic: Boolean): DataFrame = {

    val citySearchNum = flightSearchInfo.select(cityColumn).groupBy(cityColumn).count()
    val citySearchNumRDD = citySearchNum.rdd.map(row => {
      var pop: Double = 0
      if (domestic) {
        pop = math.sqrt(Math.log10(row.getLong(1)) + 1)
      } else {
        pop = math.log(row.getLong(1))
      }
      Row.apply(row.getString(0), pop)
    })
    val schema = StructType(List(
      StructField(cityColumn, StringType, nullable = false),
      StructField("popular", DoubleType, nullable = false)
    ))
    val citySearchDF = flightSearchInfo.sqlContext.createDataFrame(citySearchNumRDD, schema)
    citySearchDF
  }

  /**
    * 计算标签余弦相似度矩阵
    *
    * @param searchInfo     搜索表
    * @param cityLabelScore 城市标签得分
    * @param domestic       是否国内
    * @return 标签余弦相似度矩阵
    */
  def cosineSimilarity(searchInfo: DataFrame, cityLabelScore: DataFrame, domestic: Boolean = true): CoordinateMatrix = {

    //获取城市popular值  city | popular （区分国内 国际）
    val cityPopular = getCityPopular(searchInfo, "arr_city", domestic)
    //将标签*Popular作为新标签 city | label1 | label2 |.....
    val labelScoreWithPopular = getLabelWithPopularScore(cityPopular, cityLabelScore, "arr_city")
    //得到的矩阵为 城市索引 -> label得分
    val cityLableScoreRDD = labelScoreWithPopular.rdd.map(row => {
      val cityLableScore = new Array[Double](row.length - 1)
      for (i <- 0 until (row.length - 1)) {
        cityLableScore(i) = row.getDouble(i)
      }
      new IndexedRow(row.getDouble(row.size - 1).toLong, new DenseVector(cityLableScore))
    })

    val indexedRowMatrix = new IndexedRowMatrix(cityLableScoreRDD).toCoordinateMatrix().transpose().toIndexedRowMatrix()
    val tempRDD = indexedRowMatrix.rows.cartesian(indexedRowMatrix.rows).map(f => {
      val index1 = f._1
      val index2 = f._2
      val x1 = index1.vector.toArray.map(x => x * x).sum
      val x2 = index2.vector.toArray.map(x => x * x).sum
      val x3 = index1.vector.toArray.zip(index2.vector.toArray).map(f => f._1 * f._2).sum
      val cosineSimilarity = x3 / math.sqrt(x1 * x2)
      MatrixEntry.apply(index1.index, index2.index, cosineSimilarity)
    })
    val tempMatrix = new CoordinateMatrix(tempRDD)

    tempMatrix
  }

  /**
    *
    * 获取用户城市得分
    *
    * @param travelScore 旅行得分表
    * @param searchScore 搜索得分表
    * @return 用户城市得分表
    */
  def getSumScore(travelScore: DataFrame, searchScore: DataFrame): (DataFrame) = {

    //这里用外连接 只搜索或只旅行都有得分
    travelScore.createOrReplaceTempView("travel_score")
    searchScore.createOrReplaceTempView("search_score")
    val joinInfo = travelScore.sqlContext.sql("select * from travel_score t1 outer join search_score t2 on " +
      "t2.qunar_username == t1.qunar_username and t2.arr_city == t1.arr_city")
    val scoreRDD = joinInfo.rdd.map(row => {
      var score: Double = 0
      if (null == row.get(2)) {
        score = row.getDouble(5)
      } else if (null == row.get(5)) {
        score = row.getDouble(2)
      } else {
        score = row.getDouble(2) + row.getDouble(5)
      }
      Row.apply(row.getString(0), row.getString(1), score)
    })
    val scoreSchema = StructType(List(
      StructField("username", StringType, nullable = false),
      StructField("arr_city", StringType, nullable = false),
      StructField("score", DoubleType, nullable = false)
    ))
    val scoreDF = joinInfo.sqlContext.createDataFrame(scoreRDD, scoreSchema)

    scoreDF
  }

  /**
    * 计算用户标签得分矩阵
    *
    * @param drBean drBean
    * @return 用户标签得分矩阵
    */
  def getUserLabelScoreMatrix(drBean: DRBean): CoordinateMatrix = {

    //搜索一次得1分 旅行一次得4分 username | arr_city | score
    drBean.getDomOrder.sqlContext.udf.register("multiplicative_2", (label: Double, score: Double) => {
      label * score
    })
    val domScoreDF = getScoreDF(drBean.getOrderTravelScore, drBean.getDomOrder,
      drBean.getDomFlightSearchInfo, domestic = true)
    val interScoreDF = getScoreDF(null, drBean.getInterOrder, drBean.getInterFlightSearchInfo, domestic = false)
    //将城市替换成城市标签得分 不用关注城市顺序,因为最终求城市得分均值
    val domCityLabelScore = domScoreDF.join(drBean.getDomLabelScore, Array("arr_city"), "inner")
    val interCityLabelScore = interScoreDF.join(drBean.getInterLabelScore, Array("arr_city"), "inner")
    domCityLabelScore.createOrReplaceTempView("dom_city_label_score")
    interCityLabelScore.createOrReplaceTempView("inter_city_label_score")
    //用户国内城市标签得分
    val domUserLabelScoreTemp = domCityLabelScore.sqlContext.sql(DRConstant.DOM_SUM_LABEL_SCORE).cache()
    val domUserLabelScore = domUserLabelScoreTemp.groupBy("username").mean(DRConstant.USER_MEAN_SQL.split(","): _*)
    //用户国际城市标签得分
    val interUserLabelScoreTemp = interCityLabelScore.sqlContext.sql(DRConstant.INTER_SUM_LABEL_SCORE)
    val interUserLabelScore = interUserLabelScoreTemp.groupBy("username").mean(DRConstant.USER_MEAN_SQL.split(","): _*)

    val userLabelScore = domUserLabelScore.union(interUserLabelScore).groupBy("username").mean(DRConstant.MEAN_SQL.split(","): _*)

    userLabelScore.select("username").rdd.zipWithIndex().collect().foreach(f => {
      userMap += (f._2 -> f._1.getString(0))
    })

    //转换成矩阵
    val userLabelScoreEntry = userLabelScore.rdd.zipWithIndex().flatMap(f => {
      val array = new Array[MatrixEntry](f._1.length - 1)
      for (i <- 1 until f._1.length) {
        array(i - 1) = new MatrixEntry(f._2, i - 1, f._1.getDouble(i))
      }
      array
    })
    val userLabelScoreMatix = new CoordinateMatrix(userLabelScoreEntry)

    userLabelScoreMatix.transpose()
  }


  /**
    * 获取用户城市得分
    *
    * @param cityScore   城市标签得分
    * @param travelOrder 旅行得分
    * @param searchInfo  搜索信息
    * @param domestic    是否国内
    * @return 用户城市总得分表
    */
  def getScoreDF(cityScore: DataFrame, travelOrder: DataFrame, searchInfo: DataFrame,
                 domestic: Boolean = true): DataFrame = {

    //1.旅行得分
    val travelCount = travelOrder.groupBy("qunar_username", "arr_city").count()
    val travelScoreTemp = travelCount.select(travelCount("qunar_username"), travelCount("arr_city"),
      travelCount("count") * 4)
      .withColumnRenamed("(count * 4)", "arrive_score")
    var travelScore: DataFrame = null
    if (domestic) {
      //城市旅行概率
      val cityMeanScore = cityScore.groupBy("arr_city").mean("score").withColumnRenamed("avg(score)", "score")
      //用户城市旅行得分=旅行得分*概率
      val tempTable = travelScoreTemp.join(cityMeanScore, Array("arr_city"), "inner")
      tempTable.createOrReplaceTempView("city_travel_score")
      travelScore = tempTable.sqlContext.sql("select qunar_username,arr_city,multiplicative_2(score,arrive_score)" +
        " as score from city_travel_score")
    } else {
      travelScore = travelScoreTemp.select(travelScoreTemp("qunar_username"), travelScoreTemp("arr_city"),
        travelScoreTemp("arrive_score").cast("Double"))
    }

    //4.搜索得分
    val searchScoreTemp = searchInfo.groupBy("qunar_username", "arr_city").count().withColumnRenamed("count", "search_count")
    val searchScore = searchScoreTemp.select(searchScoreTemp("qunar_username"), searchScoreTemp("arr_city"),
      searchScoreTemp("search_count").cast("Double"))
    //用户城市总得分
    val sumScore = getSumScore(travelScore, searchScore)

    sumScore
  }


  /**
    * 转换矩阵 to DataFrame
    *
    * @param userLabelScoreMatrix 用户标签得分矩阵
    * @param sqlContext           sqlContext
    * @return dataframe
    */
  def translateMatrix2DF(userLabelScoreMatrix: BlockMatrix, sqlContext: SQLContext): DataFrame = {

    val coorMatrixRDD = userLabelScoreMatrix.toIndexedRowMatrix().rows.flatMap(f => {
      val label = f.index
      val vector = f.vector
      val array = new Array[Row](vector.size)
      for (i <- 0 until vector.size) {
        array(i) = Row.merge(Row.apply(i.toLong), Row.apply(label), Row.apply(vector(i)))
      }
      array
    })

    val coorMatrixSchema = StructType(List(
      StructField("username", LongType, nullable = false),
      StructField("label", LongType, nullable = false),
      StructField("score", DoubleType, nullable = false)
    ))
    val resultDF = sqlContext.createDataFrame(coorMatrixRDD, coorMatrixSchema)

    resultDF
  }

  /**
    * 保存结构
    *
    * @param domResultDF   domResultDF
    * @param interResultDF interResultDF
    * @param sqlContext    sqlContext
    */
  def saveToES(domResultDF: DataFrame, interResultDF: DataFrame, sqlContext: SQLContext) {

    saveUserTop3LabelToES(domResultDF)
    saveUserTop3LabelToES(interResultDF, domestic = false)
    saveLabelTop10CityToES(sqlContext)
    saveLabelTop10CityToES(sqlContext, domestic = false)

    saveCityLabelScoreToES()
    saveCityLabelScoreToES(false)
    saveUserLabelScoreToES(domResultDF)
    saveUserLabelScoreToES(interResultDF, domestic = false)

  }

  /**
    * 用户标签得分矩阵 * 标签余弦相似度矩阵
    *
    * @param domLabelMatrix   国内城市标签得分矩阵
    * @param interLabelMatrix 国际城市标签得分矩阵
    * @param userMatrix       用户标签得分
    * @param spark            spark
    * @return (domResultDF, interResultDF)
    */
  def matrixMultiply(domLabelMatrix: CoordinateMatrix, interLabelMatrix: CoordinateMatrix, userMatrix: IndexedRowMatrix,
                     spark: SparkSession): (DataFrame, DataFrame) = {

    val domLabelArr = domLabelMatrix.toIndexedRowMatrix().rows.collect()
    val domBroadCast = spark.sparkContext.broadcast(domLabelArr)
    val domResultDF = multiply(domBroadCast, userMatrix, spark.sqlContext).cache()
    domBroadCast.destroy()
    val interLabelArr = interLabelMatrix.toIndexedRowMatrix().rows.collect()
    val interBroadCast = spark.sparkContext.broadcast(interLabelArr)
    val interResultDF = multiply(interBroadCast, userMatrix, spark.sqlContext).cache()
    interBroadCast.destroy()

    (domResultDF, interResultDF)
  }


  /**
    * 记录类成员变量
    *
    * @param interCityLabelScore 国际城市标签得分
    * @param domCityLabelScore   国内城市标签得分
    */
  def rememberData(interCityLabelScore: DataFrame, domCityLabelScore: DataFrame) {

    //label行号 -> label名称
    val interLabelArr = DRConstant.LABEL_NAME.split(",")
    for (i <- 0 until interLabelArr.length) {
      labelMap += (i.asInstanceOf[Long] -> interLabelArr(i))
    }
    //国际城市行号 -> 城市名
    interCityLabelScore.select("arr_city").rdd.zipWithIndex().collect().foreach(f => {
      interCityMap += (f._2 -> f._1.getString(0))
    })
    interCityLabelScoreDF = interCityLabelScore
    //国内城市行号 -> 城市名
    domCityLabelScore.select("arr_city").rdd.zipWithIndex().collect().foreach(f => {
      domCityMap += (f._2 -> f._1.getString(0))
    })

    domCityLabelScoreDF = domCityLabelScore
  }


  /**
    * dataframe转置
    *
    * @param df df
    * @return resultDF
    */
  def transposeDF(df: DataFrame): DataFrame = {

    val matrixEntryRDD = df.rdd.zipWithIndex().flatMap(f => {
      val array = new Array[MatrixEntry](f._1.length - 1)
      for (i <- 1 until f._1.length) {
        array(i - 1) = new MatrixEntry(f._2, i - 1, f._1.getDouble(i))
      }
      array
    })
    val tranMatrix = new CoordinateMatrix(matrixEntryRDD).transpose()
    val coorMatrixRDD = tranMatrix.entries.map(entry => {
      Row.apply(entry.i, entry.j, entry.value)
    })
    val coorMatrixSchema = StructType(List(
      StructField("label", LongType, nullable = false),
      StructField("city", LongType, nullable = false),
      StructField("score", DoubleType, nullable = false)
    ))
    val resultDF = df.sqlContext.createDataFrame(coorMatrixRDD, coorMatrixSchema)

    resultDF
  }

  /**
    * 保存用户top3标签表
    *
    * @param resultDF 用户top3标签表
    * @param domestic 是否国内
    */
  def saveUserTop3LabelToES(resultDF: DataFrame, domestic: Boolean = true) {

    var top3RDD: RDD[Row] = null
    var esPath: String = null
    val user = userMap
    val label = labelMap
    top3RDD = resultDF.rdd.groupBy(f => f.getLong(0)).map(f => {
      val username = user(f._1)
      val top3 = f._2.toArray.sortWith((r1, r2) => {
        r1.getDouble(2) > r2.getDouble(2)
      }).take(3).map(_.getLong(1))
      Row.apply(username, label(top3(0)), label(top3(1)), label(top3(2)))
    }).coalesce(1)
    if (domestic) {
      esPath = "destination_recommend/dom_user_top3_label_info"
    } else {
      esPath = "destination_recommend/inter_user_top3_label_info"
    }
    val schema = StructType(List(
      StructField("username", StringType, nullable = false),
      StructField("top1Label", StringType, nullable = false),
      StructField("top2Label", StringType, nullable = false),
      StructField("top3Label", StringType, nullable = false)
    ))
    val top3LabelDF = resultDF.sqlContext.createDataFrame(top3RDD, schema)
    println("user count: " + top3LabelDF.count())
    val mappingDF = top3LabelDF.withColumn("mapping_id", top3LabelDF("username"))

    EsSparkSQL.saveToEs(mappingDF, esPath)
  }

  /**
    * 保存标签得分top10城市表
    *
    * @param sqlContext sqlContext
    * @param domestic   是否国内
    */
  def saveLabelTop10CityToES(sqlContext: SQLContext, domestic: Boolean = true) {

    var top10cityRDD: RDD[Row] = null
    var esPath: String = null
    var labelScoreDF: DataFrame = null
    var cityMap: HashMap[Long, String] = null
    var labelMap2Save: HashMap[Long, String] = null
    if (domestic) {
      labelScoreDF = transposeDF(domCityLabelScoreDF)
      cityMap = domCityMap
      labelMap2Save = labelMap
      esPath = "destination_recommend/dom_label_top10_city_info"
    } else {
      labelScoreDF = transposeDF(interCityLabelScoreDF)
      cityMap = interCityMap
      labelMap2Save = labelMap
      esPath = "destination_recommend/inter_label_top10_city_info"
    }
    top10cityRDD = labelScoreDF.rdd.groupBy(_.getLong(0)).map(f => {
      val array = new Array[String](11)
      val label = labelMap2Save(f._1)
      array(0) = label
      val top10city = f._2.toArray.sortWith((r1, r2) => {
        r1.getDouble(2) > r2.getDouble(2)
      }).take(10).map(_.getLong(1))
      for (i <- 1 until 11) {
        array(i) = cityMap(top10city(i - 1))
      }
      Row.fromSeq(array)
    }).coalesce(1)

    val schema = StructType(List(
      StructField("label", StringType, nullable = false),
      StructField("city1", StringType, nullable = false),
      StructField("city2", StringType, nullable = false),
      StructField("city3", StringType, nullable = false),
      StructField("city4", StringType, nullable = false),
      StructField("city5", StringType, nullable = false),
      StructField("city6", StringType, nullable = false),
      StructField("city7", StringType, nullable = false),
      StructField("city8", StringType, nullable = false),
      StructField("city9", StringType, nullable = false),
      StructField("city10", StringType, nullable = false)
    ))
    val top10cityDF = sqlContext.createDataFrame(top10cityRDD, schema)
    val mappingDF = top10cityDF.withColumn("mapping_id", top10cityDF("label"))

    EsSparkSQL.saveToEs(mappingDF, esPath)
  }

  /**
    * 保存用户标签得分表
    *
    * @param resultDF 用户标签得分表
    * @param domestic 是否国内
    */
  def saveUserLabelScoreToES(resultDF: DataFrame, domestic: Boolean = true) {

    var esPath: String = null
    var schema: StructType = null
    var rowRDD: RDD[Row] = null
    val userMap2Save = userMap
    rowRDD = resultDF.rdd.groupBy(_.getLong(0)).map(f => {
      val userName = userMap2Save(f._1)
      val arr = f._2.toArray.sortWith((r1, r2) => {
        r1.getLong(1) > r2.getLong(1)
      }).map(_.getDouble(2))
      Row.merge(Row.apply(userName), Row.fromSeq(arr))
    }).coalesce(1)
    val schemaArray = DRConstant.LABEL_NAME.split(",")
    val list = new util.ArrayList[StructField](32)
    list.add(0, StructField("username", StringType, nullable = false))
    for (i <- 1 until (schemaArray.length + 1)) {
      list.add(i, StructField(schemaArray(i - 1), DoubleType, nullable = false))
    }
    schema = StructType(list)
    if (domestic) {
      esPath = "destination_recommend/dom_user_label_score_info"
    } else {
      esPath = "destination_recommend/inter_user_label_score_info"
    }
    val userLabelScoreDF = resultDF.sqlContext.createDataFrame(rowRDD, schema)
    val mappingDF = userLabelScoreDF.withColumn("mapping_id", userLabelScoreDF("username"))

    EsSparkSQL.saveToEs(mappingDF, esPath)
  }

  /**
    * 保存城市标签得分表
    *
    * @param domestic 是否国内
    */
  def saveCityLabelScoreToES(domestic: Boolean = true) {

    var df: DataFrame = null
    var esPath: String = null
    if (domestic) {
      df = domCityLabelScoreDF.toDF(DRConstant.CITY_LABEL_SCHEMA.split(","): _*)
      esPath = "destination_recommend/dom_city_label_score_info"
    } else {
      df = interCityLabelScoreDF.toDF(DRConstant.CITY_LABEL_SCHEMA.split(","): _*)
      esPath = "destination_recommend/inter_city_label_score_info"
    }
    val mappingDF = df.withColumn("mapping_id", df("arr_city"))

    EsSparkSQL.saveToEs(mappingDF, esPath)
  }


  /**
    * 从hive 读取相关数据
    *
    * @param spark spark
    * @param date  搜索日期
    * @return drBean
    */
  def getAllData(spark: SparkSession, date: String): DRBean = {

    //国内四张相关表
    val domLabelScoreHive = "select * from f_fuwu.domestic_label_score"
    val flightSearchInfoHive = s"select username as qunar_username,arr_city,dom_inter from f_wide.wide_flightsearchinfo" +
      s" where dt >= $date and username is not null and arr_city is not null"
    val orderTravelScoreHive = "select arr_city,score from f_fuwu.order_travel_score where  arr_city is not null and score > 0"
    val wideOrderHive = "select qunar_username,arr_city,dom_inter from f_wide.wide_order where qunar_username is not " +
      "null and arr_city is not null"
    val interLableScoreHive = "select * from f_fuwu.inter_label_score"
    val domLabelScore = spark.sql(domLabelScoreHive)
    val flightSearchInfo = spark.sql(flightSearchInfoHive)
    val orderTravelScore = spark.sql(orderTravelScoreHive)
    val wideOrder = spark.sql(wideOrderHive)
    val interLabelScore = spark.sql(interLableScoreHive)

    flightSearchInfo.createOrReplaceTempView("flight_search_info")
    val domFlightSearchInfo = spark.sql("select * from flight_search_info where dom_inter == 0")
    val interFlightSearchInfo = spark.sql("select * from flight_search_info where dom_inter == 1")

    wideOrder.createOrReplaceTempView("wide_order")
    val domOrder = spark.sql("select * from wide_order where dom_inter == 0")
    val interOrder = spark.sql("select * from wide_order where dom_inter == 1")

    val drBean = new DRBean
    drBean.setDomOrder(domOrder)
    drBean.setInterOrder(interOrder)
    drBean.setDomFlightSearchInfo(domFlightSearchInfo)
    drBean.setInterFlightSearchInfo(interFlightSearchInfo)
    drBean.setDomLabelScore(domLabelScore)
    drBean.setInterLabelScore(interLabelScore)
    drBean.setOrderTravelScore(orderTravelScore)

    drBean
  }

  /**
    * 笛卡尔积求矩阵乘法   计算量太大 太慢
    *
    * @param A          A矩阵
    * @param B          B矩阵
    * @param sqlContext sqlContext
    * @return 矩阵乘积
    */
  def multiply(A: CoordinateMatrix, B: CoordinateMatrix, sqlContext: SQLContext): DataFrame = {
    //31*31
    val lift = A.entries.map(f => {
      (f.i, (f.j, f.value))
    })
    //31*usercount
    val right = B.entries.map(f => {
      (f.j, (f.i, f.value))
    })

    val mutil = lift.cartesian(right).filter(f => {
      f._1._2 == f._2._2
    }).map(x => ((x._1._2._1, x._2._2._1), x._1._2._2 * x._2._2._2)).reduceByKey(_ + _)

    val rowRDD = mutil.map(f => {
      Row.apply(f._1._1, f._1._2, f._2)
    })
    val coorMatrixSchema = StructType(List(
      StructField("label", LongType, nullable = false),
      StructField("username", LongType, nullable = false),
      StructField("score", DoubleType, nullable = false)
    ))
    val resultDF = sqlContext.createDataFrame(rowRDD, coorMatrixSchema)

    resultDF
  }

  /**
    * 比笛卡尔积快点 还是很慢
    *
    * @param A        矩阵A
    * @param B        矩阵B
    * @param sqlContext sqlContext
    * @return resultDF
    */
  def multiply(A: IndexedRowMatrix, B: IndexedRowMatrix, sqlContext: SQLContext): DataFrame = {

    val rowRDD = A.rows.cartesian(B.rows).map(f => {
      val index1 = f._1
      val index2 = f._2
      val value = index1.vector.toArray.zip(index2.vector.toArray).map(f => f._1 * f._2).sum
      Row(index1.index, index2.index, value)
    })
    val coorMatrixSchema = StructType(List(
      StructField("label", LongType, nullable = false),
      StructField("username", LongType, nullable = false),
      StructField("score", DoubleType, nullable = false)
    ))
    val resultDF = sqlContext.createDataFrame(rowRDD, coorMatrixSchema)

    resultDF
  }


  /**
    * 广播变量求矩阵乘法  很快
    *
    * @param broadValue 广播变量
    * @param userMatrix 用户标签得分矩阵
    * @param sqlContext sqlContext
    * @return
    */
  private def multiply(broadValue: Broadcast[Array[IndexedRow]], userMatrix: IndexedRowMatrix,
                       sqlContext: SQLContext): DataFrame = {
    val rowRDD = userMatrix.rows.flatMap(f => {
      val indexRowArr = broadValue.value
      val array = new Array[Row](indexRowArr.length)
      for (i <- indexRowArr.indices) {
        val indexA = indexRowArr(i)
        val value = indexA.vector.toArray.zip(f.vector.toArray).map(f => f._1 * f._2).sum
        array(i) = Row(f.index, indexA.index, value)
      }
      array
    })
    val coorMatrixSchema = StructType(List(
      StructField("username", LongType, nullable = false),
      StructField("label", LongType, nullable = false),
      StructField("score", DoubleType, nullable = false)
    ))
    val resultDF = sqlContext.createDataFrame(rowRDD, coorMatrixSchema)

    resultDF
  }
}