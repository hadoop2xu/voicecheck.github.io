package com.qunar.flight.fuwu.data.cf.desrecommend

import org.apache.spark.sql.DataFrame

import scala.beans.BeanProperty


/**
  * @author hadoop.xu
  */

class DRBean {

  @BeanProperty var domOrder: DataFrame = _
  @BeanProperty var interOrder: DataFrame = _
  @BeanProperty var domFlightSearchInfo: DataFrame = _
  @BeanProperty var interFlightSearchInfo: DataFrame = _
  @BeanProperty var domLabelScore: DataFrame = _
  @BeanProperty var interLabelScore: DataFrame = _
  @BeanProperty var orderTravelScore: DataFrame = _
}
