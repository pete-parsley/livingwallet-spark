import java.util
import java.util.Calendar
import java.sql.Timestamp
import java.text.{ParsePosition, SimpleDateFormat}
import java.util.concurrent.TimeUnit

import org.apache.log4j.{Level, Logger}
import org.influxdb.InfluxDBFactory
import org.influxdb.dto.{BatchPoints, Point, Query}

import scala.collection.JavaConverters
import org.apache.spark.sql._
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType, TimestampType}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._



object SparkInfluxDB {

  def main(args: Array[String]): Unit = {

    var dT = Calendar.getInstance()
    var currentHour = dT.getTime()
    println(currentHour)


    val spark = SparkSession.builder
      .appName("SparkSessionExample")
      .master("local[*]")
      .getOrCreate

    spark.sparkContext.setLogLevel("WARN")
    Logger.getRootLogger.setLevel(Level.ERROR)

    var sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'")
    var influxdb = InfluxDBFactory.connect("http://localhost:8087").setDatabase("livingwallet")

    val queryBuy = new Query("SELECT short_name, number_units, pricing FROM currency_asset_transactions WHERE time <= now() AND transaction_type='BUY'")
    val responseBuy = influxdb.query(queryBuy)


    val querySell = new Query("SELECT short_name, number_units FROM currency_asset_transactions " +
      "WHERE time <= now() AND transaction_type='SELL'")
    val responseSell = influxdb.query(querySell)


    //BUY records to Row and DF
    val responseBuyIterator = JavaConverters.asScalaIterator(responseBuy.getResults.get(0).getSeries.get(0).getValues.iterator())
    var buyList = new util.ArrayList[Row] {}


    while (responseBuyIterator.hasNext) {
      val responseRow = responseBuyIterator.next()
      val row = Row(new Timestamp(sdf.parse(responseRow.get(0).toString).getTime), responseRow.get(1), responseRow.get(2), responseRow.get(3))
      buyList.add(row)
    }


    //SELL records to Row and DF
    val responseSellIterator = JavaConverters.asScalaIterator(responseSell.getResults.get(0).getSeries.get(0).getValues.iterator())
    var sellList = new util.ArrayList[Row] {}
    while (responseSellIterator.hasNext) {
      val responseRow = responseSellIterator.next()
      val row = Row(new Timestamp(sdf.parse(responseRow.get(0).toString).getTime), responseRow.get(1), responseRow.get(2))
      sellList.add(row)
    }

    val schemaBuy = StructType(
      List(StructField("time", TimestampType, false),
        StructField("short_name", StringType, true),
        StructField("number_units", DoubleType, true),
        StructField("pricing", DoubleType, true)))
    val buyDF = spark.createDataFrame(buyList, schemaBuy)


    val schemaSell = StructType(
      List(StructField("time", TimestampType, false),
        StructField("short_name", StringType, true),
        StructField("number_units", DoubleType, true)))
    val sellDF = spark.createDataFrame(sellList, schemaSell)
    println(sellDF.printSchema())

    buyDF.createTempView("buy")
    sellDF.createTempView("sell")


    println("RESULT JOINED")
    val resultJoined = spark.sql("select a.time, a.currency, a.qty, a.pricing, a.qty_buy_sum, nvl(b.qty_sum_sell,0), a.qty_buy_sum + nvl(b.qty_sum_sell,0.0) qty_remainder from " +
      " (select time, short_name currency, number_units qty, pricing, sum(number_units) over (partition by short_name order by time) qty_buy_sum from buy order by time) a " +
      "left join (select short_name currency,sum(number_units) qty_sum_sell from sell group by currency) b on a.currency=b.currency")
    resultJoined.foreach(row => println(row))
    println(resultJoined.printSchema())
    val resultPricing = resultJoined.filter("qty_remainder > 0")
    resultPricing.show()
    val windowSpec = Window.partitionBy("currency").orderBy("time")
    val pricingRowNum = resultPricing.withColumn("row_number", row_number.over(windowSpec))
    val pricingFinal = pricingRowNum.withColumn("qty_balance", when(col("row_number") === 1, col("qty_remainder")).otherwise(col("qty")))
    val pricingFinalValue = pricingFinal.withColumn("value_balance",expr("qty_balance * pricing"))
    val pricingAggrValue = pricingFinalValue.groupBy("currency").sum("qty_balance","value_balance")
    pricingAggrValue.show()

    //write Points to influxdb
    var batchPoints = BatchPoints
      .database("livingwallet")
      .build()


    val pricingToWrite = pricingAggrValue.collect()

    for (row <- pricingToWrite) {

      var point = Point.measurement("currency_balance")
        .time(System.currentTimeMillis(), TimeUnit.MILLISECONDS)
        .tag("currency", row.get(0).asInstanceOf[String])
        .addField("qty_balance",row.get(1).asInstanceOf[java.lang.Double])
        .addField("value_balance", row.get(2).asInstanceOf[java.lang.Double])
        .build()

      batchPoints.point(point)
    }

    influxdb.write(batchPoints)
  }
}
