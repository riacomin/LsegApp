package org.lseg.exercise

import org.apache.avro.io.Encoder
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{asc, desc, expr, from_unixtime, greatest, least, lit, max, min, monotonically_increasing_id, rank, row_number, sum, to_date, when}
import org.apache.spark.sql.types.{DoubleType, IntegerType, LongType, StringType, StructField, StructType, TimestampType}
import org.apache.spark.sql.{DataFrame, Dataset, Encoders, Row, SparkSession, functions}

import java.io.File
import java.sql.Timestamp


object LsegMain extends SparkTrait {

  case class OrderCC(orderID: Long, userName: String, orderTime: String,orderType: String, quantity: Long, Price: Long)

  def main(args: Array[String]): Unit = {

    println(
      """=======================
        |-----------------------
        |----ORDER MATCHER------
        |-----------------------
        |=======================
        |""".stripMargin)

      // NOT USED
      val simpleSchema = StructType(Array(
        StructField("orderId",StringType,nullable = true),
        StructField("userName",StringType,nullable = true),
        StructField("orderTime", StringType, nullable = true),
        StructField("quantity", StringType, nullable = true),
        StructField("price", StringType, nullable = true)
      ))

    import sparkSession.implicits._
    val ordersDF = sparkSession.read
      .format("com.databricks.spark.csv")
      .csv("src/resources/exampleOrders.csv")
//      .as[OrderCC]
    // This is not working on Windows with winutils : Unable to find encoder for type OrderCC. An implicit Encoder[OrderCC] is needed to store OrderCC instances in a Dataset. Primitive types (Int, String, etc) and Product types (case classes) are supported by importing spark.implicits._  Support for serializing other types will be added in future releases.
                                                          //      .as[OrderCC]

    val ordersDFwcol = ordersDF.withColumn("_c0", ordersDF.col("_c0").cast(LongType))
      .withColumn("_c2",ordersDF.col("_c2").cast(StringType))
      .withColumn("_c4",ordersDF.col("_c4").cast(LongType))
      .withColumn("_c5",ordersDF.col("_c5").cast(LongType))
      .withColumnRenamed("_c0", "Order ID")
      .withColumnRenamed("_c1", "User Name")
      .withColumnRenamed("_c2", "Order Time")
      .withColumnRenamed("_c3", "Order Type")
      .withColumnRenamed("_c4", "Quantity")
      .withColumnRenamed("_c5", "Price").cache()

    val sellDF = ordersDFwcol.filter($"Order Type" ==="SELL" )
    val buyDF = ordersDFwcol.filter($"Order Type" ==="BUY" )
    val buyW = Window.partitionBy("Quantity").orderBy($"price")
    val sellW = Window.partitionBy("Quantity").orderBy(-$"price")

    val orderBook = ordersDFwcol.withColumnRenamed("Quantity","Quantity-OB")
      .withColumnRenamed("Order ID","Order ID-OB")
      .withColumnRenamed("Order Time","Order Time-OB")
      .withColumnRenamed("Price","Price-OB")
      .withColumnRenamed("Order Type","Order Type-OB")
      .withColumnRenamed("User Name","User Name-OB")

    val bdf = buyDF.join(orderBook, $"Quantity"===$"Quantity-OB" &&
      $"Order ID" =!= $"Order ID-OB" && $"Order Type" =!= $"Order Type-OB", "inner")
      .withColumn("bestprice",functions.min($"Price-OB").over(buyW))
      .withColumn("bestprice",$"bestprice".cast(IntegerType))
      .filter($"Price-OB" >= least($"bestprice", $"Price"))
      bdf.printSchema()

    val bdfw  = bdf

    bdfw.printSchema()

    val minBuyValue: Any =bdfw.agg(min("bestprice")).first().get(0)
    val bbfu = bdfw.filter($"Price-OB" === minBuyValue)

    val sdf =  sellDF.join(orderBook, $"Quantity"===$"Quantity-OB" &&
      $"Order ID" =!= $"Order ID-OB" && $"Order Type" =!= $"Order Type-OB", "inner")
      .withColumn("bestprice",functions.max($"Price-OB").over(sellW))
      .withColumn("bestprice",$"bestprice".cast(IntegerType))

    val maxSellValue: Any =sdf.agg(max("bestprice")).first().get(0)
    val sbfu = sdf.filter($"Price-OB" === maxSellValue)

    val completedOrdersOutput = bbfu.union(sbfu)

    completedOrdersOutput.select("Order ID", "Order ID-OB","Order Time","Quantity", "bestprice").coalesce(1)
      .write.format("com.databricks.spark.csv")
      .mode("overwrite").save("output/outputExampleMatches.csv")

    /***
     * Partial match:
     */

    val incomingOrdersDF = ordersDFwcol // // Since we have only 1 source, we compare to each other but do not compare same Order ID orders
    incomingOrdersDF.printSchema()
    orderBook.printSchema()
    val incomingOrderDFSell = incomingOrdersDF.filter($"Order Type" === "SELL" )

    val  incomingOrderDFBuy = incomingOrdersDF.filter(
      $"Order Type" === "BUY" )

    val possiblePartialMatchesBuy = incomingOrderDFBuy.join(orderBook,
      ((incomingOrdersDF.col("Order Type") ==="BUY" &&
        incomingOrdersDF.col("Price") >= orderBook.col("Price-OB")) &&
          incomingOrdersDF.col("Quantity") < orderBook.col("Quantity-OB")) &&
      incomingOrdersDF.col("Order ID") =!= orderBook.col("Order ID-OB") &&
      incomingOrdersDF.col("Order Type") =!= orderBook.col("Order Type-OB") &&
        incomingOrdersDF.col("User Name") =!= orderBook.col("User Name-OB") &&
        incomingOrdersDF.col("Order Time") < orderBook.col("Order Time-OB"),
      "inner")
      .withColumn("Price Diff", incomingOrderDFSell.col("Price") - orderBook.col("Price-OB"))
      .withColumn("LargestQunatity", orderBook.col("Quantity-OB") - incomingOrderDFSell.col("Quantity") )
      .filter($"Order Time" < $"Order Time-OB")
      .withColumn("rnprice", rank() over Window
        .partitionBy("Order ID")
        .orderBy(-$"LargestQunatity") )
    .withColumn("rankb", row_number() over Window
      .partitionBy("Order ID-OB")
      .orderBy(-$"Price-OB",-$"Price Diff"))
      .orderBy( -$"Price-OB", -$"Price Diff")

    val possiblePartialMatchesSell = incomingOrderDFSell.join(orderBook,
      ( (incomingOrdersDF.col("Order Type") ==="SELL" &&
        incomingOrdersDF.col("Price")  <= orderBook.col("Price-OB")
        && incomingOrdersDF.col("Quantity") < orderBook.col("Quantity-OB")) ||
        (incomingOrdersDF.col("Order Type") ==="BUY" &&
          incomingOrdersDF.col("Price") >= orderBook.col("Price-OB")) &&
          incomingOrdersDF.col("Quantity") < orderBook.col("Quantity-OB")) &&
        incomingOrdersDF.col("Order ID") =!= orderBook.col("Order ID-OB") &&
        incomingOrdersDF.col("Order Type") =!= orderBook.col("Order Type-OB") &&
        incomingOrdersDF.col("User Name") =!= orderBook.col("User Name-OB"),
      "inner")
      .withColumn("Price Diff", orderBook.col("Price-OB") - incomingOrderDFSell.col("Price"))
      .withColumn("LargestQunatity",  orderBook.col("Quantity-OB")-incomingOrderDFSell.col("Quantity"))
      .filter($"Order Time" < $"Order Time-OB")
      .withColumn("rnprice", rank() over Window
        .partitionBy("Order ID")
        .orderBy(-$"LargestQunatity") )
      .withColumn("rankb", row_number() over Window
      .partitionBy("Order ID-OB")
      .orderBy(-$"Price-OB",-$"Price Diff"))
      .orderBy( -$"Price-OB", -$"Price Diff")

    incomingOrderDFBuy.join(orderBook,
      ((incomingOrdersDF.col("Order Type") ==="BUY" &&
        incomingOrdersDF.col("Price") >= orderBook.col("Price-OB")) &&
        incomingOrdersDF.col("Quantity") < orderBook.col("Quantity-OB")) &&
        incomingOrdersDF.col("Order ID") =!= orderBook.col("Order ID-OB") &&
        incomingOrdersDF.col("Order Type") =!= orderBook.col("Order Type-OB") &&
        incomingOrdersDF.col("User Name") =!= orderBook.col("User Name-OB") &&
        incomingOrdersDF.col("Order Time") < orderBook.col("Order Time-OB"),
      "inner")
      .withColumn("Price Diff", incomingOrderDFSell.col("Price") - orderBook.col("Price-OB"))
      .union(
    incomingOrderDFSell.join(orderBook,
      ( (incomingOrdersDF.col("Order Type") ==="SELL" &&
        incomingOrdersDF.col("Price")  <= orderBook.col("Price-OB")
        && incomingOrdersDF.col("Quantity") < orderBook.col("Quantity-OB")) ||
        (incomingOrdersDF.col("Order Type") ==="BUY" &&
          incomingOrdersDF.col("Price") >= orderBook.col("Price-OB")) &&
          incomingOrdersDF.col("Quantity") < orderBook.col("Quantity-OB")) &&
        incomingOrdersDF.col("Order ID") =!= orderBook.col("Order ID-OB") &&
        incomingOrdersDF.col("Order Type") =!= orderBook.col("Order Type-OB") &&
        incomingOrdersDF.col("User Name") =!= orderBook.col("User Name-OB"),
      "inner")
      .withColumn("Price Diff", orderBook.col("Price-OB") - incomingOrderDFSell.col("Price")))
      .withColumn("BetterSorB",row_number() over Window
        .partitionBy("Order ID-OB")
        .orderBy("Price Diff"))
      .orderBy("Order ID")

    val partialOrders = possiblePartialMatchesBuy.union(possiblePartialMatchesSell)
      .withColumn("rankBetweenBuySell", row_number() over Window .partitionBy(
        $"Order ID",$"Order ID-OB")
        .orderBy(-$"Price Diff"))
      .orderBy("Order Time-OB")

    partialOrders
      .withColumn("Quantity-PM", when($"rankb" === 1 && $"rnprice" === 1,$"Quantity-OB" - $"Quantity").otherwise($"Quantity-OB"))
      .filter($"rankb" === 1 && $"rnprice" === 1).drop($"Quantity-OB")
      .withColumnRenamed("Order ID-OB","Order ID-PM")

    partialOrders.filter($"rankb" === 1 && $"rnprice" === 1)
      .select($"Order ID-OB",$"Order ID", $"Order Time-OB", $"Quantity", $"Price").coalesce(1)
      .write.format("com.databricks.spark.csv")
      .mode("overwrite").save("output/outputExampleMatches_v2.csv")

    val remainingOrderBook = orderBook.join(partialOrders
      .withColumn("Quantity-PM",
        when($"rankb" === 1 && $"rnprice" === 1,$"Quantity-OB" - $"Quantity").otherwise($"Quantity-OB"))
      .drop($"Quantity-OB")
      .filter($"rankb" === 1 && $"rnprice" === 1)
      .select("Order ID-OB", "Quantity-PM")
      .withColumnRenamed("Order ID-OB","Order ID-PM")
      ,$"Order ID-OB" === $"Order ID-PM","leftouter")
      .withColumn("Final Quantity", when($"Quantity-PM".isNull,$"Quantity-OB").otherwise($"Quantity-PM"))
      .drop("Quantity-OB")
      .drop("Quantity-PM")
      .withColumnRenamed("Final Quantity", "Quantity-OB")
      .select("Order ID-OB","User Name-OB","Order Time-OB","Order Type-OB","Quantity-OB","Price-OB")
      .orderBy("Order ID-OB")

    remainingOrderBook
      .select("Order ID-OB","User Name-OB","Order Time-OB","Order Type-OB","Quantity-OB","Price-OB").coalesce(1)
      .write.format("com.databricks.spark.csv")
      .mode("overwrite").save("output/remainingOrderBook.csv")

    }

}
