import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

object Operations {

 val conf = new SparkConf()
 conf.setMaster("local")
 conf.setAppName(this.getClass.getName)

 val csvPath = "src/main/resources/D1.csv"

 /**
   *
   * @param spark reference of sparl session
   * @return dataframe created by using csv file
   */
 def readFootballData(spark: SparkSession): DataFrame = {
  //spark.read.option("header", "true").option("Inferschema", "true").csv(csvPath)
  spark.read.format("csv").option("header","true").load(csvPath)
 }

}
