import Operations._
import org.apache.spark.sql._

object DataFrameApplication extends App {

 val spark = SparkSession.builder().config(conf).getOrCreate()

 val footballData= readFootballData(spark)//.groupBy("HomeTeam").count()
 footballData.show()
 footballData.groupBy("HomeTeam").count().show()

 footballData.createOrReplaceTempView("FootballTable")

 val sqlDf = spark.sql("Select FTR, (Count(FTR)* 100 / (Select Count(*) From FootballTable)) as Score From FootballTable Group By FTR")
sqlDf.show()
 // val c = footballData.selectExpr("FTR, (Count(FTR)* 100 / (Select Count(*))) as Score").groupBy("FTR")


}
