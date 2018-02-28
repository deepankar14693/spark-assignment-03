import DataFrameApplication._
import Operations._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.functions._

case class Football(HomeTeam: String,AwayTeam: String,FTHG: Int,FTAG: Int,FTR: String)

object DataSetApplication extends App {

 Logger.getLogger("org").setLevel(Level.OFF)

 val spark = SparkSession.builder().config(conf).getOrCreate()

 val soccerData = readFootballData(spark)

 import spark.implicits._

 /**
   * dataset creation
   */
 val footballDataSet = soccerData.map(row => Football(row.getString(2),row.getString(3)
,row.getString(4).toInt,row.getString(5).toInt,row.getString(6)))

 footballDataSet.show()

 footballDataSet.select($"HomeTeam").union(footballDataSet.select($"AwayTeam")).groupBy($"HomeTeam")
  .count().show()

 val homeTeam = footballDataSet.select ("HomeTeam", "FTR").where ("FTR = 'H'").groupBy ("HomeTeam").count ().withColumnRenamed ("count", "HomeWins")
 val awayTeam = footballDataSet.select ("AwayTeam", "FTR").where ("FTR = 'A'").groupBy ("AwayTeam").count ().withColumnRenamed ("count", "AwayWins")
 val teams = homeTeam.join (awayTeam, homeTeam.col ("HomeTeam") === awayTeam.col ("AwayTeam"))
 val sum: (Int, Int) => Int = (HomeMatches: Int, TeamMatches: Int) => HomeMatches + TeamMatches
 val total = udf (sum)
 val ten = 10
 teams.withColumn ("TotalWins", total (col ("HomeWins"), col ("AwayWins"))).select ("HomeTeam", "TotalWins")
  .withColumnRenamed ("HomeTeam", "Team").sort (desc ("TotalWins")).limit (ten).show ()






//val footballDataSet = soccerData.select("HomeTeam","AwayTeam","FTHG","FTAG","FTR")
//  .toDF("HomeTeam","AwayTeam","FTHG","FTAG","FTR")
// footballDataSet.show()

 import spark.implicits._

// val soccerDataSet= footballDataSet.as[Football]
// soccerDataSet.show()




}
