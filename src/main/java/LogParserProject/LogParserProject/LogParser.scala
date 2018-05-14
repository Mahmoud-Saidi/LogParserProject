package LogParserProject.LogParserProject

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext

object LogParser {

case class LogRecord(time : String, viewmode : String, zoom : String)

def parseLogLine(log: String): LogRecord = {

		val logArray = log.split("/")   
				if (logArray.size < 9) {
					LogRecord(logArray(0),"Empty","Empty")
				} else {
					LogRecord(logArray(0),logArray(4),logArray(6))
				}
}


def main(args: Array[String]) {

	val conf = new SparkConf().setAppName("LogParser").setMaster("local[*]")
			val sc = new SparkContext(conf)
			val sqlContext = new SQLContext(sc)
			import sqlContext.implicits._

			//lecture du csv 
			val log = sc.textFile(args(0))

			//transformer les logs en Dataframe
			val logTable = log.map(line => parseLogLine(line)).toDF()

			//enregistrer le dataframe sous forme d'une table temporaire
			logTable.registerTempTable("logTable")

			val result1 = sqlContext.sql("""SELECT viewmode, count(*) as number, max(time) as max_time
					FROM
					(SELECT `logTable`.*, (row_number() over (ORDER BY time) - row_number() over (partition BY viewmode ORDER BY time)) AS grp
					FROM `logTable`) t 
					GROUP BY viewmode,grp
					ORDER BY max_time""")


			// Affichage du premier resultat
			result1.filter(result1("viewmode").!==("Empty"))
			.collect().foreach(row => println(row.getString(0)+"\t"+row.getString(1)))

			val result2 = sqlContext.sql("""SELECT viewmode, count(*) as number, max(time) as max_time, concat_ws(",",collect_list("zoom"))
					FROM
					(SELECT `logTable`.*, (row_number() over (ORDER BY time) - row_number() over (partition BY viewmode ORDER BY time)) AS grp
					FROM `logTable`) t 
					GROUP BY viewmode,grp
					ORDER BY max_time""")

			// Affichage du second resultat
			result2.filter(result1("viewmode").!==("Empty")).
			collect().foreach(row => println(row.getString(0)+"\t"+row.getString(1)+"\t"+row.getString(3)))


}
}