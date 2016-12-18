/* Main.scala */
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.log4j.Logger
import org.apache.log4j.Level
import dictionarybuilder.DictionaryBuilder




object OpinionMining {

	
	
	/*example
	input: RDD["One    two. three, four   five-six"]
	output: RDD["one two three four five six"]
	*/
	def processReviews(reviews: org.apache.spark.rdd.RDD[String]):org.apache.spark.rdd.RDD[String] = 
	{ reviews.map(x=>x.replaceAll("""[\p{Punct}]"""," ").replaceAll("""\s+"""," ").toLowerCase) }


	/*example
	input: RDD["alpha alpha alpha beta beta", "word1 word2 word2 word2 word3 word4"]
	output: RDD[Array(("alpha",3),("beta",2)), Array(("word1",1),("word2",3),("word3",1),("word4",1)]
	*/
	def getTermFrequencies(reviews: org.apache.spark.rdd.RDD[String]):org.apache.spark.rdd.RDD[Array[(String, Int)]] = 
	{ reviews.map(x=> x.split(" ").groupBy(x=>x).mapValues(_.size).toArray) }


	def main(args: Array[String]) {

		Logger.getLogger("org").setLevel(Level.ERROR)
		Logger.getLogger("akka").setLevel(Level.ERROR)
		
		
		
		val conf = new SparkConf().setAppName("Opinion-Mining")
		val sc = new SparkContext(conf)
		

		val trainPos = sc.textFile("hdfs://localhost:9000/dataSubset/train/pos")
		val trainNeg = sc.textFile("hdfs://localhost:9000/dataSubset/train/neg")

		val trainPosFreq = getTermFrequencies( processReviews(trainPos))
		val trainNegFreq = getTermFrequencies( processReviews(trainNeg))
		
		println(trainPosFreq.take(2).deep.mkString("\n\n"))
		
		println("SUCCESS")

	}
}
