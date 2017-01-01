/* Main.scala */
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.mllib.linalg.SparseVector
import org.apache.spark.mllib.linalg.{Vector, Vectors}

import dictionarybuilder.DictionaryBuilder




object OpinionMining {

	
	
	/*example
	input: RDD["One    two. three, four   five-six"]
	output: RDD[Array("one", "two", "three", "four", "five", "six")]
	*/
	def processReviews(reviews: org.apache.spark.rdd.RDD[String]):org.apache.spark.rdd.RDD[Array[String]] = 
	{ reviews.map(x=>x.replaceAll("""[\p{Punct}]"""," ").replaceAll("""\s+"""," ").toLowerCase.split(" ")) }


	/*example
	input: RDD[Array("alpha", "alpha", "alpha", "beta", beta"), Array("word1", "word2", "word2", "word2", "word3", "word4"]
	output: RDD[Array(("alpha",3),("beta",2)), Array(("word1",1),("word2",3),("word3",1),("word4",1)]
	*/
	def getTermFrequencies(reviews: org.apache.spark.rdd.RDD[Array[String]]):org.apache.spark.rdd.RDD[Array[(String, Int)]] = 
	{ reviews.map(x=> x.groupBy(x=>x).mapValues(_.size).toArray) }

	def filterWithDictionary(reviews: org.apache.spark.rdd.RDD[Array[String]],dictionary: scala.collection.Map[String,String])
	:org.apache.spark.rdd.RDD[Array[String]] 
	={reviews.map(termList => termList.map( term => dictionary.getOrElse(term,"NULL")).filter(term=> term!="NULL")) }
	
	def vectorize(reviewTF: Array[(String,Int)], idMap: scala.collection.immutable.Map[String,Int], numTerms: Int ): org.apache.spark.mllib.linalg.Vector =
	{
		 Vectors.sparse(numTerms, reviewTF.map( pair => (idMap(pair._1),pair._2.toDouble) ))
	}

	
	//argument 1: path to project
	//argument 2: path to data

	def main(args: Array[String]) {

		Logger.getLogger("org").setLevel(Level.ERROR)
		Logger.getLogger("akka").setLevel(Level.ERROR)
		
		
		
		val conf = new SparkConf().setAppName("Opinion-Mining")
		val sc = new SparkContext(conf)
		
		
		val trainPos = sc.textFile(args(0) + "/train/pos")
		val trainNeg = sc.textFile(args(0) + "/train/neg")
		val test = sc.textFile(args(0) +"/test")

		val stemPairMap = sc.textFile(args(1)+"/stemPairsSorted.txt").map(x=>(x.split(" ")(0),x.split(" ")(1))).collectAsMap()
		val termIdMap = stemPairMap.values.toSet.toArray.sorted.zipWithIndex.toMap

		
		val stemPairMapBC = sc.broadcast(stemPairMap)
		val spm = stemPairMapBC.value

		val termIdMapBC = sc.broadcast(termIdMap)
		val tim = termIdMapBC.value
		
		val numTerms = tim.keys.size



		val trainPosVectors = getTermFrequencies( filterWithDictionary( processReviews(trainPos), spm)).map(x=>vectorize(x,tim,numTerms))
		val trainNegVectors = getTermFrequencies( filterWithDictionary( processReviews(trainNeg), spm)).map(x=>vectorize(x,tim,numTerms))


		val testVectors = getTermFrequencies( filterWithDictionary( processReviews(test),spm)).map(x=>vectorize(x,tim,numTerms))

		println(trainPosVectors.take(2).deep.mkString("\n\n"))
		
		println("SUCCESS")
		
	}
}
