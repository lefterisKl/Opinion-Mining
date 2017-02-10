/* Main.scala */
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.mllib.linalg.SparseVector
import org.apache.spark.mllib.linalg.{Vector, Vectors}

import org.apache.spark.storage.StorageLevel
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.mllib.classification.{NaiveBayes, NaiveBayesModel}
import org.apache.spark.mllib.classification.{SVMModel, SVMWithSGD}
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.tuning.{ParamGridBuilder, CrossValidator}
import org.apache.spark.ml.classification.RandomForestClassifier
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator

import org.apache.spark.sql.types.StructType

import org.apache.spark.sql.types.StructField






object OpinionMining {

	
	
	
	def processReviews(reviews: org.apache.spark.rdd.RDD[String]):org.apache.spark.rdd.RDD[Array[String]] = 
	{ reviews.map(x=>x.replaceAll("""[\p{Punct}]"""," ").replaceAll("""\s+"""," ").toLowerCase.split(" ")) }


	
	def getTermFrequencies(reviews: org.apache.spark.rdd.RDD[Array[String]]):org.apache.spark.rdd.RDD[Array[(String, Int)]] = 
	{ reviews.map(x=> x.groupBy(x=>x).mapValues(_.size).toArray) }

	def filterWithDictionary(reviews: org.apache.spark.rdd.RDD[Array[String]],dictionary: scala.collection.Map[String,String])
	:org.apache.spark.rdd.RDD[Array[String]] 
	={reviews.map(termList => termList.map( term => dictionary.getOrElse(term,"NULL")).filter(term=> term!="NULL")) }
	
	def vectorize(reviewTF: Array[(String,Int)], idMap: scala.collection.immutable.Map[String,Int], numTerms: Int ): org.apache.spark.mllib.linalg.Vector =
	{
		 Vectors.sparse(numTerms, reviewTF.map( pair => (idMap(pair._1),pair._2.toDouble) ))
	}

	def evaluateAccurasy(m:org.apache.spark.mllib.classification.ClassificationModel,test:org.apache.spark.rdd.RDD[LabeledPoint]):Double = {
		val correct = test.map { point => val score = m.predict(point.features)
		(score== point.label) }.filter(x=>(x==true)).count()
		val numTest = test.count()
		1.0*correct/numTest
	}
	

	
	//argument 1: path to project
	//argument 2: path to data

	def main(args: Array[String]) {
		
		Logger.getLogger("org").setLevel(Level.ERROR)
		Logger.getLogger("akka").setLevel(Level.ERROR)
		
		
		
		val conf = new SparkConf().setAppName("Opinion-Mining")
		val sc = new SparkContext(conf)
		
		
		val trainPos = sc.textFile(args(0) + "/train/pos").repartition(4)
		val trainNeg = sc.textFile(args(0) + "/train/neg").repartition(4)
		val test = sc.textFile(args(0) +"/test",4)

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

		
		val labeledVectors = trainPosVectors.map(x=>LabeledPoint(1.0,x)).union(trainNegVectors.map(x=>LabeledPoint(0.0,x))).persist(StorageLevel.MEMORY_AND_DISK)

		val splits = labeledVectors.randomSplit(Array(0.5, 0.5), seed = 11L)
		val training = splits(0).cache()
		val testing = splits(1)


		//SVM model
		val numIterations = 100
		val svm_model = SVMWithSGD.train(training, numIterations)
		println(evaluateAccurasy(svm_model,testing))


		//println(trainPosVectors.take(2).deep.mkString("\n\n"))
		println("SUCCESS")
		
		
	}
}
