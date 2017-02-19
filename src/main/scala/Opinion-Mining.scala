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

import org.apache.spark.mllib.classification.{LogisticRegressionModel, LogisticRegressionWithLBFGS}







object OpinionMining {

	
	//The review preprocessing chain	
	

	def tokenize( review:String) : Array[String] =
	{
		review.replaceAll("""[\p{Punct}]"""," ").replaceAll("""\s+"""," ").toLowerCase.split(" ")
	}

	def frequencies(terms: Array[String]):Array[(String,Int)] =
	{
			terms.groupBy(x=>x).mapValues(_.size).toArray
	}

		def stem(input:Array[String],dictionary: scala.collection.Map[String,String]):Array[String] =
	{
		input.map( term => dictionary.getOrElse(term,"NULL")).filter(term=> term!="NULL")
	}



	//Four different TF definitions (raw,normalized,double normalized, log) to create the vectors
	
	def rawTF(reviewTF: Array[(String,Int)], idMap: scala.collection.immutable.Map[String,Int], numTerms: Int ): org.apache.spark.mllib.linalg.Vector =
	{
		 Vectors.sparse(numTerms, reviewTF.map( pair => (idMap(pair._1),pair._2.toDouble) ))
	}

	def normalizedTF(reviewTF: Array[(String,Int)], idMap: scala.collection.immutable.Map[String,Int], numTerms: Int ):org.apache.spark.mllib.linalg.Vector =
	{
		val maxFrequency:Double = reviewTF.map(x=>x._2).reduceLeft(_ max _)
		Vectors.sparse(numTerms, reviewTF.map( pair => (idMap(pair._1),pair._2.toDouble/maxFrequency) ))
	}

	def doubleNormalizedTF(reviewTF: Array[(String,Int)], idMap: scala.collection.immutable.Map[String,Int], numTerms: Int ):org.apache.spark.mllib.linalg.Vector =
	{
		val maxFrequency:Double = reviewTF.map(x=>x._2).reduceLeft(_ max _)
		Vectors.sparse(numTerms, reviewTF.map( pair => (idMap(pair._1),pair._2.toDouble/maxFrequency) ))
	}

	

		def logOrZero(x:Double):Double={ if (x==0) 0 else Math.log(x) }


		def logNormalizedTF(reviewTF: Array[(String,Int)], idMap: scala.collection.immutable.Map[String,Int], numTerms: Int ):org.apache.spark.mllib.linalg.Vector =
	{
		
		Vectors.sparse(numTerms, reviewTF.map( pair => (idMap(pair._1), 1 + logOrZero(pair._2.toDouble)) ))
	}
	
	//One TF-IDF definition

	def logNormalizedTFIDF(reviewTF: Array[(String,Int)], idMap: scala.collection.immutable.Map[String,Int],dfMap: scala.collection.Map[String,Int], numTerms: Int ):org.apache.spark.mllib.linalg.Vector =
	{
		
		Vectors.sparse(numTerms, reviewTF.map( pair => (idMap(pair._1), (1 + logOrZero(pair._2.toDouble)) *Math.log( 50000.0/(1.0+dfMap(pair._1).toDouble)) ) ))
	}

	
	//evaluate accurasy percentage of classification model on a set with known labels

	type classificationModel = org.apache.spark.mllib.classification.ClassificationModel

	def evaluateAccurasy(m:classificationModel,test:org.apache.spark.rdd.RDD[LabeledPoint]):Double = {
		val correct = test.map { point => val score = m.predict(point.features)
		(score== point.label) }.filter(x=>(x==true)).count()
		val numTest = test.count()
		1.0*correct/numTest
	}

	def vote(score:Double,limit:Double)={ if(score > limit) 1.0 else 0.0 }	

	def hybrid(m1: classificationModel, m2: classificationModel, m3:classificationModel, test:org.apache.spark.rdd.RDD[LabeledPoint]):Double =
	{
				val correct = test.map { point => val score = m1.predict(point.features) +m2.predict(point.features) +m3.predict(point.features)
																	
																	(vote(score,1) == point.label) }.filter(x=>(x==true)).count()
				val numTest = test.count()
				1.0*correct/numTest
	}

	//predict on unlabeled data


	def predict(m: classificationModel,test:org.apache.spark.rdd.RDD[(String,org.apache.spark.mllib.linalg.Vector)]) : org.apache.spark.rdd.RDD[(String,Double)] = 
{
	test.map( x => (x._1 , m.predict(x._2)))
}

	

	

	

	
	//argument 1: path to project
	//argument 2: path to data

	def main(args: Array[String]) {
		
		//Logger.getLogger("org").setLevel(Level.ERROR)
		//Logger.getLogger("akka").setLevel(Level.ERROR)
		Logger.getLogger("org").setLevel(Level.OFF)
		Logger.getLogger("akka").setLevel(Level.OFF)
		
		
		
		val conf = new SparkConf().setAppName("Opinion-Mining")
		val sc = new SparkContext(conf)

		println("Loading and preprocessing reviews..")
		
		val partitions:Int = args(2).toInt
		val trainPos = sc.textFile(args(0) + "/train/pos").repartition(partitions)
		val trainNeg = sc.textFile(args(0) + "/train/neg").repartition(partitions)
		val test = sc.wholeTextFiles(args(0)+"/test").repartition(partitions)

		//map from unstemmed term to stemmed term. Non english terms are mapped to NULL and are discarded. Broadcasted for all executors.
		val stemPairMap = sc.textFile(args(1) + "/stemPairsSorted.txt").map(x=>(x.split(" ")(0),x.split(" ")(1))).collectAsMap()
		val stemPairMapBC = sc.broadcast(stemPairMap)
		val spm = stemPairMapBC.value


		//Map from (stemmed) term to termID. Broadcasted for all executors.
		val termIdMap = stemPairMap.values.toSet.toArray.sorted.zipWithIndex.toMap
		val termIdMapBC = sc.broadcast(termIdMap)
		val tim = termIdMapBC.value

		//Map from (stemmed) term to document frequency. Broadcasted for all executors. Used for IDF computation.
		val documentFrequencyMap = sc.textFile(args(1) + "/DocFrequencies.txt").map(x=>(x.split(" ")(0),x.split(" ")(1).toInt)).collectAsMap()
		val documentFrequencyMapBC = sc.broadcast(documentFrequencyMap)
		val dfm = documentFrequencyMapBC.value
		

		//number of distinct terms (after stemming)
		val numTerms = tim.keys.size

		

		if ( args(3).toUpperCase == "EVALUATE" ){

				val trainPosVectors = trainPos.map(x=> frequencies( stem(tokenize(x) ,spm) ) ).map(x=>logNormalizedTF(x,tim,numTerms))
				val trainNegVectors = trainNeg.map(x=> frequencies( stem(tokenize(x) ,spm) ) ).map(x=>logNormalizedTF(x,tim,numTerms))
				val labeledVectors = trainPosVectors.map(x=>LabeledPoint(1.0,x)).union(trainNegVectors.map(x=>LabeledPoint(0.0,x))).persist(StorageLevel.MEMORY_AND_DISK)
				val splits = labeledVectors.randomSplit(Array(0.6, 0.4), seed = 11L)
				val training = splits(0).cache()
				val testing = splits(1)
				val numIterations = 600

			
				//println("Training Logistic Regression model..")
				val lr_model = new LogisticRegressionWithLBFGS().setNumClasses(2).run(training)
				println("Logistic Regression accurasy: " +evaluateAccurasy(lr_model,testing).toString)
			
				println("Training Naive Bayes model..")
				val nb_model = NaiveBayes.train(training, lambda=1.0, modelType = "multinomial")
				println("Naive Bayes model accurasy: " + evaluateAccurasy(nb_model, testing).toString)

				println("Training SVM model..")
				val svm_model = SVMWithSGD.train(training, numIterations)
				println("SVM accurasy:" +evaluateAccurasy(svm_model,testing).toString)

			
				println("Hybrid model accurasy: " + hybrid(nb_model,lr_model,svm_model,testing).toString)
			

		}
		else if (args(3).toUpperCase == "PREDICT"){
			
				val trainPosVectors = trainPos.map(x=> frequencies( stem(tokenize(x) ,spm) ) ).map(x=>logNormalizedTFIDF(x,tim,dfm,numTerms))
				val trainNegVectors = trainNeg.map(x=> frequencies( stem(tokenize(x) ,spm) ) ).map(x=>logNormalizedTFIDF(x,tim,dfm,numTerms))
				val labeledVectors = trainPosVectors.map(x=>LabeledPoint(1.0,x)).union(trainNegVectors.map(x=>LabeledPoint(0.0,x))).persist(StorageLevel.MEMORY_AND_DISK)
				val testVectors = test.map( x =>(x._1 , frequencies(stem(tokenize(x._2),spm)))).map(x=>(x._1,logNormalizedTFIDF(x._2,tim,dfm,numTerms)))

				val numIterations = 600
				val svm_model = SVMWithSGD.train(labeledVectors, numIterations)
				println("Model created, saving predictions..")
				val predictions = predict(svm_model,testVectors)
				predictions.repartition(1).sortByKey().saveAsTextFile(args(4))


		}
		else{
			println("Uknown MODE parameter. Valid values are EVALUATE and PREDICT.")
			
		}

		

		println("SUCCESS")
		
		

	

	}
}
