package dictionarybuilder
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
//import org.apache.spark.mllib.feature.Stemmer
import org.apache.spark.sql._





object DictionaryBuilder{

	/*example
	input: RDD["One    two. three, four   five-six"]
	output: RDD["one two three four five six"]
	*/
	def processReviews(reviews: org.apache.spark.rdd.RDD[String]):org.apache.spark.rdd.RDD[String] = 
	{ reviews.map(x=>x.replaceAll("""[\p{Punct}]"""," ").replaceAll("""\s+"""," ").toLowerCase) }






	// "inputs" should contain 4 strings
	// 0: path to train/pos
	// 1: path to train/neg
	// 2: path to test
	// 3: path to english lexicon 

	// "outputLocation" is the path in the driver node where the output will be written.

	def buildDictionary( sc: SparkContext,inputs: List[String],outputLocation:String):Unit = {
		
		
		//Load all the reviews
		val trainPosRaw = sc.textFile(inputs(0))
		val trainNegRaw = sc.textFile(inputs(1))
		val testRaw = sc.textFile(inputs(2))

		//Tokenize the reviews of each folder and remove the duplicates
		val trainPosTerms = processReviews(trainPosRaw).flatMap(x=>x.split(" ")).distinct()
		val trainNegTerms = processReviews(trainNegRaw).flatMap(x=>x.split(" ")).distinct()
		val testTerms = processReviews(testRaw).flatMap(x=>x.split(" ")).distinct()
	
		//Intersect to find all terms
		val terms = trainPosTerms.intersection(trainNegTerms).intersection(testTerms)
		
		val sqlContext = new org.apache.spark.sql.SQLContext(sc)
		/*
		//Stem the terms
		
		val termsDF = sqlContext.createDataFrame(terms).toDF("word")
		val stemmed = new Stemmer().setInputCol("word").setOutputCol("stemmed").setLanguage("English").transform(termsDF)
		val stemmedTerms: org.apache.spark.rdd.RDD[String] = stemmed.select("stemmed").map(x=>x.toString())
		*/

		//keep only words that appear in an english dictionary
		val englishDictionary = sc.textFile(inputs(3))
		val finalTerms =  terms.intersection(englishDictionary)
	
		//save the result in the specified output location of the driver node
		finalTerms.saveAsTextFile(outputLocation)
	}

	

	
}

