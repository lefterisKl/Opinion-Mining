



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

def buildDictionary( inputs: List[String],outputLocation:String):Unit
{
	//Load all the reviews
	val trainPosRaw = sc.textFile(inputs(0))
	val trainNegRaw = sc.textFile(inputs(1))
	val testRaw = sc.textFile(inputs(2))

	//Tokenize the reviews of each folder and remove the duplicates
	val trainPosTerms = processReviews(trainPosRaw).distinct()
	val trainNegTerms = processReviews(trainNegRaw).distinct()
	val testTerms = processReviews(testRaw).distinct()
	
	//Intersect to find all terms
	val terms = trainPosTerms.intersect(trainNegTerms).intersect(testTerms)
	
	//Stem the terms
	val sqlContext = new org.apache.spark.sql.SQLContext(sc)
	val termsDF = sqlContext.createDataFrame(terms).toDF("word")
	val stemmed = new Stemmer().setInputCol("word").setOutputCol("stemmed").setLanguage("English").transform(termsDF)
	val stemmedTerms: org.apache.spark.rdd.RDD[String] = stemmed.select("stemmed").map(x=>x.toString())
	
	//keep only words that appear in an english dictionary
	val englishDictionary = sc.textFile(inputs(3))
	val finalTerms =  stemmedTerms.intersect(englishDictionary)
	
	//save the result in the specified output location of the driver node
	finalTerms.collect().saveAsTextFile(outputLocation)
}
