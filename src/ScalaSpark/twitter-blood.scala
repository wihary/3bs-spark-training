import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.sql.functions.{col, lit, when}
import org.apache.spark.streaming._
import org.apache.spark.streaming.twitter._
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SaveMode
import org.apache.spark.rdd.RDD
import java.util.concurrent._
import java.util.concurrent.atomic._
import Utilities._
import org.apache.log4j.{Level, Logger}

/** Listens to a stream of Tweets and filters the ones related to urgent blood requirement
 *  hashtags over a 5 minute window.
 */
object DonateBlood {
  
  /** Our main function where the action happens */
  def main(args: Array[String]) {

    // Configure Twitter credentials using twitter.txt
    setupTwitter()
    
    /* Set up a Spark streaming context named "DonateBlood" that runs locally using
    all CPU cores and one-second batches of data*/
    val ssc = new StreamingContext("local[*]", "DonateBlood", Seconds(1))
    
    // Get rid of log spam (should be called after the context is set up)
    val rootLogger = Logger.getRootLogger()
    rootLogger.setLevel(Level.ERROR)
    
    //Create a list of filter keywords to pass as arguments while creating twitter stream
    
     val filters = List("blood","plasma", "#blood", "#bloodrequired","#BloodMatters","#BloodDrive","#DonateBlood","#Blood","#NeedBlood")
     
    // Create a DStream from Twitter using the Spark streaming context and the filters
    // Note: We are passing 'None' as Twitter4J authentication because 
    // the twitter credentials are already set by calling System.setProperty(Twitter4J's default OAuth authorization) in setupTwitter()
     
    val tweets = TwitterUtils.createStream(ssc, None, filters)
   
   // Now extract the id, date, user,text,location, retweet, hashtags of each status into DStreams using map()
    val statuses = tweets.map{status =>
        val id = status.getUser.getId
        val user = status.getUser.getName
        val date = status.getCreatedAt.toString
        val text = status.getText
        val location = status.getUser.getLocation
        val retweet = status.isRetweet()
      
        //For extracting hashtags, create a list of words by splitting the text of the status using ' '(space) and then filter 
        //only the words that start with #
        val hashtags = status.getText.split(" ").toList.filter(word => word.startsWith("#"))
        (id, date, user,text,location, retweet, hashtags)
    }
    
    //path for storing the solution file
    val path = "solution.parquet"
    
    // foreachRDD applies the 'function' inside it to each RDD generated from the stream. 
    // This function should push the data in each RDD to an external system (in this case, a parquet file and a temp view)
    statuses.foreachRDD((rdd, time) => 
    {      
     val spark = SparkSession.builder().appName("MyProject").getOrCreate()
         
      import spark.implicits._
      
      // Filter out empty batches
      if (rdd.count() > 0) {
       //Convert rdd to Dataframe
       val requestsDataFrame = rdd.toDF("id","date","user","text","location","retweet","hashtags")

       val bloodDonateTweets =  requestsDataFrame.filter(col("text").contains("urgent") || col("text").contains("need") || col("text").contains("emergency") || col("text").contains("required")).dropDuplicates("text")

        // Filter out empty batches
       if (bloodDonateTweets.count() > 0)
       {
      //Create a SQL table from this DataFrame
      bloodDonateTweets.createOrReplaceTempView("BloodDonationTable")
      
      val solution =  spark.sqlContext.sql("select * from BloodDonationTable order by date")
      solution.show(false)   
      
      //Write the data into a parquet file
      solution.coalesce(1).write.mode(SaveMode.Append).parquet(path)
       }
      }
    })
    
    // Set a checkpoint directory
    ssc.checkpoint("C:/checkpoint/")
    ssc.start()
    ssc.awaitTermination()
  }  
}