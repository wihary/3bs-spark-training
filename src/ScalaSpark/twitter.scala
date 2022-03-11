import org.apache.spark.SparkConf
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds
import twitter4j.conf.ConfigurationBuilder
import twitter4j.auth.OAuthAuthorization
import twitter4j.Status
import org.apache.spark.streaming.twitter.TwitterUtils

// Set the system properties so that Twitter4j library used by twitter stream
// can use them to generat OAuth credentials
// System.setProperty("twitter4j.oauth.consumerKey", "vo4JL76Egj5TDoRJqMV9bk7zz")
// System.setProperty("twitter4j.oauth.consumerSecret", "1sXOD1aeDAHj5HxKqXr0lsYxIWbrzxTUavi1PxnqZUir2FIDeL")
// System.setProperty("twitter4j.oauth.accessToken", "94358829-cxDHKs7kuK1oWSQ3B2WIFFZ3FOcab0gFBKwdPkc5p")
// System.setProperty("twitter4j.oauth.accessTokenSecret", "2TouMsEj6rwuAewm79lFlGrToOOAG6G545cVQ2ZRAy8IH")

    val appName = "TwitterData"
    val ssc = new StreamingContext(sc, Seconds(5))

    val Array(consumerKey, consumerSecret, accessToken, accessTokenSecret) = Array("vo4JL76Egj5TDoRJqMV9bk7zz", "1sXOD1aeDAHj5HxKqXr0lsYxIWbrzxTUavi1PxnqZUir2FIDeL", "94358829-cxDHKs7kuK1oWSQ3B2WIFFZ3FOcab0gFBKwdPkc5p", "2TouMsEj6rwuAewm79lFlGrToOOAG6G545cVQ2ZRAy8IH")

    val filters = List("blood","plasma", "#blood", "#bloodrequired","#BloodMatters","#BloodDrive","#DonateBlood","#Blood","#NeedBlood")
    val cb = new ConfigurationBuilder

    cb.setDebugEnabled(true).setOAuthConsumerKey(consumerKey)
      .setOAuthConsumerSecret(consumerSecret)
      .setOAuthAccessToken(accessToken)
      .setOAuthAccessTokenSecret(accessTokenSecret)

    val auth = new OAuthAuthorization(cb.build)
    val tweets = TwitterUtils.createStream(ssc, Some(auth), filters)

    tweets .saveAsTextFiles("tweets", "json")
    ssc.start()
    ssc.awaitTermination()