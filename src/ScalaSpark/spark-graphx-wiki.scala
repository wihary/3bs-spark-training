import org.apache.spark._
import org.apache.spark.rdd.RDD 
import org.apache.spark.util.IntParam 
import org.apache.spark.graphx._
import org.apache.spark.graphx.util.GraphGenerators 



val edgeGraph : Graph[Int, Int] = GraphLoader.edgeListFile(sc, "D:/3bStudio/Sandbox/3bs-spark-training/resources/edges.txt")

val vertices = sc.textFile("D:/3bStudio/Sandbox/3bs-spark-training/resources/verts.txt").map {l =>
  val lineSplits = l.split("\\s+")
  val id = lineSplits(0).trim.toLong
  val data = lineSplits.slice(1, lineSplits.length).mkString(" ")
  (id, data)
}

val pageGraph = edgeGraph.outerJoinVertices(vertices)({ (vid, _, title) => title.getOrElse("xxxx")}).cache

val ranks = pageGraph.pageRank(0.0001).vertices

val ranksByPage = vertices.join(ranks).map {
  case (id, (title, rank)) => (title, rank)
}.sortBy(pair => pair._2, false)

// Print the result
ranksByPage.take(10).foreach(println)