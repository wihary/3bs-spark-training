import org.apache.spark._
import org.apache.spark.rdd.RDD 
import org.apache.spark.util.IntParam 
import org.apache.spark.graphx._
import org.apache.spark.graphx.util.GraphGenerators 

// Create an RDD for the vertices
val users: RDD[(VertexId, (String, Int))] =
  sc.parallelize(Seq((1L, ("Alice", 28)), (2L, ("Bob", 27)),
                       (3L, ("Charlie", 65)), (4L, ("David", 42)),
                       (5L, ("Ed", 55)), (6L, ("Fran", 50))))

// Create an RDD for edges
val relationships: RDD[Edge[Int]] =
  sc.parallelize(Seq(Edge(2L, 1L, 1), Edge(4L, 1L, 1),
                       Edge(2L, 4L, 2), Edge(3L, 2L, 4),
                       Edge(5L, 2L, 2), Edge(5L, 3L, 8),
                       Edge(5L, 6L, 3), Edge(3L, 6L, 3)))

// Build the initial Graph
val graph: Graph[(String, Int), Int] = 
    Graph(users, relationships)

val inDegrees: VertexRDD[Int] = graph.inDegrees

val ageCount = graph.vertices.filter { case (id, (name, age)) => age == 28 }.count

val count = graph.edges.filter(e => e.srcId > e.dstId).count

val population: RDD[String] =
  graph.triplets.map(triplet => triplet.srcAttr._1 + " is linked to " + triplet.dstAttr._1)
  
population.collect.foreach(println(_))