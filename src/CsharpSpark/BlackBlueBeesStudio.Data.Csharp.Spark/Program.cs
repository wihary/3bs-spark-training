// var sparkSession = SparkSession.Builder()
//     .Master("local")
//     .Config("spark.sql.autoBroadcastJoinThreshold", -1)
//     .Config("spark.executor.memory", "500mb")
//     .AppName("Exercise1")
//     .GetOrCreate();

// // Chargement des tables de données
// var products_table = sparkSession.Read().Parquet("D:/3bStudio/Sandbox/spark-program/resources/products_parquet");
// var sales_table = sparkSession.Read().Parquet("../resources/sales_parquet");
// var sellers_table = sparkSession.Read().Parquet("../resources/sellers_parquet");

// // Comptage du nombre d'éléments par table
// var productCount = products_table.Count();
// var salesCount = sales_table.Count();
// var sellersCount = sellers_table.Count();

// // Affichage des résultats
// Console.WriteLine($"______________nb of products: {productCount}");
// Console.WriteLine($"______________nb of sales: {salesCount}");
// Console.WriteLine($"______________nb of Sellers: {sellersCount}");

// //products_table.OrderBy("product_id").Show();

// // Produit disctint vendu
// // sales_table.GroupBy("date")
// //             .Agg(Functions.CountDistinct("product_id"))
// //             .Alias("distinct_products_sold")
// //             .Show();

// // sales_table.CreateOrReplaceTempView("salesTable");
// // sparkSession
// //     .Sql("SELECT date, count(*) as distinct_products_sold FROM salesTable GROUP BY date")
// //     .Show();


// sales_table
//     .Join(products_table, sales_table["product_id"] == products_table["product_id"], "inner")
//     .Agg(Functions.Avg(products_table["price"] * sales_table["num_pieces_sold"]))
//     .Show();

// sales_table.CreateOrReplaceTempView("salesTable");
// products_table.CreateOrReplaceTempView("productsTable");
// sparkSession
//     .Sql(@"SELECT AVG(productsTable.price*salesTable.num_pieces_sold) as average_sales 
//             FROM salesTable 
//             INNER JOIN productsTable on productsTable.product_id = productsTable.product_id")
//     .Show();

// Console.Read();

using Microsoft.Spark.Sql;

using static Microsoft.Spark.Sql.Functions;

var sparkSession = SparkSession.Builder()
    .Master("local")
    .AppName("Warmup")
    .GetOrCreate();

sparkSession.SparkContext.SetLogLevel("WARN");

DataFrame lines = sparkSession
    .ReadStream()
    .Format("socket")
    .Option("host", "localhost")
    .Option("port", 9999)
    .Option("includeTimestamp", true)
    .Load();

DataFrame words = lines
    .Select(Explode(Split(lines["value"], " "))
    .Alias("word"), lines["timestamp"]);
DataFrame windowedCounts = words
    .GroupBy(Window(words["timestamp"], "10 seconds", "5 seconds"), words["word"])
    .Count()
    .OrderBy("window");

var query = windowedCounts
    .WriteStream()
    .OutputMode("complete")
    .Option("truncate", false)
    .Format("console")
    .Start();

query.AwaitTermination();