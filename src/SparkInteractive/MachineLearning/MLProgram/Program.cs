using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using Microsoft.ML;
using Microsoft.ML.Data;
using Microsoft.Spark;
using Microsoft.Spark.Sql;
using Microsoft.Spark.Sql.Types;
using static Microsoft.Spark.Sql.Functions;

// run spark : spark-submit --class org.apache.spark.deploy.dotnet.DotnetRunner --master local D:\3bStudio\Sandbox\spark-program\FirstSparkProgram\bin\Debug\net6.0\microsoft-spark-3-0_2.12-2.1.0.jar debug
// see Spark portal at : http://localhost:4040
var spark = SparkSession
    .Builder()
    .AppName("spark-use-ml-model")
    .GetOrCreate();

spark.SparkContext.SetLogLevel("WARN");

var inputSchema = new StructType(new[]
    {
        new StructField("age", new FloatType()),
        new StructField("cholesterol", new FloatType()),
        new StructField("restingBP", new FloatType()),
        new StructField("fastingBS", new FloatType())
    });

DataFrame df = spark.Read().Schema(inputSchema).Json(@"D:\2.Sandbox\3bs-spark-training\resources\ml-input.json");

var schema = df.Schema();
Console.WriteLine(schema.SimpleString);

IEnumerable<Row> rows = df.Collect();
foreach (Row row in rows)
{
    Console.WriteLine(row);
}

spark.Udf().Register<float, float, float, float, bool>("MLudf", (age, cholesterol, restingBP, fastingBS) => predict(age, cholesterol, restingBP, fastingBS));

// Use Spark SQL to call ML.NET UDF
df.CreateOrReplaceTempView("HeartData");
DataFrame sqlDf = spark.Sql("SELECT age, cholesterol, restingBP, fastingBS, MLudf(age, cholesterol, restingBP, fastingBS) FROM HeartData");
sqlDf.Show();

// Print out first 20 rows of data
// Prevent data getting cut off by setting truncate = 0
sqlDf.Show(20, 0, false);

spark.Stop();


static bool predict(float age, float cholesterol, float restingBP, float fastingBS)
{
    MLContext mlContext = new MLContext();
    ITransformer model = mlContext.Model.Load(@"D:\2.Sandbox\3bs-spark-training\resources\heart-classification.zip", out DataViewSchema schema);
    var predEngine = mlContext.Model.CreatePredictionEngine<HeartProfile, PredictionSummary>(model);

    return predEngine.Predict(new HeartProfile
        {
            Age = age,
            Cholesterol = cholesterol,
            RestingBP = restingBP,
            FastingBS = fastingBS
        }).Prediction;
}

public class HeartProfile
{
    public float Age;
    
    public float Cholesterol;
    public float RestingBP;
    public float FastingBS;
    public bool HeartDisease;
}

public class PredictionSummary
{
    [ColumnName("PredictedLabel")]
    public bool Prediction { get; set; }

    public float Probability { get; set; }

    public float Score { get; set; }
}
