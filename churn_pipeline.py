from pyspark.sql import SparkSession
from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, OneHotEncoder, VectorAssembler, StandardScaler
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

spark = SparkSession.builder.appName("CustomerChurnPipeline").getOrCreate()

# Load data from HDFS
data = spark.read.csv(
    "hdfs:///user/hadoop/churn_input/Churn_Modelling.csv",
    header=True,
    inferSchema=True
)

# Drop useless columns
data = data.drop("RowNumber", "CustomerId", "Surname")

# Index categorical
geo_indexer = StringIndexer(inputCol="Geography", outputCol="GeographyIndex")
gender_indexer = StringIndexer(inputCol="Gender", outputCol="GenderIndex")

encoder = OneHotEncoder(
    inputCols=["GeographyIndex", "GenderIndex"],
    outputCols=["GeographyVec", "GenderVec"]
)

assembler = VectorAssembler(
    inputCols=[
        "CreditScore", "Age", "Tenure", "Balance",
        "NumOfProducts", "EstimatedSalary",
        "GeographyVec", "GenderVec"
    ],
    outputCol="features"
)

scaler = StandardScaler(
    inputCol="features",
    outputCol="scaledFeatures"
)

lr = LogisticRegression(
    labelCol="Exited",
    featuresCol="scaledFeatures"
)

pipeline = Pipeline(stages=[
    geo_indexer,
    gender_indexer,
    encoder,
    assembler,
    scaler,
    lr
])

# train/test split
train, test = data.randomSplit([0.8, 0.2], seed=42)

model = pipeline.fit(train)

predictions = model.transform(test)

predictions.select("Exited", "prediction", "probability").show(5)

evaluator = MulticlassClassificationEvaluator(
    labelCol="Exited",
    predictionCol="prediction",
    metricName="accuracy"
)

accuracy = evaluator.evaluate(predictions)

print("Accuracy:", accuracy)

spark.stop()
