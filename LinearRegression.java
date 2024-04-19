import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.regression.LinearRegression;
import org.apache.spark.ml.regression.LinearRegressionModel;

public class LinearRegression {
    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .appName("Click Prediction")
                .getOrCreate();

        StructType schema = new StructType()
                .add("bidID", DataTypes.StringType, true) // 1st column
                .add("timestamp", DataTypes.IntegerType, true) // 2nd column
                .add("logtype", DataTypes.IntegerType, true) // 3rd column
                .add("iPinYouID", DataTypes.StringType, true) // 4th column
                .add("userAgent", DataTypes.StringType, true) // 5th column
                .add("IP", DataTypes.StringType, true) // 6th column
                .add("region", DataTypes.IntegerType, true) // 7th column
                .add("city", DataTypes.IntegerType, true) // 8th column
                .add("adExchange", DataTypes.IntegerType, true) // 9th column
                .add("domain", DataTypes.StringType, true) // 10th column
                .add("URL", DataTypes.StringType, true) // 11th column
                .add("URLID", DataTypes.StringType, true) // 12th column
                .add("adSlotID", DataTypes.StringType, true) // 13th column
                .add("width", DataTypes.IntegerType, true) // 14th column
                .add("height", DataTypes.IntegerType, true) // 15th column
                .add("visibility", DataTypes.IntegerType, true) // 16th column
                .add("format", DataTypes.IntegerType, true) // 17th column
                .add("floorPrice", DataTypes.IntegerType, true) // 18th column
                .add("creativeID", DataTypes.StringType, true) // 18th column
                .add("biddingPrice", DataTypes.IntegerType, true) // 20th column
                .add("payingPrice", DataTypes.IntegerType, true) // 21st column
                .add("keyPageURL", DataTypes.StringType, true) // 22nd column
                .add("advertiserID", DataTypes.IntegerType, true) // 23nd column
                .add("userTags", DataTypes.IntegerType, true); // 24th column
                .add("numClick", DataTypes.IntegerType, true); // 25th column
                .add("conversion", DataTypes.IntegerType, true); // 26th column

        Dataset<Row> df = spark.read()
                .option("sep", "\t")
                .schema(schema)
                .csv("path_to_your_data/sample_testing_data.txt");

        // Assemble the features into a feature vector
        VectorAssembler assembler = new VectorAssembler()
                .setInputCols(new String[]{"region", "city", "adExchange", "width", "height", "floorPrice", "biddingPrice", "payingPrice"})
                .setOutputCol("features");

        Dataset<Row> output = assembler.transform(df);

        // Split the data into training and test sets (30% held out for testing)
        Dataset<Row>[] splits = output.randomSplit(new double[]{0.7, 0.3});
        Dataset<Row> trainingData = splits[0];
        Dataset<Row> testData = splits[1];

        // Define the Linear Regression model
        LinearRegression lr = new LinearRegression()
                .setLabelCol("numClick")
                .setFeaturesCol("features");

        // Train the model
        LinearRegressionModel model = lr.fit(trainingData);

        // Make predictions
        Dataset<Row> predictions = model.transform(testData);

        // Select example rows to display
        predictions.select("prediction", "numberOfClicks", "features").show(5);

        spark.stop();
    }

    

}
