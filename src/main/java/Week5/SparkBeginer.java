package Week5;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.function.ForeachFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import scala.collection.mutable.AnyRefMap;

import java.io.FileWriter;
import java.io.IOException;
import java.util.concurrent.TimeoutException;

public class SparkBeginer {
    public static void main(String[] args) throws TimeoutException, StreamingQueryException, IOException {
        Logger.getLogger("org").setLevel(Level.OFF);
        SparkSession spark = SparkSession.builder().appName("Spark-Streaming")
                                        .master("local[*]").getOrCreate();
        Dataset<Row> df = spark.readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers","10.140.0.13:9092")
                .option("startingOffsets", "earliest")
                .option("subscribe","Shopee.shopping_app.category")
                .load().selectExpr("CAST(value AS STRING)");
        StructType payload = new StructType()
                .add("after",DataTypes.StringType)
                .add("patch",DataTypes.StringType)
                .add("filter",DataTypes.StringType)
                .add("op",DataTypes.StringType);
        StructType schema = new StructType()
                .add("schema", DataTypes.StringType)
                .add("payload",payload);

       Dataset<Row> data = df.select(functions.from_json(df.col("value"), DataType.fromJson(schema.json())).as("data"))
               .select("data.payload.*");



        StreamingQuery query = data.coalesce(1)
                .writeStream()
                .outputMode("append")
                .format("console")
//                .format("json")
//                .option("path","hdfs://localhost:9000/user/anhhd25")
//
//                .option("checkpointLocation","hdfs://localhost:9000/user/checkpoint")
                .start();
        query.awaitTermination();
    }
}
