package Week5;

import java.util.*;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.TaskContext;
import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.*;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.StreamingContext;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.kafka010.*;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import scala.Function1;
import scala.Option;
import scala.Tuple2;
import com.fasterxml.jackson.databind.ObjectMapper;
import javax.xml.crypto.Data;

public class CSV {
    public static void main(String[] args) throws InterruptedException {
        SparkConf sparkConf = new SparkConf().setAppName("Simple Spark Streaming app").setMaster("local[*]");
        JavaStreamingContext streamingContext = new JavaStreamingContext(sparkConf,  Durations.seconds(10));
        SparkSession spark = SparkSession.builder().appName("SimpleApp").getOrCreate();

        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers", "10.140.0.13:9092");
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", StringDeserializer.class);
        kafkaParams.put("group.id", "group2");
        kafkaParams.put("auto.offset.reset", "earliest");
        kafkaParams.put("enable.auto.commit", false);


        Collection<String> topics = Arrays.asList("Shopee.shopping_app.category");
        JavaInputDStream<ConsumerRecord<String,String>>stream =
                KafkaUtils.createDirectStream(
                        streamingContext,
                        LocationStrategies.PreferConsistent(),
                        ConsumerStrategies.<String, String>Subscribe(topics, kafkaParams)
                );
        JavaPairDStream<String, String> results = stream
                .mapToPair(
                        record -> new Tuple2<>(record.key(), record.value())
                );
        JavaDStream<String> lines = results
                .map(
                        tuple2 -> tuple2._2()
                );

        JavaDStream<String> key = results
                .map(
                        tuple2 -> tuple2._1()
                );


//        StructType payload = new StructType()
//                .add("after", DataTypes.StringType)
//                .add("patch",DataTypes.StringType)
//                .add("filter",DataTypes.StringType)
//                .add("op",DataTypes.StringType);
//        StructType schema = new StructType()
//                .add("schema", DataTypes.StringType)
//                .add("payload",payload);


        lines.foreachRDD(new VoidFunction<JavaRDD<String>>() {
            @Override
            public void call(JavaRDD<String> stringJavaRDD) throws Exception {
                    Dataset<Row>dataFile = spark.read().json("hdfs://localhost:9000/user/anhhd25");
                    dataFile.createOrReplaceTempView("Result");
                    if(!stringJavaRDD.isEmpty()){

                        Dataset<Row> data = spark.read().json(stringJavaRDD).select("payload");
                        data.printSchema();

                        //table get from kafka
                        Dataset<Row> df = data.select("payload")
                                .toDF("payload").select("payload.after","payload.filter","payload.op","payload.patch");
                        df.show();
                        //filter data where op = "c"
//                        Dataset<Row>insert_0 = df.select("after").where("op ='c'");
//                        Dataset<String    >insert_1 = insert_0.as(Encoders.STRING());
//                        Dataset<Row>insert_2 = spark.read().json(insert_1);
//                        insert_2.coalesce(1).write().mode(SaveMode.Append).json("hdfs://localhost:9000/user/anhhd25");

//
                        Dataset<Row>delete_0 = df.select("filter").where("op = 'd'");
                        Dataset<String>delete_1 = delete_0.as(Encoders.STRING());
                        Dataset<Row>delete_2 = spark.read().json(delete_1);
                        delete_2.createOrReplaceTempView("table_delete");

                        Dataset<Row>table1 = spark.sql("select * from Result");
                        Dataset<Row>table2 = spark.sql("select * from table_delete");
                        Dataset<Row>result = spark.sql("select * from Result where _id not in (select _id from table_delete)");
                        result.coalesce(1).write().mode("overwrite").json("hdfs://localhost:9000/user/anhhd25");


//
                    }

                }
        });


        streamingContext.start();
        streamingContext.awaitTermination();

    }
}
