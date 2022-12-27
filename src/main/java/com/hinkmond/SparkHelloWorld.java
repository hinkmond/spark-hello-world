package com.hinkmond;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.rdd.RDD;
// DataFrame
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.function.Predicate;


public class SparkHelloWorld {
    public static void main(String[] args) {
        Optional<String> debugMode = Optional.ofNullable(System.getProperty("log4j.debugMode"))
                                             .filter(Predicate.not(String::isEmpty));

        debugMode.ifPresent(s -> Logger.getRootLogger()
                                       .setLevel(Level.toLevel(s)));

        SparkConf sparkConf = new SparkConf()
                .setAppName("SparkHelloWorld")
                .setMaster("local[*]");
        SparkSession spark = SparkSession.builder()
                            .config(sparkConf)
                            .getOrCreate();

        RDD<String> textFile = spark.sparkContext().textFile("/opt/spark/README.md",
                1);
        System.out.println(textFile.count());

        List<String> data = Arrays.asList(("Java, 5000"), ("Scala, 3000"), ("Python, 100000"));

        // DataFrame
        Dataset<Row> exampleDF = spark.sqlContext().createDataset(data, Encoders.STRING()).toDF();
        exampleDF.printSchema();
        exampleDF.show();

        // Convert
        Dataset<Row> exampleTableDF = exampleDF.selectExpr("split(value, ',')[0] as PROG_LANG",
                        "split(value, ',')[1] as NUM_DEV");
        exampleTableDF.printSchema();
        exampleTableDF.show();
    }
}
