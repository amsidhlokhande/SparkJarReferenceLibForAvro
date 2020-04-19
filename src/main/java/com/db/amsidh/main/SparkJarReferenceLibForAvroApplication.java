package com.db.amsidh.main;

import com.db.amsidh.config.SparkConfig;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.data.web.SpringDataWebAutoConfiguration;
import org.springframework.boot.autoconfigure.web.servlet.WebMvcAutoConfiguration;

import java.net.URL;
import java.net.URLClassLoader;

@SpringBootApplication(exclude = {SpringDataWebAutoConfiguration.class, WebMvcAutoConfiguration.class})
public class SparkJarReferenceLibForAvroApplication implements CommandLineRunner {
    protected static Log logger = LogFactory.getLog(SparkJarReferenceLibForAvroApplication.class);

    @Autowired
    private SparkSession sparkSession;

    public static void main(String[] args) {

        SpringApplication.run(new Class[]{SparkJarReferenceLibForAvroApplication.class, SparkConfig.class}, args);
    }

    @Override
    public void run(String... args) throws Exception {
        System.out.println("Spark AvroReferencedJarFileInSpark Started");
        logger.info("Spark AvroReferencedJarFileInSpark Started");
        printlnAllSystemLoaderFiles();
        DataFrameReader avro = sparkSession.read().format("avro");
        URL path = avro.getClass().getResource("avro");
        System.out.println("Path ="+ path.getPath());
        Dataset<Row> dataSetRow = sparkSession.read().format("avro").load(args[0]);
        logger.info("Total number of Records = " + dataSetRow.count());
        dataSetRow.show(10, false);

        System.out.println("Spark AvroReferencedJarFileInSpark Ended");
        logger.info("Spark AvroReferencedJarFileInSpark Ended");

    }


    public static void printlnAllSystemLoaderFiles(){
        System.out.println("All System Class Loaders Files");
        ClassLoader cl = ClassLoader.getSystemClassLoader();

        URL[] urls = ((URLClassLoader)cl).getURLs();

        for(URL url: urls){
            System.out.println(url.getFile());
        }
    }


}
