package com.db.amsidh.config;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class SparkConfig {
    @Value("${spark.master}")
    private String masterUri="local[*]";

    @Bean
    public SparkSession getSparkSession(){
      return SparkSession.builder()
                .appName("AVROReferencedJarFileInSpark")
                .master(masterUri)
                .getOrCreate();
    }


}
