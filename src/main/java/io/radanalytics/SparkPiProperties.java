package io.radanalytics;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import org.springframework.stereotype.*;
import org.springframework.beans.factory.annotation.*;
import org.springframework.context.annotation.*;
import javax.validation.constraints.*;
import javax.annotation.*;

@Component
public class SparkPiProperties {

    @Value("${sparkpi.jarfile}")
    private String jarFile;

    public String getJarFile() {
        return jarFile;
    }

}
