package com.ibm.dataservices.controller;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;
import org.springframework.web.bind.annotation.*;

import org.apache.log4j.*;

import com.ibm.featureprocessing.ProcessingFunctions;

import java.util.HashMap;
import java.util.Map;

@RestController
public class DataservicesController {

    @RequestMapping("/sample")
    public Map<String,String> sample(@RequestParam(value="name", defaultValue="World") String name) {
        Map<String,String> result = new HashMap<>();
        result.put("message", String.format("Hello, %s", name));
        return result;
    }

    @RequestMapping("/getFeatures")
    public Map<String,String> prepareData(@RequestParam(value="inputJson", defaultValue="{'attributes':[{'desc':'CustomerIdentifier','dbtype':'mongodb','table':'cards','column':'Customer_ID','colT4mtn':'NA'}]}") String input) {
        SparkConf sparkConf = new SparkConf().setAppName("MihuDataserviceAPI");
        sparkConf.setJars(new String[]{"/opt/app-root/src/@project.name@-@project.version@-original.jar"});
        SparkSession spark = SparkSession.builder().config(sparkConf).getOrCreate();
        //LOGGER.info("Input Parameters : ", input);
        ProcessingFunctions featureProcessing = new ProcessingFunctions();
        String res = featureProcessing.prepareAttrs(spark,input);
        Map<String,String> result = new HashMap<>();
        result.put("message", String.format("%s", res));
        return result;
    }
}
