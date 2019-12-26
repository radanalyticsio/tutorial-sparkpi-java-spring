package com.ibm.dataservices.controller;

import org.apache.spark.sql.SparkSession;
import org.apache.log4j.*;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.*;

import com.ibm.featureprocessing.ProcessingFunctions;
@RestController
@RequestMapping("/dataservice")
public class DataservicesController {

    private static final Logger LOGGER = Logger.getRootLogger();
   // private static final Logger LOGGER = LoggerFactory.getLogger(DataservicesController.class);

    @GetMapping("/getFeatures")
    public String prepareData(@RequestParam("inputJson") String input) {

        SparkSession spark = SparkSession.builder().getOrCreate();
        //LOGGER.info("Input Parameters : ", input);
        ProcessingFunctions featureProcessing = new ProcessingFunctions();
        return "input " + featureProcessing.prepareAttrs(spark,input);
    }

}
