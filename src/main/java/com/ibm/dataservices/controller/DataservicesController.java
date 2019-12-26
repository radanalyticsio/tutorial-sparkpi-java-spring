package com.ibm.dataservices.controller;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;
import org.springframework.web.bind.annotation.*;

import org.apache.log4j.*;

import com.ibm.featureprocessing.ProcessingFunctions;
@RestController
@RequestMapping("/dataservice")
public class DataservicesController {

    private static final Logger LOGGER = Logger.getRootLogger();

   // private static final Logger LOGGER = LoggerFactory.getLogger(DataservicesController.class);

    @GetMapping("/getFeatures")
    public String prepareData(@RequestParam("inputJson") String input) {
        SparkConf sparkConf = new SparkConf().setAppName("MihuDataserviceAPI");
        sparkConf.setJars(new String[]{"/opt/app-root/src/@project.name@-@project.version@-original.jar"});
        SparkSession spark = SparkSession.builder().config(sparkConf).getOrCreate();
        //LOGGER.info("Input Parameters : ", input);
        ProcessingFunctions featureProcessing = new ProcessingFunctions();
        return "input " + featureProcessing.prepareAttrs(spark,input);
    }

}
