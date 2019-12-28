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
    public Map<String,String> prepareData(@RequestParam(value="inputJson", defaultValue="{'attributes':[{'desc':'Spend Amount','dbtype':'mongodb','table':'cards','column':'Spend_Amount','colT4mtn':'NA'},{'desc':'CustomerIdentifier','dbtype':'mongodb','table':'cards','column':'Customer_ID','colT4mtn':'NA'},{'desc':'CustomerIdentifier','dbtype':'mariadb','table':'demographics','column':'Customer_ID','colT4mtn':'NA'},{'desc':'BusinessDate','dbtype':'mongodb','table':'cards','column':'Business_Date','colT4mtn':'NA'},{'desc':'CardType','dbtype':'mongodb','table':'cards','column':'Card_Type','colT4mtn':'NA'},{'desc':'PaidAmount','dbtype':'mongodb','table':'cards','column':'Paid_Amount','colT4mtn':'NA'},{'desc':'DateofBirth','dbtype':'mariadb','table':'demographics','column':'DOB','colT4mtn':'NA'},{'desc':'MaritalStatus','dbtype':'mariadb','table':'demographics','column':'Marital_Status','colT4mtn':'NA'},{'desc':'PostalCode','dbtype':'mariadb','table':'demographics','column':'Postal_Code','colT4mtn':'NA'},{'desc':'SelfEmployedorNot','dbtype':'mariadb','table':'demographics','column':'Self_Employed','colT4mtn':'NA'}],'transformations':[{'DST4mtn':'RollingWindow','params':'(Spend_Amount,10,Customer_ID,Business_Date)','colName':'Calculated_Carry_Over_Amt'}],'datasetname':'sample'}") String input) {
        SparkConf sparkConf = new SparkConf().setAppName("MihuDataserviceAPI");
        sparkConf.setJars(new String[]{"/opt/app-root/src/dataserviceapi-0.0.1-SNAPSHOT.jar"});
        SparkSession spark = SparkSession.builder().config(sparkConf).getOrCreate();
        //LOGGER.info("Input Parameters : ", input);
        ProcessingFunctions featureProcessing = new ProcessingFunctions();
        String res = featureProcessing.prepareAttrs(spark,input);
        Map<String,String> result = new HashMap<>();
        result.put("message", String.format("%s", res));
        return result;
    }
}
