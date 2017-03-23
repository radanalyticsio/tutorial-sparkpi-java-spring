package io.radanalytics;

import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.bind.annotation.RequestMapping;

@RestController
public class SparkPiController {

    @RequestMapping("/")
    public String index() {
        SparkPiProducer pi = new SparkPiProducer();
        return pi.GetPi();
    }

}
