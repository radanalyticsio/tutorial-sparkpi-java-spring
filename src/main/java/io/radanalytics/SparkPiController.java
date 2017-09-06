package io.radanalytics;

import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.bind.annotation.RequestMapping;

@RestController
public class SparkPiController {

    @RequestMapping("/")
    public String index() {
        return "Java Spring Boot SparkPi server running. Add the 'sparkpi' route to this URL to invoke the app.";
    }

    @RequestMapping("/sparkpi")
    public String sparkpi() {
        SparkPiProducer pi = new SparkPiProducer();
        return pi.GetPi();
    }
}
