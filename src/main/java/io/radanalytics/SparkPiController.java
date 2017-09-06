package io.radanalytics;

import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;

@RestController
public class SparkPiController {

    @RequestMapping("/")
    public String index() {
        return "Java Spring Boot SparkPi server running. Add the 'sparkpi' route to this URL to invoke the app.";
    }

    @RequestMapping("/sparkpi")
    public String sparkpi(@RequestParam(value="scale", defaultValue="2") String scale) {
        SparkPiProducer pi = new SparkPiProducer();
        return pi.GetPi(Integer.parseInt(scale));
    }
}
