package io.radanalytics;

import javax.annotation.*;

import org.apache.log4j.*;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.beans.factory.annotation.Autowired;

@SpringBootApplication
public class SparkPiBootApplication {

    private static final Logger log = Logger.getRootLogger();

    @Autowired
    private SparkPiProperties properties;

    @PostConstruct
    public void init() {
        log.info("SparkPi submit jar is: "+properties.getJarFile());
        if (!SparkContextProvider.init(properties)) {
            // masterURL probably not set,
            // meaning this was likely run outside of oshinko
            System.exit(1);
        }
    }

    public static void main(String[] args) {
        SpringApplication.run(SparkPiBootApplication.class, args);
    }

}
