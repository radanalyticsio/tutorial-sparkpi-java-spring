package com.ibm.dataservices;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import springfox.documentation.builders.ApiInfoBuilder;
import springfox.documentation.builders.PathSelectors;
import springfox.documentation.builders.RequestHandlerSelectors;
import springfox.documentation.spi.DocumentationType;
import springfox.documentation.spring.web.plugins.Docket;
import springfox.documentation.swagger.web.*;
import springfox.documentation.swagger2.annotations.EnableSwagger2;

@SpringBootApplication
@EnableSwagger2
public class Main {
    //@Value("${VERSION}")
    String version = "1.0";

    public static void main(String[] args) {
        SpringApplication.run(Main.class, args);
    }

    @Bean
    public Docket swaggerApi() {
        return new Docket(DocumentationType.SWAGGER_2)
                .select()
                .apis(RequestHandlerSelectors.basePackage("com.ibm.dataservices"))
                .paths(PathSelectors.any())
                .build()
                .apiInfo(new ApiInfoBuilder().version(version).title("Dataservices API").description("Documentation dataservice API v" + version).build());
    }

    @Bean
    UiConfiguration uiConfig() {
        return new UiConfiguration(Boolean.TRUE, Boolean.FALSE, 1, 1, ModelRendering.MODEL, Boolean.FALSE, DocExpansion.LIST, Boolean.FALSE, null, OperationsSorter.ALPHA, Boolean.FALSE, TagsSorter.ALPHA, UiConfiguration.Constants.DEFAULT_SUBMIT_METHODS, null);
    }
}
