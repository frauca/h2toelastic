package frauca.h2toelastic;

import frauca.h2toelastic.loader.R2dbcLoaderService;
import frauca.h2toelastic.transfer.TransferService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import reactor.core.publisher.Flux;

import java.util.Map;

@SpringBootApplication
@Slf4j
public class H2toelasticApplication {

    final TransferService service;

    public H2toelasticApplication(TransferService service, ApplicationArguments arguments) {
        this.service = service;
    }

    public static void main(String[] args) {
        SpringApplication app = new SpringApplication(H2toelasticApplication.class);
        app.setWebApplicationType(WebApplicationType.NONE);
        app.run(args).close();
    }

    @Bean
    public CommandLineRunner transferSQLResul() {
        return args -> {
            String sqlFile = args[0];
            long elementsToTransfer = numfrom(args);
            log.info("Reading sql from " + sqlFile);
            long totalTransfered = service.transfer(sqlFile, elementsToTransfer).block();
            log.info(String.format("%s transfered", totalTransfered));
        };
    }

    long numfrom(String[] args) {
        long defaultToFetch = 2_000L;
        try {
            return Integer.parseInt(args[1]);
        } catch (Exception e) {
            log.info("Second argument could not be getted [" + e.getMessage() + "]. It will be fetched " + defaultToFetch);
            return defaultToFetch;
        }
    }
}
