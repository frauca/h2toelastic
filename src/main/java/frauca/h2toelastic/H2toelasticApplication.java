package frauca.h2toelastic;

import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
@Slf4j
public class H2toelasticApplication {

	public static void main(String[] args) {
		SpringApplication.run(H2toelasticApplication.class, args);
	}

}
