package frauca.h2toelastic.writer;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Map;

public interface Writer {

    Mono<Long> write(Flux<Map<String, Object>> input);
}
