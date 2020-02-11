package frauca.h2toelastic.loader;

import reactor.core.publisher.Flux;

import java.util.Map;

public interface Loader {
    Flux<Map<String, Object>> read(String input);
}
