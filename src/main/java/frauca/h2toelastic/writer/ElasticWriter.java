package frauca.h2toelastic.writer;

import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.springframework.data.elasticsearch.client.reactive.ReactiveElasticsearchClient;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Map;

@Service
@Slf4j
public class ElasticWriter implements Writer {
    final ReactiveElasticsearchClient client;
    final BulkRequestConverter bulkRequest;

    public ElasticWriter(ReactiveElasticsearchClient client, BulkRequestConverter bulkRequest) {
        this.client = client;
        this.bulkRequest = bulkRequest;
        log.info("server is " + client.status().block().hosts().toArray()[0].toString());
    }

    @Override
    public Mono<Long> write(Flux<Map<String, Object>> input) {
        return input
                .limitRate(2)
                .doOnNext(row -> log.info(row.toString()))
                .map(bulkRequest::fromMap)
                .map(this::sendToElastic)
                .map(this::countSuccess)
                .reduce(Long::sum);
    }

    public BulkResponse sendToElastic(BulkRequest request) {
        return client.bulk(request).block();
    }

    public Long countSuccess(BulkResponse response) {
        long bulked = 1;
        if (response.hasFailures()) {
            log.info(response.buildFailureMessage());
            bulked = 0;
        }
        return bulked;
    }

}
