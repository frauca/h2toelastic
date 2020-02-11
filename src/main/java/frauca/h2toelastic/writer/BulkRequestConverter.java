package frauca.h2toelastic.writer;

import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.common.xcontent.XContentType;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Component
class BulkRequestConverter {

    final static String EMPTY = "";

    BulkRequest fromMap(Map<String, Object> document) {
        LocalDateTime timestamp = (LocalDateTime) Objects.requireNonNull(document.get("timestamp"));
        BulkRequest request = new BulkRequest();
        request.add(
                new IndexRequest(indexName(timestamp), "movement")
                        .id(document.get("id").toString())
                        .source(XContentType.JSON, toKeysAndValues(document))
        );
        return request;
    }

    String indexName(LocalDateTime date) {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyyMMdd");
        return String.format("account_movs_%s", date.format(formatter));
    }

    Object[] toKeysAndValues(Map<String, Object> document) {
        return document.entrySet().stream()
                .flatMap(entry -> Stream.of(entry.getKey(), entry.getValue()))
                .collect(Collectors.toList())
                .toArray();
    }
}
