package frauca.h2toelastic.writer;

import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.common.xcontent.XContentType;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class BulkRequestConverter {

    final static String EMPTY = "";
    final String index_name;
    final String timestamp_field;

    public BulkRequestConverter(String index_name) {
        this(index_name, "timestamp");
    }

    public BulkRequestConverter(String index_name, String timestamp_field) {
        this.index_name = index_name;
        this.timestamp_field = timestamp_field;
    }

    BulkRequest fromMap(Map<String, Object> document) {

        LocalDateTime timestamp = (LocalDateTime) Objects.requireNonNull(document.get(timestamp_field));
        BulkRequest request = new BulkRequest();
        request.add(
                createIndexRequest(document, timestamp)
        );
        return request;
    }

    private IndexRequest createIndexRequest(Map<String, Object> document, LocalDateTime timestamp) {
        IndexRequest indexRequest = new IndexRequest(indexName(timestamp), "movement")
                .source(XContentType.JSON, toKeysAndValues(document));
        if (document.get("id") == null) {
            return indexRequest;
        }
        return indexRequest
                .id(document.get("id").toString());

    }

    String indexName(LocalDateTime date) {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy");
        return String.format("%s_%s", index_name, date.format(formatter));
    }

    Object[] toKeysAndValues(Map<String, Object> document) {
        return document.entrySet().stream()
                .flatMap(entry -> Stream.of(entry.getKey(), entry.getValue()))
                .collect(Collectors.toList())
                .toArray();
    }
}
