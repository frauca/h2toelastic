package frauca.h2toelastic.writer;

import org.junit.jupiter.api.Test;
import org.mockito.Spy;

import java.time.LocalDateTime;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.*;

class BulkRequestConverterTest {

    @Spy
    BulkRequestConverter converter = new BulkRequestConverter("test");

    @Test
    public void indexName() {
        assertEquals("account_movs_20200208",
                converter.indexName(LocalDateTime.of(2020, 2, 8, 0, 0)));
    }

    @Test
    public void propValues() {
        Map<String, Object> map = Map.of("one", "oneValue", "two", "twoValue");
        assertThat(converter.toKeysAndValues(map)).contains("one", "oneValue", "two", "twoValue");
    }
}