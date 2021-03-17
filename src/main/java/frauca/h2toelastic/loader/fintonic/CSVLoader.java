package frauca.h2toelastic.loader.fintonic;

import com.univocity.parsers.common.record.Record;
import com.univocity.parsers.conversions.Conversion;
import com.univocity.parsers.conversions.FormattedBigDecimalConversion;
import com.univocity.parsers.csv.CsvParser;
import com.univocity.parsers.csv.CsvParserSettings;
import frauca.h2toelastic.loader.Loader;
import reactor.core.publisher.Flux;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.Reader;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;

public class CSVLoader implements Loader {

    private static final Locale SPANISH = new Locale("es", "ES");

    @Override
    public Flux<Map<String, Object>> read(String csvFile) {
        return Flux.fromIterable(parse(csvFile))
                .map(this::format);
    }

    private Map<String, Object> format(Record record) {
        Map<String, Object> fields = record.toFieldObjectMap();
        Map<String, Object> converted = fields.entrySet().stream()
                .collect(Collectors.toMap(
                        entry -> varName(entry.getKey()),
                        entry -> entry.getValue())
                );
        return converted;
    }

    private List<Record> parse(String csvFile) {
        CsvParserSettings settings = new CsvParserSettings();
        settings.setHeaderExtractionEnabled(true);
        CsvParser parser = new CsvParser(settings);
        parser.beginParsing(getReader(csvFile));
        fintonicFormat(parser);
        return parser.parseAllRecords();
    }

    private void fintonicFormat(CsvParser parser) {
        parser.getRecordMetadata().convertFields(number()).set("Importe");
        parser.getRecordMetadata().convertFields(date()).set("Fecha Valor", "Fecha de operación");
        parser.getRecordMetadata().setDefaultValueOfColumns("","Nota");
    }

    private FormattedBigDecimalConversion number() {
        DecimalFormat spanish = (DecimalFormat) NumberFormat.getInstance(SPANISH);
        return new FormattedBigDecimalConversion(spanish);
    }

    private Conversion<String, LocalDateTime> date() {
        return new Conversion<String, LocalDateTime>() {
            final DateTimeFormatter formatter = DateTimeFormatter.ofPattern("d/M/yy", SPANISH);

            @Override
            public LocalDateTime execute(String input) {
                return LocalDate.parse(input, formatter).atStartOfDay();
            }

            @Override
            public String revert(LocalDateTime time) {
                return time.format(formatter);
            }
        };
    }

    private String varName(String old_name) {
        return old_name.toLowerCase(Locale.ROOT).trim()
                .replace(' ', '_')
                .replace('ó', 'o');
    }

    private Reader getReader(String relativePath) {
        try {
            return new BufferedReader(new FileReader(relativePath));
        } catch (FileNotFoundException e) {
            throw new RuntimeException(String.format("Could not open file {}", relativePath), e);
        }
    }
}

