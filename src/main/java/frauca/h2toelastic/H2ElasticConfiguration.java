package frauca.h2toelastic;

import frauca.h2toelastic.loader.Loader;
import frauca.h2toelastic.loader.R2dbcLoaderService;
import frauca.h2toelastic.loader.fintonic.CSVLoader;
import frauca.h2toelastic.transfer.TransferService;
import frauca.h2toelastic.writer.BulkRequestConverter;
import frauca.h2toelastic.writer.ElasticWriter;
import frauca.h2toelastic.writer.Writer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.data.elasticsearch.client.reactive.ReactiveElasticsearchClient;
import org.springframework.data.r2dbc.core.DatabaseClient;

@Configuration
public class H2ElasticConfiguration {

    @Bean
    @Profile("default")
    public TransferService h2ToElastic(DatabaseClient db, ReactiveElasticsearchClient elastic) {
        Loader loader = new R2dbcLoaderService(db);
        Writer writer = new ElasticWriter(elastic, new BulkRequestConverter("account_movs"));
        return new TransferService(loader, writer);
    }

    @Bean
    @Profile("fintonic")
    public TransferService fintonicCSV(ReactiveElasticsearchClient elastic) {
        Loader loader = new CSVLoader();
        BulkRequestConverter converter = new BulkRequestConverter("fintonic", "fecha_valor");
        Writer writer = new ElasticWriter(elastic, converter);
        return new TransferService(loader, writer);
    }
}
