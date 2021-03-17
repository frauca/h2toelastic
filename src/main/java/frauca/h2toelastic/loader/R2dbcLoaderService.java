package frauca.h2toelastic.loader;

import org.springframework.data.r2dbc.core.DatabaseClient;
import org.springframework.data.repository.init.ResourceReader;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Map;

public class R2dbcLoaderService implements Loader {
    final DatabaseClient client;

    public R2dbcLoaderService(DatabaseClient client) {
        this.client = client;
    }

    public Flux<Map<String, Object>> read(String file) {
        String sql = readFile(file);
        return client.execute(sql).fetch().all();
    }

    public String readFile(String input)  {
        try {
            return new String ( Files.readAllBytes( Paths.get(input) ) );
        } catch (IOException e) {
            throw new CouldNotFindFile(String.format("Could not find the file with the sql %s",input)
            ,e);
        }
    }
}
