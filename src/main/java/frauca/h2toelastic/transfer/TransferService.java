package frauca.h2toelastic.transfer;

import frauca.h2toelastic.loader.Loader;
import frauca.h2toelastic.writer.Writer;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

public class TransferService {
    private final Loader loader;
    private final Writer writer;

    public TransferService(Loader loader, Writer writer) {
        this.loader = loader;
        this.writer = writer;
    }

    public Mono<Long> transfer(String input, long elementsToTransfer){
        return writer.write(
          loader.read(input).take(elementsToTransfer)
        );
    }
}
