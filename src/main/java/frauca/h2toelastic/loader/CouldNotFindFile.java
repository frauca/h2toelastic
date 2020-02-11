package frauca.h2toelastic.loader;

public class CouldNotFindFile extends RuntimeException {
    public CouldNotFindFile(String message, Throwable cause) {
        super(message, cause);
    }
}
