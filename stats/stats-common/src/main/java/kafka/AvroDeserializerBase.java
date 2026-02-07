package kafka;

import org.apache.avro.Schema;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.ByteArrayInputStream;
import java.io.IOException;

public class AvroDeserializerBase<T extends SpecificRecordBase> implements Deserializer<T> {

    private final DecoderFactory decoderFactory = DecoderFactory.get();
    private final Schema targetSchema;

    public AvroDeserializerBase(Schema targetSchema) {
        this.targetSchema = targetSchema;
    }

    @Override
    public T deserialize(String topic, byte[] payload) {
        try (ByteArrayInputStream inputStream = new ByteArrayInputStream(payload)) {
            BinaryDecoder decoder = decoderFactory.binaryDecoder(inputStream, null);
            DatumReader<T> reader = new SpecificDatumReader<>(targetSchema);
            return reader.read(null, decoder);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
