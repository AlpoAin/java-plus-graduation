package kafka;

import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

public class AvroSerializer implements Serializer<SpecificRecordBase> {

    private final EncoderFactory encoderFactory = EncoderFactory.get();
    private BinaryEncoder reusableEncoder;

    @Override
    public byte[] serialize(String topic, SpecificRecordBase record) {
        try (ByteArrayOutputStream outputStream = new ByteArrayOutputStream()) {
            if (record == null) {
                return null;
            }

            reusableEncoder = encoderFactory.binaryEncoder(outputStream, reusableEncoder);

            DatumWriter<SpecificRecordBase> writer = new SpecificDatumWriter<>(record.getSchema());
            writer.write(record, reusableEncoder);
            reusableEncoder.flush();

            return outputStream.toByteArray();
        } catch (IOException ex) {
            throw new SerializationException("Failed to serialize record for topic [" + topic + "]", ex);
        }
    }
}
