package kafka;

import ru.practicum.ewm.stats.avro.EventAvro;

public class EventDeserializer extends AvroDeserializerBase<EventAvro> {
    public EventDeserializer() {
        super(EventAvro.getClassSchema());
    }
}
