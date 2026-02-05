package event.persistence.mapper;

import dto.compilation.CompilationResponse;
import dto.compilation.NewCompilationRequest;
import dto.event.EventShortDto;
import event.persistence.model.Compilation;
import org.mapstruct.Mapper;
import org.mapstruct.Mapping;

import java.util.Set;

@Mapper(componentModel = "spring", uses = EventMapper.class)
public interface CompilationMapper {

    @Mapping(target = "id", ignore = true)
    @Mapping(target = "events", ignore = true)
    Compilation toEntity(NewCompilationRequest request);

    @Mapping(target = "events", source = "events")
    CompilationResponse toDto(Compilation compilation, Set<EventShortDto> events);
}