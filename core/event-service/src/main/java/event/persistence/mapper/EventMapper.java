package event.persistence.mapper;

import dto.categories.CategoryDto;
import dto.event.*;
import dto.user.UserShortDto;
import event.persistence.model.Category;
import event.persistence.model.Event;
import event.persistence.model.EventLocation;
import org.mapstruct.*;
import org.mapstruct.factory.Mappers;

@Mapper(
        componentModel = MappingConstants.ComponentModel.SPRING,
        unmappedTargetPolicy = ReportingPolicy.IGNORE,
        nullValuePropertyMappingStrategy = NullValuePropertyMappingStrategy.IGNORE
)
public interface EventMapper {
    EventMapper INSTANCE = Mappers.getMapper(EventMapper.class);

    @Mapping(target = "id", ignore = true)
    @Mapping(target = "category", source = "category", qualifiedByName = "categoryIdToCategory")
    @Mapping(target = "initiator", ignore = true)
    @Mapping(target = "createdAt", ignore = true)
    @Mapping(target = "publishedAt",  ignore = true)
    @Mapping(target = "location", source = "location", qualifiedByName = "locationDtoToEventLocation")
    @Mapping(target = "isPaid", source = "paid")
    @Mapping(target = "isRequestModeration", source = "requestModeration")
    Event toEvent(NewEventDto newEventDto);

    @Mapping(target = "id", source = "event.id")
    @Mapping(target = "category", source = "event.category", qualifiedByName = "categoryToDto")
    @Mapping(target = "initiator", source = "user")
    @Mapping(target = "confirmedRequests", ignore = true)
    @Mapping(target = "paid", source = "event.isPaid")
    @Mapping(target = "rating", ignore = true)
    EventShortDto toShortDto(Event event, UserShortDto user);

    @Mapping(target = "id", source = "event.id")
    @Mapping(target = "annotation", source = "event.annotation")
    @Mapping(target = "description", source = "event.description")
    @Mapping(target = "eventDate", source = "event.eventDate")
    @Mapping(target = "participantLimit", source = "event.participantLimit")
    @Mapping(target = "state", source = "event.state")
    @Mapping(target = "title", source = "event.title")
    @Mapping(target = "createdOn", source = "event.createdAt")
    @Mapping(target = "publishedOn", source = "event.publishedAt")
    @Mapping(target = "paid", source = "event.isPaid")
    @Mapping(target = "requestModeration", source = "event.isRequestModeration")
    @Mapping(target = "location", source = "event.location", qualifiedByName = "eventLocationToLocationDto")
    @Mapping(target = "category", source = "event.category", qualifiedByName = "categoryToDto")
    @Mapping(target = "initiator", source = "user")
    @Mapping(target = "confirmedRequests", ignore = true)
    @Mapping(target = "rating", ignore = true)
    EventFullDto toFullDto(Event event, UserShortDto user);

    @BeanMapping(nullValuePropertyMappingStrategy = NullValuePropertyMappingStrategy.IGNORE)
    @Mapping(target = "id", ignore = true)
    @Mapping(target = "category", source = "category", qualifiedByName = "categoryIdToCategory")
    @Mapping(target = "location", source = "location", qualifiedByName = "locationDtoToEventLocation")
    @Mapping(target = "isPaid", source = "paid")
    @Mapping(target = "isRequestModeration", source = "requestModeration")
    @Mapping(target = "state", ignore = true)
    void updateEventFromAdminRequest(UpdateEventAdminRequest request, @MappingTarget Event event);

    @BeanMapping(nullValuePropertyMappingStrategy = NullValuePropertyMappingStrategy.IGNORE)
    @Mapping(target = "id", ignore = true)
    @Mapping(target = "category", source = "category", qualifiedByName = "categoryIdToCategory")
    @Mapping(target = "location", source = "location", qualifiedByName = "locationDtoToEventLocation")
    @Mapping(target = "isPaid", source = "paid")
    @Mapping(target = "isRequestModeration", source = "requestModeration")
    @Mapping(target = "state", ignore = true)
    void updateEventFromUserRequest(UpdateEventUserRequest request, @MappingTarget Event event);

    @Named("categoryIdToCategory")
    default Category toCategory(Long categoryId) {
        if (categoryId == null) {
            return null;
        }
        return new Category(categoryId, null);
    }

    @Named("categoryToDto")
    default CategoryDto categoryToDto(Category category) {
        if (category == null) {
            return null;
        }
        CategoryDto categoryDto = new CategoryDto();
        categoryDto.setId(category.getId());
        categoryDto.setName(category.getName());
        return categoryDto;
    }


    @Named("locationDtoToEventLocation")
    default EventLocation toEventLocation(LocationDto locationDto) {
        if (locationDto == null) {
            return null;
        }
        return new EventLocation(
                locationDto.getLat(),
                locationDto.getLon()
        );
    }

    @Named("eventLocationToLocationDto")
    default LocationDto toLocationDto(EventLocation eventLocation) {
        if (eventLocation == null) {
            return null;
        }
        return new LocationDto(
                eventLocation.getLatitude(),
                eventLocation.getLongitude()
        );
    }
}