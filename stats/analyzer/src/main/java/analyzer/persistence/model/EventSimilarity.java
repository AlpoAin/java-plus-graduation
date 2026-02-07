package analyzer.persistence.model;

import jakarta.persistence.*;
import lombok.*;

import java.time.OffsetDateTime;

@Getter
@Setter
@EqualsAndHashCode
@NoArgsConstructor
@AllArgsConstructor
@Entity
@Builder
@Table(name = "similarities")
public class EventSimilarity {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "event1", nullable = false)
    private Long eventAId;

    @Column(name = "event2", nullable = false)
    private Long eventBId;

    @Column(name = "similarity", nullable = false)
    private double score;

    @Column(name = "ts", nullable = false)
    private OffsetDateTime calculatedAt;
}
