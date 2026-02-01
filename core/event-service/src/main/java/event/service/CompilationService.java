package event.service;

import dto.compilation.CompilationResponse;
import dto.compilation.NewCompilationRequest;
import dto.compilation.UpdateCompilationRequest;
import org.springframework.data.domain.Pageable;

import java.util.List;

public interface CompilationService {
    CompilationResponse createCompilation(NewCompilationRequest request);

    void deleteCompilation(Long compId);

    CompilationResponse updateCompilation(Long compId, UpdateCompilationRequest request);

    List<CompilationResponse> getCompilations(Boolean pinned, Pageable pageable);

    CompilationResponse getCompilationById(Long compId);
}
