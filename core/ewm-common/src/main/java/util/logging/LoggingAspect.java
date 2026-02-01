package util.logging;

import lombok.extern.slf4j.Slf4j;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.springframework.stereotype.Component;

@Slf4j
@Aspect
@Component
public class LoggingAspect {
    @Around("@annotation(util.logging.Loggable)")
    public Object logExecution(ProceedingJoinPoint joinPoint) throws Throwable {
        log.info("Entering method: {}", joinPoint.getSignature().getName());
        long startTime = System.currentTimeMillis();
        Object[] args = joinPoint.getArgs();
        log.info("Request parameters: {}", args);
        Object result = joinPoint.proceed(args);
        long duration = System.currentTimeMillis() - startTime;
        log.info("Exiting method: {} - Response: {} ({}ms)", joinPoint.getSignature().getName(), result, duration);
        return result;
    }
}
