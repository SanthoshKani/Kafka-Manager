package com.opentext.security.analytics.messagehub.kafkamanager.common;

import com.opentext.security.analytics.messagehub.kafkamanager.config.CorrelationIdFilter;
import jakarta.servlet.http.HttpServletRequest;
import java.net.URI;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import org.slf4j.MDC;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.http.HttpStatus;
import org.springframework.http.ProblemDetail;
import org.springframework.validation.FieldError;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

@RestControllerAdvice
@Order(Ordered.HIGHEST_PRECEDENCE)
public class ApiProblemAdvice {

    @ExceptionHandler(ApiException.class)
    ProblemDetail handleApiException(ApiException exception, HttpServletRequest request) {
        return problem(
                exception.getStatus(),
                request,
                exception.getSafeDetail(),
                exception.getErrorCode().name(),
                null);
    }

    @ExceptionHandler(MethodArgumentNotValidException.class)
    ProblemDetail handleValidation(MethodArgumentNotValidException exception, HttpServletRequest request) {
        Map<String, String> errors = new LinkedHashMap<>();
        for (FieldError fieldError : exception.getBindingResult().getFieldErrors()) {
            errors.put(fieldError.getField(), fieldError.getDefaultMessage());
        }
        return problem(
                HttpStatus.BAD_REQUEST, request, "Validation failed", ApiErrorCode.VALIDATION_ERROR.name(), errors);
    }

    @ExceptionHandler(Exception.class)
    ProblemDetail handleUnexpected(Exception exception, HttpServletRequest request) {
        return problem(
                HttpStatus.INTERNAL_SERVER_ERROR,
                request,
                "Unexpected server error",
                ApiErrorCode.OPERATION_FAILED.name(),
                null);
    }

    private static ProblemDetail problem(
            HttpStatus status,
            HttpServletRequest request,
            String detail,
            String errorCode,
            Map<String, String> validationErrors) {
        ProblemDetail problem = ProblemDetail.forStatusAndDetail(status, detail);
        problem.setType(URI.create("urn:kafka-manager:error:" + errorCode.toLowerCase(Locale.ROOT)));
        problem.setTitle(status.getReasonPhrase());
        problem.setInstance(URI.create(request.getRequestURI()));
        problem.setProperty("errorCode", errorCode);
        problem.setProperty("timestamp", Instant.now().toString());
        problem.setProperty(
                "correlationId",
                MDC.get(CorrelationIdFilter.MDC_KEY) != null
                        ? MDC.get(CorrelationIdFilter.MDC_KEY)
                        : request.getHeader(CorrelationIdFilter.HEADER));
        if (validationErrors != null && !validationErrors.isEmpty()) {
            problem.setProperty("validationErrors", validationErrors);
        }
        return problem;
    }
}
