package com.opentext.security.analytics.messagehub.kafkamanager.config;

import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.Optional;
import java.util.UUID;
import org.slf4j.MDC;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;
import org.springframework.web.util.ContentCachingRequestWrapper;
import org.springframework.web.util.ContentCachingResponseWrapper;

@Component
@Order(Ordered.HIGHEST_PRECEDENCE)
public class CorrelationIdFilter extends OncePerRequestFilter {

    public static final String HEADER = "X-Correlation-Id";
    public static final String MDC_KEY = "correlationId";
    private static final int ERROR_STATUS_THRESHOLD = 400;

    @Override
    protected void doFilterInternal(HttpServletRequest request, HttpServletResponse response, FilterChain filterChain)
            throws ServletException, IOException {
        var correlationId = Optional.ofNullable(request.getHeader(HEADER))
                .filter(value -> !value.isBlank())
                .orElseGet(() -> UUID.randomUUID().toString());
        MDC.put(MDC_KEY, correlationId);
        response.setHeader(HEADER, correlationId);

        // Wrap request/response for logging
        ContentCachingRequestWrapper wrappedRequest = new ContentCachingRequestWrapper(request, 1024 * 1024);
        ContentCachingResponseWrapper wrappedResponse = new ContentCachingResponseWrapper(response);

        long startTime = System.currentTimeMillis();
        try {
            filterChain.doFilter(wrappedRequest, wrappedResponse);
        } finally {
            long duration = System.currentTimeMillis() - startTime;
            logRequestResponse(wrappedRequest, wrappedResponse, duration);
            MDC.remove(MDC_KEY);
        }
    }

    private void logRequestResponse(
            ContentCachingRequestWrapper request, ContentCachingResponseWrapper response, long duration) {
        String method = request.getMethod();
        String uri = request.getRequestURI();
        String query = request.getQueryString();
        int status = response.getStatus();

        // Log at INFO level for errors, DEBUG for success
        if (status >= ERROR_STATUS_THRESHOLD) {
            org.slf4j.LoggerFactory.getLogger(CorrelationIdFilter.class)
                    .info("HTTP {} {}?{} -> {} ({}ms)", method, uri, query, status, duration);
        } else {
            org.slf4j.LoggerFactory.getLogger(CorrelationIdFilter.class)
                    .debug("HTTP {} {}?{} -> {} ({}ms)", method, uri, query, status, duration);
        }
    }
}
