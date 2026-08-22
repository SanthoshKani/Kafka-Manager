package com.opentext.security.analytics.messagehub.kafkamanager.common;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import org.jspecify.annotations.NonNull;
import org.springframework.context.annotation.Profile;
import org.springframework.http.HttpStatus;
import org.springframework.security.access.AccessDeniedException;
import org.springframework.security.web.access.AccessDeniedHandler;
import org.springframework.stereotype.Component;

@Component
@Profile("prod")
public class ApiAccessDeniedHandler implements AccessDeniedHandler {

    private final ProblemResponseWriter writer;

    public ApiAccessDeniedHandler(ProblemResponseWriter writer) {
        this.writer = writer;
    }

    @Override
    public void handle(
            @NonNull HttpServletRequest request,
            @NonNull HttpServletResponse response,
            @NonNull AccessDeniedException accessDeniedException)
            throws IOException {
        writer.write(request, response, HttpStatus.FORBIDDEN, "Access denied", "AUTHORIZATION_DENIED");
    }
}
