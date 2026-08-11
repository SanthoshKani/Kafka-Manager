package com.opentext.security.analytics.messagehub.kafkamanager.common;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import org.jspecify.annotations.NonNull;
import org.springframework.http.HttpStatus;
import org.springframework.security.core.AuthenticationException;
import org.springframework.security.web.AuthenticationEntryPoint;
import org.springframework.stereotype.Component;

@Component
public class ApiAuthenticationEntryPoint implements AuthenticationEntryPoint {

    private final ProblemResponseWriter writer;

    public ApiAuthenticationEntryPoint(ProblemResponseWriter writer) {
        this.writer = writer;
    }

    @Override
    public void commence(
            @NonNull HttpServletRequest request,
            @NonNull HttpServletResponse response,
            @NonNull AuthenticationException authException)
            throws IOException {
        writer.write(request, response, HttpStatus.UNAUTHORIZED, "Authentication required", "AUTHENTICATION_REQUIRED");
    }
}
