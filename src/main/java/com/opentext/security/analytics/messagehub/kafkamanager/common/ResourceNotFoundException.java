package com.opentext.security.analytics.messagehub.kafkamanager.common;

import java.io.Serial;
import org.springframework.http.HttpStatus;

public final class ResourceNotFoundException extends ApiException {

    @Serial
    private static final long serialVersionUID = 1L;

    public ResourceNotFoundException(String safeDetail) {
        super(HttpStatus.NOT_FOUND, ApiErrorCode.NOT_FOUND, safeDetail);
    }
}
