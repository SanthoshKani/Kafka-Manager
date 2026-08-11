package com.opentext.security.analytics.messagehub.kafkamanager.common;

import org.springframework.http.HttpStatus;

public final class ConflictException extends ApiException {

    private static final long serialVersionUID = 1L;

    public ConflictException(String safeDetail) {
        super(HttpStatus.CONFLICT, ApiErrorCode.CONFLICT, safeDetail);
    }
}
