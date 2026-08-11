package com.opentext.security.analytics.messagehub.kafkamanager.common;

import java.io.Serial;
import org.springframework.http.HttpStatus;

public final class InvalidOperationException extends ApiException {

    @Serial
    private static final long serialVersionUID = 1L;

    public InvalidOperationException(String safeDetail) {
        super(HttpStatus.UNPROCESSABLE_ENTITY, ApiErrorCode.INVALID_STATE, safeDetail);
    }
}
