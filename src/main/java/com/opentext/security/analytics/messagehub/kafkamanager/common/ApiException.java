package com.opentext.security.analytics.messagehub.kafkamanager.common;

import java.io.Serial;
import org.springframework.http.HttpStatus;

public class ApiException extends RuntimeException {

    @Serial
    private static final long serialVersionUID = 1L;

    private final HttpStatus status;
    private final ApiErrorCode errorCode;
    private final String safeDetail;

    public ApiException(HttpStatus status, ApiErrorCode errorCode, String safeDetail) {
        super(safeDetail);
        this.status = status;
        this.errorCode = errorCode;
        this.safeDetail = safeDetail;
    }

    public HttpStatus getStatus() {
        return status;
    }

    public ApiErrorCode getErrorCode() {
        return errorCode;
    }

    public String getSafeDetail() {
        return safeDetail;
    }
}
