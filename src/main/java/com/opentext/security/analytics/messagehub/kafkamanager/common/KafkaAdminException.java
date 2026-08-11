package com.opentext.security.analytics.messagehub.kafkamanager.common;

import java.io.Serial;
import org.springframework.http.HttpStatus;

public final class KafkaAdminException extends ApiException {

    @Serial
    private static final long serialVersionUID = 1L;

    private final String kafkaCategory;

    public KafkaAdminException(HttpStatus status, ApiErrorCode errorCode, String kafkaCategory, String safeDetail) {
        super(status, errorCode, safeDetail);
        this.kafkaCategory = kafkaCategory;
    }

    public String getKafkaCategory() {
        return kafkaCategory;
    }
}
