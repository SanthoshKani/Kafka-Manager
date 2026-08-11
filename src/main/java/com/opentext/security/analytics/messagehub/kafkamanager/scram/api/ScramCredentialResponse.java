package com.opentext.security.analytics.messagehub.kafkamanager.scram.api;

public record ScramCredentialResponse(String userName, String mechanism, int iterations) {}
