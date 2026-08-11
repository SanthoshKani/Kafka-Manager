package com.opentext.security.analytics.messagehub.kafkamanager.delegationtokens.api;

public record DelegationTokenResponse(String tokenId, String owner, long expiryTimestamp, String hmacBase64) {}
