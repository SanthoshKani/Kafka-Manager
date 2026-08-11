package com.opentext.security.analytics.messagehub.kafkamanager.clusterregistry.service;

import com.opentext.security.analytics.messagehub.kafkamanager.clusterregistry.domain.SecretEntity;
import java.util.UUID;
import org.springframework.stereotype.Service;

@Service
public class SecretStoreService {

    private final SecretStore secretRepository;
    private final SecretCipherService cipherService;

    public SecretStoreService(SecretStore secretRepository, SecretCipherService cipherService) {
        this.secretRepository = secretRepository;
        this.cipherService = cipherService;
    }

    public UUID store(String purpose, String secret) {
        UUID id = UUID.randomUUID();
        SecretEntity entity = new SecretEntity(id, purpose, cipherService.encrypt(secret), "AES-256-GCM");
        secretRepository.save(entity);
        return id;
    }

    public String resolve(UUID id) {
        if (id == null) {
            return null;
        }
        return cipherService.decrypt(secretRepository.findById(id).orElseThrow().getCiphertext());
    }
}
