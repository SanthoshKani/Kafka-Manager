package com.opentext.security.analytics.messagehub.kafkamanager;

import com.opentext.security.analytics.messagehub.kafkamanager.config.KafkaManagerProperties;
import com.opentext.security.analytics.messagehub.kafkamanager.config.PrometheusScrapeProperties;
import org.bouncycastle.crypto.CryptoServicesRegistrar;
import org.bouncycastle.jcajce.provider.BouncyCastleFipsProvider;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.scheduling.annotation.EnableScheduling;

import java.security.Provider;
import java.security.Security;

@SpringBootApplication
@EnableScheduling
@EnableConfigurationProperties({KafkaManagerProperties.class, PrometheusScrapeProperties.class})
public class KafkaManagerApplication {

    public static void main(String[] args) {
        initializeFips();
        SpringApplication.run(KafkaManagerApplication.class, args);
    }

    private static void initializeFips() {
        /*
         * Approved-only mode is thread-local. Set it before constructing
         * the provider and before Spring starts worker threads.
         */
        CryptoServicesRegistrar.setApprovedOnlyMode(true);

        Provider existing = Security.getProvider("BCFIPS");

        if (existing == null) {
            int position = Security.insertProviderAt(new BouncyCastleFipsProvider(), 1);

            if (position == -1) {
                throw new IllegalStateException("Unable to register BCFIPS provider");
            }
        }

        Provider provider = Security.getProvider("BCFIPS");

        if (!(provider instanceof BouncyCastleFipsProvider)) {
            throw new IllegalStateException("BCFIPS is not backed by BouncyCastleFipsProvider: " + provider);
        }

        if (!CryptoServicesRegistrar.isInApprovedOnlyMode()) {
            throw new IllegalStateException("Bouncy Castle approved-only mode is not active");
        }

        System.out.println("Provider: " + provider.getName()
                + ", version: " + provider.getVersionStr()
                + ", approved-only: "
                + CryptoServicesRegistrar.isInApprovedOnlyMode());
    }
}
