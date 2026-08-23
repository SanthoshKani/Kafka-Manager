package com.opentext.security.analytics.messagehub.kafkamanager.config;

import io.swagger.v3.oas.annotations.OpenAPIDefinition;
import io.swagger.v3.oas.annotations.enums.SecuritySchemeType;
import io.swagger.v3.oas.annotations.info.Contact;
import io.swagger.v3.oas.annotations.info.Info;
import io.swagger.v3.oas.annotations.info.License;
import io.swagger.v3.oas.annotations.security.SecurityScheme;
import io.swagger.v3.oas.annotations.servers.Server;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;

@Configuration
@Profile("prod")
@OpenAPIDefinition(
        info =
                @Info(
                        title = "Kafka Manager API",
                        version = "v1",
                        description = "KRaft-native Kafka cluster manager",
                        contact = @Contact(name = "OpenText", url = "https://opentext.com"),
                        license = @License(name = "Apache 2.0", url = "https://www.apache.org/licenses/LICENSE-2.0")),
        security = @io.swagger.v3.oas.annotations.security.SecurityRequirement(name = "bearerAuth"),
        servers = {@Server(url = "/", description = "Default Server")})
@SecurityScheme(name = "bearerAuth", type = SecuritySchemeType.HTTP, scheme = "bearer", bearerFormat = "JWT")
public class OpenApiConfig {}
