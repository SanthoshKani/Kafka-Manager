package com.opentext.security.analytics.messagehub.kafkamanager.config;

import com.opentext.security.analytics.messagehub.kafkamanager.common.ApiAccessDeniedHandler;
import com.opentext.security.analytics.messagehub.kafkamanager.common.ApiAuthenticationEntryPoint;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.http.HttpMethod;
import org.springframework.security.config.Customizer;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configurers.AbstractHttpConfigurer;
import org.springframework.security.config.annotation.web.configurers.HeadersConfigurer;
import org.springframework.security.config.http.SessionCreationPolicy;
import org.springframework.security.core.userdetails.User;
import org.springframework.security.core.userdetails.UserDetails;
import org.springframework.security.core.userdetails.UserDetailsService;
import org.springframework.security.crypto.bcrypt.BCryptPasswordEncoder;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.security.oauth2.jwt.JwtDecoder;
import org.springframework.security.oauth2.jwt.NimbusJwtDecoder;
import org.springframework.security.provisioning.InMemoryUserDetailsManager;
import org.springframework.security.web.SecurityFilterChain;
import org.springframework.security.web.header.writers.StaticHeadersWriter;

@Configuration
@Profile("prod")
public class SecurityConfig {

    @Bean
    SecurityFilterChain securityFilterChain(
            HttpSecurity http,
            ApiAuthenticationEntryPoint authenticationEntryPoint,
            ApiAccessDeniedHandler accessDeniedHandler,
            KafkaManagerProperties properties) {
        http.csrf(AbstractHttpConfigurer::disable)
                .sessionManagement(session -> session.sessionCreationPolicy(SessionCreationPolicy.STATELESS))
                .authorizeHttpRequests(auth -> auth.requestMatchers(
                                HttpMethod.GET,
                                "/management/health",
                                "/management/health/**",
                                "/management/info",
                                "/v3/api-docs/**",
                                "/v3/api-docs.yaml",
                                "/v3/api-docs",
                                "/openapi.yaml",
                                "/api-docs/**",
                                "/api-docs",
                                "/favicon.ico")
                        .permitAll()
                        .requestMatchers("/swagger-ui/**", "/swagger-ui.html", "/swagger-ui")
                        .permitAll()
                        .anyRequest()
                        .authenticated())
                .exceptionHandling(exception -> exception
                        .authenticationEntryPoint(authenticationEntryPoint)
                        .accessDeniedHandler(accessDeniedHandler))
                .httpBasic(Customizer.withDefaults());

        var oauth2 = properties.security() != null ? properties.security().oauth2ResourceServer() : null;
        if (oauth2 != null
                && ((oauth2.issuerUri() != null && !oauth2.issuerUri().isBlank())
                        || (oauth2.jwkSetUri() != null && !oauth2.jwkSetUri().isBlank()))) {
            http.oauth2ResourceServer(oauth2Config -> oauth2Config.jwt(Customizer.withDefaults()));
        }

        http.headers(headers -> headers.frameOptions(HeadersConfigurer.FrameOptionsConfig::disable)
                .addHeaderWriter(new StaticHeadersWriter("X-Content-Type-Options", "nosniff"))
                .addHeaderWriter(new StaticHeadersWriter("X-Frame-Options", "DENY"))
                .addHeaderWriter(new StaticHeadersWriter("X-XSS-Protection", "1; mode=block"))
                .addHeaderWriter(new StaticHeadersWriter("Referrer-Policy", "strict-origin-when-cross-origin"))
                .addHeaderWriter(new StaticHeadersWriter(
                        "Content-Security-Policy",
                        "default-src 'self'; frame-ancestors 'none'; base-uri 'self'; form-action 'self'"))
                .httpStrictTransportSecurity(hsts ->
                        hsts.includeSubDomains(true).maxAgeInSeconds(31536000).preload(true)));
        return http.build();
    }

    @Bean
    UserDetailsService userDetailsService(KafkaManagerProperties properties, PasswordEncoder passwordEncoder) {
        var basicAuth = properties.security() != null ? properties.security().basicAuth() : null;
        if (basicAuth != null
                && basicAuth.username() != null
                && !basicAuth.username().isBlank()
                && basicAuth.password() != null
                && !basicAuth.password().isBlank()) {
            UserDetails user = User.withUsername(basicAuth.username())
                    .password(passwordEncoder.encode(basicAuth.password()))
                    .roles("ADMIN", "USER")
                    .build();
            return new InMemoryUserDetailsManager(user);
        }
        throw new IllegalStateException("Production profile requires app.security.basic-auth.username and password");
    }

    @Bean
    PasswordEncoder passwordEncoder() {
        return new BCryptPasswordEncoder();
    }

    @Bean
    @org.springframework.boot.autoconfigure.condition.ConditionalOnProperty(
            prefix = "app.security.oauth2-resource-server",
            name = {"issuer-uri", "jwk-set-uri"},
            matchIfMissing = false)
    JwtDecoder jwtDecoder(KafkaManagerProperties properties) {
        var oauth2 = properties.security() != null ? properties.security().oauth2ResourceServer() : null;
        if (oauth2 != null && oauth2.issuerUri() != null && !oauth2.issuerUri().isBlank()) {
            return org.springframework.security.oauth2.jwt.JwtDecoders.fromIssuerLocation(oauth2.issuerUri());
        }
        if (oauth2 != null && oauth2.jwkSetUri() != null && !oauth2.jwkSetUri().isBlank()) {
            return NimbusJwtDecoder.withJwkSetUri(oauth2.jwkSetUri()).build();
        }
        return null;
    }
}
