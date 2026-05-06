package com.karnataka.ksl.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;
import org.springframework.security.config.annotation.web.configurers.AbstractHttpConfigurer;
import org.springframework.security.config.annotation.web.configurers.HeadersConfigurer;
import org.springframework.security.web.SecurityFilterChain;

/**
 * Security config — opens everything for sandbox/demo use.
 * No login prompt, no token needed.
 * CORS is fully open so the HTML dashboard can call localhost:8080.
 */
@Configuration
@EnableWebSecurity
public class SecurityConfig {

    @Bean
    public SecurityFilterChain filterChain(HttpSecurity http) throws Exception {
        http
            // Disable CSRF — not needed for stateless REST sandbox
            .csrf(AbstractHttpConfigurer::disable)
            // Allow H2 console iframes
            .headers(h -> h.frameOptions(HeadersConfigurer.FrameOptionsConfig::sameOrigin))
            // Permit ALL requests — no login required
            .authorizeHttpRequests(auth -> auth.anyRequest().permitAll())
            // Disable form login and HTTP Basic — stops the browser popup
            .formLogin(AbstractHttpConfigurer::disable)
            .httpBasic(AbstractHttpConfigurer::disable);
        return http.build();
    }
}
