package br.com.itau.challenge.config

import br.com.itau.challenge.balance.adapter.input.web.security.ApiSecurityProperties
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.context.annotation.Configuration

@Configuration
@EnableConfigurationProperties(ApiSecurityProperties::class)
class ApiSecurityConfig
