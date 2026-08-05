package br.com.itau.challenge.balance.adapter.observability

import org.junit.jupiter.api.Test
import org.mockito.BDDMockito.given
import org.mockito.Mockito.any
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc
import org.springframework.test.context.bean.override.mockito.MockitoBean
import org.springframework.test.web.servlet.MockMvc
import org.springframework.test.web.servlet.get
import software.amazon.awssdk.services.dynamodb.DynamoDbClient
import software.amazon.awssdk.services.dynamodb.model.DescribeTableRequest
import software.amazon.awssdk.services.dynamodb.model.DescribeTableResponse
import software.amazon.awssdk.services.dynamodb.model.TableDescription
import software.amazon.awssdk.services.dynamodb.model.TableStatus

@SpringBootTest
@AutoConfigureMockMvc
class HealthEndpointTest(
    @Autowired private val mockMvc: MockMvc,
) {

    @MockitoBean
    private lateinit var dynamoDbClient: DynamoDbClient

    @Test
    fun `should expose liveness probe`() {
        mockMvc.get("/actuator/health/liveness").andExpect {
            status { isOk() }
            jsonPath("$.status") { value("UP") }
        }
    }

    @Test
    fun `should expose readiness probe including dynamodb`() {
        given(dynamoDbClient.describeTable(any(DescribeTableRequest::class.java))).willReturn(
            DescribeTableResponse
                .builder()
                .table(
                    TableDescription
                        .builder()
                        .tableName("AccountBalances")
                        .tableStatus(TableStatus.ACTIVE)
                        .build(),
                ).build(),
        )

        mockMvc.get("/actuator/health/readiness").andExpect {
            status { isOk() }
            jsonPath("$.status") { value("UP") }
        }
    }
}
