package org.bsc.langgraph4j.agentexecutor;

import dev.langchain4j.agent.tool.ToolExecutionRequest;
import dev.langchain4j.data.message.AiMessage;
import dev.langchain4j.model.output.Response;
import org.bsc.langgraph4j.DotEnvConfig;
import dev.langchain4j.model.openai.OpenAiChatModel;
import dev.langchain4j.model.output.FinishReason;
import org.bsc.langgraph4j.langchain4j.tool.ToolNode;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.util.List;

import static java.lang.String.format;
import static java.util.Collections.emptyList;
import static org.junit.jupiter.api.Assertions.*;

@Disabled
public class AgentTest {

    @BeforeAll
    public static void loadEnv() {
        DotEnvConfig.load();
    }

    @Test
    public void runAgentTest() throws Exception  {

        assertTrue(DotEnvConfig.valueOf("OPENAI_API_KEY").isPresent());

        OpenAiChatModel chatLanguageModel = OpenAiChatModel.builder()
                .apiKey( DotEnvConfig.valueOf("OPENAI_API_KEY").get() )
                .modelName( "gpt-4o-mini" )
                .logResponses(true)
                .maxRetries(2)
                .temperature(0.0)
                .maxTokens(2000)
                .build();

        TestTool tool = new TestTool();
        Agent agent = Agent.builder()
                .chatLanguageModel(chatLanguageModel)
                .tools( ToolNode.of(tool).toolSpecifications() )
                .build();

        String msg = "hello world";
        Response<AiMessage> response = agent.execute(format("this is an AI test with message: '%s'", msg), emptyList() );

        assertNotNull(response);
        assertEquals(response.finishReason(), FinishReason.TOOL_EXECUTION );
        AiMessage content = response.content();
        assertNotNull(content);
        assertNull( content.text());
        assertTrue(content.hasToolExecutionRequests());
        List<ToolExecutionRequest> toolExecutionRequests = content.toolExecutionRequests();
        assertEquals(1, toolExecutionRequests.size());
        ToolExecutionRequest toolExecutionRequest = toolExecutionRequests.get(0);
        assertEquals("execTest", toolExecutionRequest.name());
        assertEquals("{\"arg0\":\"hello world\"}", toolExecutionRequest.arguments().replaceAll("\n",""));

    }
}
