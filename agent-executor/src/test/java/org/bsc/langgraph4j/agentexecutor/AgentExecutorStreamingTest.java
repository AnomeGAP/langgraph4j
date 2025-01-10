package org.bsc.langgraph4j.agentexecutor;

import dev.langchain4j.model.openai.OpenAiStreamingChatModel;
import org.bsc.async.AsyncGenerator;
import org.bsc.langgraph4j.*;
import org.bsc.langgraph4j.checkpoint.BaseCheckpointSaver;
import org.bsc.langgraph4j.checkpoint.MemorySaver;
import org.bsc.langgraph4j.state.AgentState;
import org.bsc.langgraph4j.streaming.StreamingOutput;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.bsc.langgraph4j.StateGraph.END;
import static org.bsc.langgraph4j.StateGraph.START;
import static org.bsc.langgraph4j.action.AsyncEdgeAction.edge_async;
import static org.bsc.langgraph4j.action.AsyncNodeAction.node_async;
import static org.junit.jupiter.api.Assertions.*;

@Disabled
public class AgentExecutorStreamingTest {

    @BeforeAll
    public static void loadEnv() {
        DotEnvConfig.load();
    }

    private StateGraph<AgentExecutor.State> newGraph()  throws Exception {

        String openApiKey = DotEnvConfig.valueOf("OPENAI_API_KEY")
                .orElseThrow( () -> new IllegalArgumentException("no APIKEY provided!"));

        OpenAiStreamingChatModel chatLanguageModel = OpenAiStreamingChatModel.builder()
                .apiKey( openApiKey )
                .modelName( "gpt-4o-mini" )
                .logResponses(true)
                .temperature(0.0)
                .maxTokens(2000)
                .build();

        return AgentExecutor.graphBuilder()
                .chatLanguageModel(chatLanguageModel)
                .toolSpecification(new TestTool())
                .build();
    }

    private List<AgentExecutor.State> executeAgent( String prompt )  throws Exception {

        return toStateList( newGraph().compile().stream(Collections.singletonMap( "input", prompt ) ) );
    }

    private List<AgentExecutor.State> executeAgent( String prompt,
                                                    String threadId,
                                                    BaseCheckpointSaver saver)  throws Exception
    {

        CompileConfig compileConfig = CompileConfig.builder()
                .checkpointSaver( saver )
                .build();

        RunnableConfig config = RunnableConfig.builder().threadId(threadId).build();

        CompiledGraph graph = newGraph().compile( compileConfig );

        return toStateList(  graph.stream( Collections.singletonMap( "input", prompt ), config ) );
    }

    private List<AgentExecutor.State> toStateList(AsyncGenerator<NodeOutput<AgentExecutor.State>> generator ) {

        return generator.stream()
                .filter( s -> {
                    if( s instanceof StreamingOutput ) {
                        StreamingOutput<AgentExecutor.State> streamingOutput = (StreamingOutput<AgentExecutor.State>) s;
                        System.out.printf( "%s '%s'\n", streamingOutput.node(), streamingOutput.chunk() );
                        return false;
                    }
                    return true;
                })
                .peek( s -> System.out.printf( "NODE: %s\n", s.node() ) )
                .map( NodeOutput::state)
                .collect(Collectors.toList());
    }

    @Test
    void executeAgentWithSingleToolInvocation() throws Exception {

        List<AgentExecutor.State> states = executeAgent("what is the result of test with messages: 'MY FIRST TEST'");
        AgentExecutor.State state = states.get( states.size() - 1 );
        assertNotNull(state);
        assertFalse(state.intermediateSteps().isEmpty());
        assertEquals( 1, state.intermediateSteps().size());
        assertTrue(state.agentOutcome().isPresent());
        assertNotNull(state.agentOutcome().get().getFinish());
        assertTrue( state.agentOutcome().get().getFinish().getReturnValues().containsKey("returnValues"));
        String returnValues = state.agentOutcome().get().getFinish().getReturnValues().get("returnValues").toString();
        assertTrue( returnValues.contains( "MY FIRST TEST") );
        System.out.println(returnValues);
    }

    @Test
    void executeAgentWithDoubleToolInvocation() throws Exception {

        List<AgentExecutor.State> states = executeAgent("what is the result of test with messages: 'MY FIRST TEST' and the result of test with message: 'MY SECOND TEST'");
        AgentExecutor.State state = states.get( states.size() - 1 );
        assertNotNull(state);
        assertFalse(state.intermediateSteps().isEmpty());
        assertEquals( 2, state.intermediateSteps().size());
        assertTrue(state.agentOutcome().isPresent());
        assertNotNull(state.agentOutcome().get().getFinish());
        assertTrue( state.agentOutcome().get().getFinish().getReturnValues().containsKey("returnValues"));
        String returnValues = state.agentOutcome().get().getFinish().getReturnValues().get("returnValues").toString();
        assertTrue( returnValues.contains( "MY FIRST TEST") );
        assertTrue( returnValues.contains( "MY SECOND TEST") );
        System.out.println(returnValues);

    }

    @Test
    void executeAgentWithDoubleToolInvocationWithCheckpoint() throws Exception {

        MemorySaver saver = new MemorySaver();
        List<AgentExecutor.State> states = executeAgent(
                "what is the result of test with messages: 'MY FIRST TEST' and the result of test with message: 'MY SECOND TEST'",
                "thread_1",
                saver
                );
        assertEquals( 7, states.size() ); // iterations
        AgentExecutor.State state = states.get( states.size() - 1 );
        assertNotNull(state);
        assertFalse(state.intermediateSteps().isEmpty());
        assertEquals( 2, state.intermediateSteps().size());
        assertTrue(state.agentOutcome().isPresent());
        assertNotNull(state.agentOutcome().get().getFinish());
        assertTrue( state.agentOutcome().get().getFinish().getReturnValues().containsKey("returnValues"));
        String returnValues = state.agentOutcome().get().getFinish().getReturnValues().get("returnValues").toString();
        assertTrue( returnValues.contains( "MY FIRST TEST") );
        assertTrue( returnValues.contains( "MY SECOND TEST") );
        System.out.println(returnValues);

        states = executeAgent(
                "what is the result of test with messages: 'MY FIRST TEST' and the result of test with message: 'MY SECOND TEST'",
                "thread_1",
                saver
        );
        assertEquals( 3, states.size() ); // iterations
        state = states.get( states.size() - 1 );
        assertNotNull(state);
        assertTrue(state.agentOutcome().isPresent());
        assertNotNull(state.agentOutcome().get().getFinish());
        assertTrue( state.agentOutcome().get().getFinish().getReturnValues().containsKey("returnValues"));
        returnValues = state.agentOutcome().get().getFinish().getReturnValues().get("returnValues").toString();
        assertTrue( returnValues.contains( "MY FIRST TEST") );
        assertTrue( returnValues.contains( "MY SECOND TEST") );
        System.out.println(returnValues);
    }

    @Test
    public void getGraphTest() throws Exception {

        Map<String, String> map = new HashMap<>();
        map.put("continue", "action");
        map.put("end", END);
        CompiledGraph app = new StateGraph<>(AgentState::new)
            .addEdge(START,"agent")
            .addNode( "agent", node_async( state -> Collections.EMPTY_MAP ))
            .addNode( "action", node_async( state -> Collections.EMPTY_MAP ))
            .addConditionalEdges(
                    "agent",
                    edge_async(state -> ""),
                    map
            )
            .addEdge("action", "agent")
            .compile();

        GraphRepresentation plantUml = app.getGraph( GraphRepresentation.Type.PLANTUML, "Agent Executor" );

        System.out.println( plantUml.getContent() );

        GraphRepresentation mermaid = app.getGraph( GraphRepresentation.Type.MERMAID, "Agent Executor" );

        System.out.println( mermaid.getContent() );
    }
}
