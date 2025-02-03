package org.bsc.langgraph4j.agentexecutor;

import org.bsc.langgraph4j.agentexecutor.serializer.jackson.JSONStateSerializer;
import org.bsc.langgraph4j.agentexecutor.serializer.std.STDStateSerializer;
import org.bsc.langgraph4j.agentexecutor.state.AgentAction;
import org.bsc.langgraph4j.agentexecutor.state.AgentOutcome;
import org.bsc.langgraph4j.agentexecutor.state.IntermediateStep;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ObjectInputStream;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;


public class SerializableTest {

    @Test
    public void customJsonStateDeserializeTest() throws Exception {

        String data = "{\n" +
                "                \"input\":\"perform test twice\",\n" +
                "                \"intermediate_steps\":[],\n" +
                "                \"agent_outcome\":{\n" +
                "                    \"actions\":[" +
                "                        {\n" +
                "                          \"toolExecutionRequest\":{\n" +
                "                              \"id\":\"call_m6TnU4B1Net6tm6zMPzXKJxP\",\n" +
                "                              \"name\":\"execTest\",\n" +
                "                              \"arguments\":\"{\\\"arg0\\\":\\\"perform test\\\"}\"\n" +
                "                          },\n" +
                "                          \"log\":\"\"\n" +
                "                        }" +
                "                ],\n" +
                "                    \"finish\":null\n" +
                "                }\n" +
                "            }";

        JSONStateSerializer serializer = new JSONStateSerializer();


        AgentExecutor.State state = serializer.read( data );

        assertNotNull(state);
        assertTrue(state.input().isPresent());
        assertEquals("perform test twice", state.input().get() );
        assertNotNull(state.intermediateSteps());
        assertInstanceOf( List.class, state.intermediateSteps() );
        List<IntermediateStep> intermediateSteps = state.intermediateSteps();
        assertTrue(intermediateSteps.isEmpty());
        assertTrue( state.agentOutcome().isPresent());
        assertInstanceOf( AgentOutcome.class, state.agentOutcome().get() );
        AgentOutcome agentOutcome = state.agentOutcome().get();
        assertNotNull(agentOutcome);
        List<AgentAction> actions = agentOutcome.getActions();
        assertNotNull(actions);
        assertEquals("execTest", actions.get(0).getToolExecutionRequest().name());
        assertEquals("{\"arg0\":\"perform test\"}", actions.get(0).getToolExecutionRequest().arguments());

    }

    @Test
    public void jsonSerializeTest2() throws Exception {

        String data = "{\n" +
                "    \"input\":\"perform test another time\",\n" +
                "    \"intermediate_steps\":[\n" +
                "        {\n" +
                "            \"action\": {\n" +
                "                \"toolExecutionRequest\":{\n" +
                "                    \"id\":\"call_B4KyzWwytOlrVG6cY3HfVeYq\",\n" +
                "                    \"name\":\"execTest\",\n" +
                "                    \"arguments\":\"{\\\"arg0\\\":\\\"perform test once\\\"}\"\n" +
                "                },\n" +
                "                \"log\":\"\"\n" +
                "            },\n" +
                "            \"observation\":\"test tool executed: perform test once\"\n" +
                "        }\n" +
                "    ],\n" +
                "    \"agent_outcome\":{\n" +
                "        \"actions\":[" +
                "            {\n" +
                "              \"toolExecutionRequest\":{\n" +
                "                  \"id\":\"call_0LiS88saSYysfgAKHBMrIVEF\",\n" +
                "                  \"name\":\"execTest\",\n" +
                "                  \"arguments\":\"{\\\"arg0\\\":\\\"perform test once\\\"}\"\n" +
                "              },\n" +
                "              \"log\":\"\"\n" +
                "            }," +
                "            {\n" +
                "              \"toolExecutionRequest\":{\n" +
                "                  \"id\":\"call_0LiS88saSYysfgAKHBMrIVEF\",\n" +
                "                  \"name\":\"execTest\",\n" +
                "                  \"arguments\":\"{\\\"arg0\\\":\\\"perform test twice\\\"}\"\n" +
                "              },\n" +
                "              \"log\":\"\"\n" +
                "            }" +
                "        ],\n" +
                "        \"finish\":null\n" +
                "    }\n" +
                "}";

        JSONStateSerializer serializer = new JSONStateSerializer() ;

        AgentExecutor.State state = serializer.read(data);

        assertNotNull(state);
        assertTrue(state.input().isPresent());
        assertEquals("perform test another time", state.input().get() );
        assertNotNull(state.intermediateSteps() );
        assertInstanceOf( List.class, state.intermediateSteps() );
        List<IntermediateStep> intermediateSteps =state.intermediateSteps();
        assertEquals(1,intermediateSteps.size());
        IntermediateStep intermediateStep = intermediateSteps.get(0);
        assertNotNull(intermediateStep);
        assertEquals("test tool executed: perform test once", intermediateStep.getObservation() );
        assertTrue(state.agentOutcome().isPresent());
        assertInstanceOf( AgentOutcome.class, state.agentOutcome().get() );
        AgentOutcome agentOutcome = state.agentOutcome().get();
        assertNotNull(agentOutcome);
        List<AgentAction> actions = agentOutcome.getActions();
        assertNotNull(actions);
        assertEquals("execTest", actions.get(0).getToolExecutionRequest().name());
        assertEquals("{\"arg0\":\"perform test once\"}", actions.get(0).getToolExecutionRequest().arguments());

    }
}

