package org.bsc.langgraph4j.agentexecutor.actions;

import dev.langchain4j.agent.tool.ToolExecutionRequest;
import dev.langchain4j.data.message.AiMessage;
import dev.langchain4j.data.message.ToolExecutionResultMessage;
import dev.langchain4j.model.output.FinishReason;
import dev.langchain4j.model.output.Response;
import lombok.extern.slf4j.Slf4j;
import org.bsc.langgraph4j.action.NodeAction;
import org.bsc.langgraph4j.agentexecutor.*;
import org.bsc.langgraph4j.agentexecutor.state.AgentAction;
import org.bsc.langgraph4j.agentexecutor.state.AgentFinish;
import org.bsc.langgraph4j.agentexecutor.state.AgentOutcome;
import org.bsc.langgraph4j.agentexecutor.state.IntermediateStep;
import org.bsc.langgraph4j.langchain4j.generators.LLMStreamingGenerator;

import java.util.*;

/**
 * The CallAgent class implements the NodeAction interface for handling 
 * actions related to an AgentExecutor's state.
 */
@Slf4j
public class CallAgent implements NodeAction<AgentExecutor.State> {

    final Agent agent;

    /**
     * Constructs a CallAgent with the specified agent.
     *
     * @param agent the agent to be associated with this CallAgent
     */
    public CallAgent( Agent agent ) {
        this.agent = agent;
    }

    /**
     * Maps the result of the response from an AI message to a structured format.
     *
     * @param response the response containing the AI message
     * @return a map containing the agent's outcome
     * @throws IllegalStateException if the finish reason of the response is unsupported
     */
    private Map<String,Object> mapResult( Response<AiMessage> response )  {

        AiMessage content = response.content();
        System.out.println("LLM response: " + response);

        if( response.finishReason() == FinishReason.STOP && !response.content().hasToolExecutionRequests()) {
            String result = content.text();
            AgentFinish finish = new AgentFinish(Collections.singletonMap("returnValues", result), result);
            return Collections.singletonMap("agent_outcome", new AgentOutcome(Collections.emptyList(), finish));
        }

        if (response.finishReason() == FinishReason.TOOL_EXECUTION || response.content().hasToolExecutionRequests() ) {

            List<ToolExecutionRequest> toolExecutionRequests = response.content().toolExecutionRequests();
            List<AgentAction> actions = new ArrayList<>();
            for (ToolExecutionRequest request: toolExecutionRequests) {
                ToolExecutionRequest reqWithId = request;
                if (request.id() == null) {
                    reqWithId = ToolExecutionRequest.builder().id("call_" + UUID.randomUUID())
                            .name(request.name())
                            .arguments(request.arguments())
                            .build();
                }
                AgentAction action = new AgentAction(reqWithId, "");
                actions.add(action);
            }

            return Collections.singletonMap("agent_outcome", new AgentOutcome(actions, null));

        }

        throw new IllegalStateException("Unsupported finish reason: " + response.finishReason() );
    }

    /**
     * Applies the action to the given state and returns the result.
     *
     * @param state the state to which the action is applied
     * @return a map containing the agent's outcome
     * @throws IllegalArgumentException if no input is provided in the state
     */
    @Override
    public Map<String,Object> apply( AgentExecutor.State state )  {
        log.trace( "callAgent" );
        String input = state.input()
                .orElseThrow(() -> new IllegalArgumentException("no input provided!"));

        List<IntermediateStep> intermediateSteps = state.intermediateSteps();

        if( agent.isStreaming()) {

            LLMStreamingGenerator generator = LLMStreamingGenerator.<AiMessage, AgentExecutor.State>builder()
                    .mapResult( this::mapResult )
                    .startingNode("agent")
                    .startingState( state )
                    .build();

            if (state.systemMessage().isPresent()) {
                agent.execute(state.systemMessage().get(), input, intermediateSteps, generator.handler());
            } else {
                agent.execute(input, intermediateSteps, generator.handler());
            }

            return Collections.singletonMap( "agent_outcome", generator);
        }
        else {
            Response<AiMessage> response =state.systemMessage()
                    .map(systemMsg -> agent.execute(systemMsg, input, intermediateSteps))
                    .orElse(agent.execute(input, intermediateSteps));

            return mapResult(response);
        }

    }

}
