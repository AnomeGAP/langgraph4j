package org.bsc.langgraph4j.agentexecutor.actions;

import dev.langchain4j.agent.tool.ToolExecutionRequest;
import dev.langchain4j.data.message.ToolExecutionResultMessage;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.bsc.langgraph4j.action.NodeAction;
import org.bsc.langgraph4j.agentexecutor.Agent;
import org.bsc.langgraph4j.agentexecutor.state.AgentAction;
import org.bsc.langgraph4j.agentexecutor.AgentExecutor;
import org.bsc.langgraph4j.agentexecutor.state.AgentOutcome;
import org.bsc.langgraph4j.agentexecutor.state.IntermediateStep;
import org.bsc.langgraph4j.langchain4j.tool.ToolNode;

import java.util.Collections;
import java.util.List;
import java.util.Map;


/**
 * The ExecuteTools class implements the NodeAction interface for handling 
 * actions related to executing tools within an agent's context.
 */
@Slf4j
public class ExecuteTools implements NodeAction<AgentExecutor.State> {

    /**
     * The agent associated with this execution tool.
     */
    final Agent agent;

    /**
     * The tool node that will be executed.
     */
    final ToolNode toolNode;

    /**
     * Constructs an ExecuteTools instance with the specified agent and tool node.
     *
     * @param agent the agent to be associated with this execution tool, must not be null
     * @param toolNode the tool node to be executed, must not be null
     */
    public ExecuteTools(@NonNull Agent agent, @NonNull ToolNode toolNode) {
        this.agent = agent;
        this.toolNode = toolNode;
    }

    /**
     * Applies the tool execution logic based on the provided agent state.
     *
     * @param state the current state of the agent executor
     * @return a map containing the intermediate steps of the execution
     * @throws IllegalArgumentException if no agent outcome is provided
     * @throws IllegalStateException if no action or tool is found for execution
     */
    @Override
    public Map<String,Object> apply(AgentExecutor.State state )  {
        log.trace( "executeTools" );

        AgentOutcome agentOutcome = state.agentOutcome().orElseThrow(() -> new IllegalArgumentException("no agentOutcome provided!"));

        List<IntermediateStep> intermediateSteps = new java.util.ArrayList<>();
        for (AgentAction action: agentOutcome.getActions()) {
           ToolExecutionRequest request = action.getToolExecutionRequest();
            String result = toolNode.execute(request)
                    .map( ToolExecutionResultMessage::text )
                    .orElseThrow(() -> new IllegalStateException("no tool found for: " + request.name()));
            if (result.startsWith("cannot resolve")) {
                int errMsgBegin = result.indexOf(':');
                int errMsgEnd = result.indexOf(';');
                String errMsg = result.substring(errMsgBegin, errMsgEnd);
                String sql = request.arguments();
                String sparkErrMsg = String.format("Spark SQL syntax error:\noriginal sql: %s\nerror message: %s\n", sql, errMsg);
                intermediateSteps.add(new IntermediateStep(action, sparkErrMsg));
            } else {
                intermediateSteps.add(new IntermediateStep(action, result));
            }
        }

        return Collections.singletonMap("intermediate_steps", intermediateSteps);
    }

}
