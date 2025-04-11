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

import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;


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
    public Map<String, Object> apply(AgentExecutor.State state) {
        log.trace("executeTools");

        AgentOutcome agentOutcome = state.agentOutcome()
                .orElseThrow(() -> new IllegalArgumentException("no agentOutcome provided!"));

        List<AgentAction> actions = agentOutcome.getActions();
        int numThreads = 10;
        ExecutorService executor = Executors.newFixedThreadPool(numThreads);

        try {
            List<Callable<IntermediateStep>> tasks = actions.stream()
                    .map(action -> (Callable<IntermediateStep>) () -> {
                        ToolExecutionRequest request = action.getToolExecutionRequest();
                        String result = toolNode.execute(request)
                                .map(ToolExecutionResultMessage::text)
                                .orElseThrow(() -> new IllegalStateException("no tool found for: " + request.name()));
                        return new IntermediateStep(action, result);
                    })
                    .collect(Collectors.toList());

            List<Future<IntermediateStep>> futures = executor.invokeAll(tasks);
            List<IntermediateStep> intermediateSteps = new ArrayList<>(futures.size());

            for (Future<IntermediateStep> future : futures) {
                intermediateSteps.add(future.get());
            }

            return Collections.singletonMap("intermediate_steps", intermediateSteps);
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException("Tool execution failed", e);
        } finally {
            executor.shutdown();
        }
    }

}
