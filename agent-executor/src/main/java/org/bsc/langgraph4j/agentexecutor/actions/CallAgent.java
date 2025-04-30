package org.bsc.langgraph4j.agentexecutor.actions;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import dev.langchain4j.agent.tool.ToolExecutionRequest;
import dev.langchain4j.data.message.AiMessage;
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

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

/**
 * The CallAgent class implements the NodeAction interface for handling 
 * actions related to an AgentExecutor's state.
 */
@Slf4j
public class CallAgent implements NodeAction<AgentExecutor.State> {

    final Agent agent;

    private HashSet<ToolExecutionRequest> hashSet = new HashSet<>();

    private String previousObserv = null;

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
        ObjectMapper mapper = new ObjectMapper();
        List<String> pretties = content.toolExecutionRequests().stream().map(req -> {
                    String pretty;
                    try {
                        JsonNode root = mapper.readTree(req.toString().substring(20)); // remove ToolExecutionRequest
                        pretty = root.toPrettyString();
                    } catch (Exception e) {
                        pretty = e.getMessage();
                    }
                    return pretty;
                }
        ).collect(Collectors.toList());
        System.out.println("LLM response:\n" + content.text() + "\n" + String.join("\n", pretties));

        String result = content.text();
        FinishReason finishReason = response.finishReason();
        if (response.content().hasToolExecutionRequests()) {
            boolean repeatReq = content.toolExecutionRequests().stream().map(req -> hashSet.contains(req)).reduce((a, b) -> a && b).orElse(true);
            if (repeatReq) {
                finishReason = FinishReason.STOP;
                result = this.previousObserv;
            } else {
                finishReason = FinishReason.TOOL_EXECUTION;
            }
        } else if (response.content().text().startsWith("<|python_start|>")) {
            finishReason = FinishReason.TOOL_EXECUTION;
        }

        if( finishReason == FinishReason.STOP ) {
            AgentFinish finish = new AgentFinish(Collections.singletonMap("returnValues", result), result);
            return Collections.singletonMap("agent_outcome", new AgentOutcome(Collections.emptyList(), finish));
        }

        if ( finishReason == FinishReason.TOOL_EXECUTION ) {

            List<ToolExecutionRequest> toolExecutionRequests = Collections.EMPTY_LIST;
            if (response.content().hasToolExecutionRequests()) {
                toolExecutionRequests = response.content().toolExecutionRequests();
            } else if (response.content().text().startsWith("<|python_start|>")) {
                toolExecutionRequests = toToolExecutionRequests(response.content().text());
            }
            List<AgentAction> actions = new ArrayList<>();
            for (ToolExecutionRequest request: toolExecutionRequests) {
                this.hashSet.add(request);

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

    private List<ToolExecutionRequest> toToolExecutionRequests(String text) {
        // Extract JSON between markers
        String prefix = "<|python_start|>";
        String suffix = "<|python_end|>";
        int start = text.indexOf(prefix) + prefix.length();
        int end = text.indexOf(suffix);
        String jsonString = text.substring(start, end).trim();

        List<ToolExecutionRequest> requests = new ArrayList<>();
        // Parse JSON
        ObjectMapper mapper = new ObjectMapper();
        try {
            JsonNode root = mapper.readTree(jsonString);
            String name = root.get("name").asText();
            JsonNode parameters = root.get("parameters");
            convertBracketedStringsToList(parameters, mapper);
            // Return ToolExecutionRequest
            requests.add(ToolExecutionRequest.builder().id("call_" + UUID.randomUUID())
                    .name(name)
                    .arguments(parameters.toString())
                    .build());
        } catch (Exception e) {
           System.out.println(e.getMessage());
        }

        return requests;
    }

    public void convertBracketedStringsToList(JsonNode node, ObjectMapper mapper) {
        Iterator<Map.Entry<String, JsonNode>> fields = node.fields();
        while (fields.hasNext()) {
            Map.Entry<String, JsonNode> entry = fields.next();
            JsonNode value = entry.getValue();

            if (value.isTextual() && value.asText().trim().startsWith("[")) {
                String text = value.asText().trim();
                try {
                    // Try parsing the string into an ArrayNode
                    JsonNode parsedNode = mapper.readTree(text);
                    if (parsedNode.isArray()) {
                        ((ObjectNode)node).set(entry.getKey(), parsedNode);
                    }
                } catch (IOException e) {
                    // Ignore or log parsing errors, value is not a valid JSON array
                    System.err.println("Failed to parse field '" + entry.getKey() + "' as JSON array: " + text);
                }
            }
        }
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
            if (!intermediateSteps.isEmpty()) {
                this.previousObserv = intermediateSteps.get(intermediateSteps.size() - 1).getObservation();
            }
            LLMStreamingGenerator generator = LLMStreamingGenerator.<AiMessage, AgentExecutor.State>builder()
                    .mapResult(this::mapResult)
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
            if (!intermediateSteps.isEmpty()) {
                this.previousObserv = intermediateSteps.get(intermediateSteps.size() - 1).getObservation();
            }

            return mapResult(response);
        }

    }

}
