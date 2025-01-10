package org.bsc.langgraph4j.agentexecutor.state;

import java.util.Map;

/**
 * Represents the result of an agent's finish operation.
 * 
 * @param returnValues a map containing the return values from the operation
 * @param log a string representing the log of the operation
 */
@lombok.Data
public class AgentFinish {
    final Map<String, Object> returnValues;
    final String log;
}
