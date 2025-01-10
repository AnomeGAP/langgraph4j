package org.bsc.langgraph4j.agentexecutor.state;

/**
 * Represents an intermediate step in a process, encapsulating an action taken by an agent
 * and the corresponding observation made.
 */
@lombok.Data
public class IntermediateStep {
    final AgentAction action;
    final String observation;
}
