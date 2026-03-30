"""Agent wrapper for the query system."""

import logging

from langchain_core.messages import HumanMessage, AIMessage, ToolMessage

from .graph import graph
from .config import MAX_CONVERSATION_HISTORY

logger = logging.getLogger(__name__)


class FinancialQueryAgent:
    """Stateful agent that maintains conversation history."""

    def __init__(self):
        self.graph = graph
        self.history: list = []  # Only user + final AI messages (no tool internals)

    def query(self, user_question: str) -> str:
        """Send a question and get an answer."""
        self.history.append(HumanMessage(content=user_question))

        # Trim history to avoid context overflow
        if len(self.history) > MAX_CONVERSATION_HISTORY * 2:
            self.history = self.history[-(MAX_CONVERSATION_HISTORY * 2):]

        try:
            # Send only clean history (user + final AI) to the graph.
            # The graph will handle tool calls internally within a single invocation.
            result = self.graph.invoke({"messages": list(self.history)})
            response_messages = result["messages"]

            # Extract final AI response text
            response_text = None
            for msg in reversed(response_messages):
                if isinstance(msg, AIMessage) and msg.content and not msg.tool_calls:
                    response_text = msg.content
                    break

            if not response_text:
                response_text = "I couldn't generate a response. Please try rephrasing your question."

            # Store only the final answer in history (not intermediate tool messages)
            self.history.append(AIMessage(content=response_text))
            return response_text

        except Exception as e:
            logger.error(f"Query failed: {e}")
            error_msg = "An error occurred while processing your question. Please try again."
            # Keep history consistent — remove the user message if we can't respond
            if self.history and isinstance(self.history[-1], HumanMessage):
                self.history.pop()
            return error_msg

    def reset(self):
        """Clear conversation history."""
        self.history = []
