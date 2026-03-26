"""Agent wrapper for the query system."""

import logging

from langchain_core.messages import HumanMessage, AIMessage

from .graph import graph
from .config import MAX_CONVERSATION_HISTORY

logger = logging.getLogger(__name__)


class FinancialQueryAgent:
    """Stateful agent that maintains conversation history."""

    def __init__(self):
        self.graph = graph
        self.messages: list = []

    def query(self, user_question: str) -> str:
        """Send a question and get an answer."""
        self.messages.append(HumanMessage(content=user_question))

        # Trim history to avoid context overflow
        if len(self.messages) > MAX_CONVERSATION_HISTORY * 2:
            self.messages = self.messages[-(MAX_CONVERSATION_HISTORY * 2):]

        try:
            result = self.graph.invoke({"messages": self.messages})
            response_messages = result["messages"]

            # Update conversation history with all new messages
            self.messages = response_messages

            # Extract final AI response text
            for msg in reversed(response_messages):
                if isinstance(msg, AIMessage) and msg.content:
                    return msg.content

            return "I couldn't generate a response. Please try rephrasing your question."

        except Exception as e:
            logger.error(f"Query failed: {e}")
            return f"An error occurred: {str(e)}. Please try again."

    def reset(self):
        """Clear conversation history."""
        self.messages = []
