"""Guardrails to block profanity and code/architecture disclosure attempts."""

import re

# Profanity word list (Portuguese + English common terms)
PROFANITY_WORDS = {
    # Portuguese
    "porra", "caralho", "puta", "merda", "foda", "fodase", "foda-se",
    "cacete", "viado", "arrombado", "desgraça", "desgraçado", "filhadaputa",
    "filho da puta", "vai se foder", "vai tomar no cu", "cu", "buceta",
    "piranha", "vadia", "corno", "otario", "otário", "babaca", "imbecil",
    # English
    "fuck", "shit", "bitch", "asshole", "bastard", "damn", "crap",
    "dick", "pussy", "motherfucker", "bullshit", "wtf", "stfu",
}

# Patterns that attempt to extract code, architecture, or system info
CODE_DISCLOSURE_PATTERNS = [
    r"(show|reveal|display|print|give).{0,20}(code|source|script|implementation|arquivo|código|codigo)",
    r"(show|reveal|display|print|give).{0,20}(system\s*prompt|instruc|prompt\s*do\s*sistema)",
    r"(what|qual|como).{0,20}(your|seu|sua).{0,20}(architecture|arquitetura|stack|tech|framework)",
    r"(what|which|qual).{0,30}(framework|stack|tech|technology|architecture).{0,30}(use|used|utilize|built|run)",
    r"(what|qual).{0,20}(system\s*prompt|instrução|instrucao|prompt\s*interno)",
    r"(ignore|bypass|skip|forget|esqueça|esqueca|esquecer).{0,20}(instructions|instruc|rules|regras|prompt|guardrail)",
    r"(repeat|repita|print|mostre).{0,20}(system|sistema|instructions|instruc|above|acima)",
    r"(how|como).{0,30}(built|construi|implement|feito|developed|desenvolvid)",
    r"(what|qual|which).{0,20}(database|banco|db|duckdb|modelo|model|llm).{0,20}(use|usa|using|utiliz)",
    r"(tell|diga|fale).{0,20}(about|sobre).{0,20}(your|seu|sua).{0,20}(code|código|codigo|internal|intern)",
    r"(docker|container|dockerfile|compose|langraph|langgraph|groq|streamlit)",
    r"(sql\s*inject|drop\s*table|delete\s*from|update\s+\w+\s+set|alter\s+table|create\s+table)",
]

BLOCKED_PROFANITY_RESPONSE = (
    "I'm sorry, but I can't process messages with inappropriate language. "
    "Please rephrase your question in a respectful manner, and I'll be happy to help "
    "with your financial data queries."
)

BLOCKED_DISCLOSURE_RESPONSE = (
    "I'm a financial data analysis assistant. I can only help with questions about "
    "the financial performance of Dipped Products PLC (DIPD) and Richard Pieris Exports PLC (REXP). "
    "I cannot provide information about my internal implementation or architecture. "
    "Please ask me a question about the companies' financial data."
)


def check_profanity(text: str) -> bool:
    """Return True if the text contains profanity."""
    text_lower = text.lower()
    # Check individual words
    words = re.findall(r'\b\w+\b', text_lower)
    for word in words:
        if word in PROFANITY_WORDS:
            return True
    # Check multi-word phrases
    for phrase in PROFANITY_WORDS:
        if " " in phrase and phrase in text_lower:
            return True
    return False


def check_code_disclosure(text: str) -> bool:
    """Return True if the text attempts to extract code/architecture info."""
    text_lower = text.lower()
    for pattern in CODE_DISCLOSURE_PATTERNS:
        if re.search(pattern, text_lower):
            return True
    return False


def run_guardrails(user_message: str) -> str | None:
    """Run all guardrails. Returns a block message if triggered, None if safe."""
    if check_profanity(user_message):
        return BLOCKED_PROFANITY_RESPONSE
    if check_code_disclosure(user_message):
        return BLOCKED_DISCLOSURE_RESPONSE
    return None
