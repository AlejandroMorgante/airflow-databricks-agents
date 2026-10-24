"""Present remote agent responses consistently across the three integration paths."""

from __future__ import annotations

import json


def inspect_response(response: dict) -> dict:
    """Read a full invocation envelope from the operator or direct hook."""
    output = response["output"]
    answer = output.get("message", output) if isinstance(output, dict) else output
    if not isinstance(answer, str):
        answer = json.dumps(answer, ensure_ascii=False)
    return {"final_answer": answer, "databricks_responses": [output]}


def inspect_agent_response(response: str, message_history: str | list[dict]) -> dict:
    """Read the caller's answer and every remote response from its transcript."""
    # AgentOperator emits the transcript as JSON text in its message_history XCom.
    if isinstance(message_history, str):
        message_history = json.loads(message_history)
    databricks_responses = []
    for message in message_history:
        for part in message.get("parts", []):
            if (
                part.get("part_kind") == "tool-return"
                and part.get("tool_name") == "ask_databricks_agent"
            ):
                content = part.get("content")
                if isinstance(content, str):
                    try:
                        content = json.loads(content)
                    except json.JSONDecodeError:
                        pass
                databricks_responses.append(content)
    return {
        "final_answer": response,
        "databricks_responses": databricks_responses,
    }
