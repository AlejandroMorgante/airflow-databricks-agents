# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
"""LLM-backed agent served by DurableAgentServer for the Airflow examples."""

from __future__ import annotations

import asyncio
import os
from typing import Any

import uvicorn
from databricks.sdk.service.serving import ChatMessage, ChatMessageRole
from databricks_agentkit import DurableAgentServer, InvocationContext, workspace_client

app = DurableAgentServer()
MODEL_ENDPOINT = os.environ.get("DATABRICKS_MODEL_ENDPOINT") or "databricks-gpt-oss-20b"
SYSTEM_PROMPT = (
    "You are a helpful demo agent running on Databricks Agent Runtime, invoked by Apache Airflow. "
    "Answer the user's request briefly and accurately. "
    "When asked to introduce yourself, explain where you run and that you use a language model. "
    "You have no external tools and cannot inspect the user's data or run jobs."
)


def query_model(input: Any) -> dict[str, Any]:
    """Use the app's SDK-managed OAuth credentials, or a local CLI profile."""
    if not isinstance(input, dict) or not isinstance(input.get("messages"), list):
        raise ValueError("input must contain a messages list")
    if not input["messages"]:
        raise ValueError("input.messages must not be empty")
    messages = [ChatMessage(role=ChatMessageRole.SYSTEM, content=SYSTEM_PROMPT)]
    for message in input["messages"]:
        if (
            not isinstance(message, dict)
            or message.get("role") not in {"user", "assistant"}
            or not isinstance(message.get("content"), str)
            or not message["content"].strip()
        ):
            raise ValueError(
                "Each message needs a user/assistant role and nonempty text content"
            )
        messages.append(
            ChatMessage(
                role=ChatMessageRole(message["role"]), content=message["content"]
            )
        )
    response = workspace_client().serving_endpoints.query(
        name=MODEL_ENDPOINT,
        messages=messages,
        max_tokens=2048,
        temperature=0.2,
    )
    if not response.choices or not response.choices[0].message:
        raise RuntimeError("The model returned no assistant message")
    text = response.choices[0].message.content
    if not isinstance(text, str) or not text.strip():
        raise RuntimeError("The model returned no text; check its output token limit")
    return {
        "message": text,
        "model": MODEL_ENDPOINT,
        "usage": response.usage.as_dict() if response.usage else {},
    }


@app.invoke
async def invoke(input: Any, context: InvocationContext) -> dict[str, Any]:
    # The SDK's HTTP call is synchronous; keep the server event loop available
    # for status requests while the model is generating the answer.
    response = await asyncio.to_thread(query_model, input)
    return {**response, "session_id": context.session_id}


if __name__ == "__main__":
    uvicorn.run(
        app, host="0.0.0.0", port=int(os.environ.get("DATABRICKS_APP_PORT", "8000"))
    )
