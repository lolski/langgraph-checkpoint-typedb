"""End-to-end example: a LangGraph chatbot whose state persists across invocations.

Run prerequisites:
  - A TypeDB 3.8+ server reachable at TYPEDB_ADDR (default localhost:1729)
  - ANTHROPIC_API_KEY exported in your environment

Two invocations on the same `thread_id` share message history because the
checkpointer reloads prior state from TypeDB on the second call.
"""

from __future__ import annotations

import os
from typing import Annotated, TypedDict

from langchain_anthropic import ChatAnthropic
from langchain_core.messages import HumanMessage
from langgraph.graph import START, StateGraph
from langgraph.graph.message import add_messages
from typedb.driver import (
    Credentials,
    DriverOptions,
    DriverTlsConfig,
    TypeDB,
)

from langgraph_checkpoint_typedb import TypeDBSaver

DATABASE = "langgraph_checkpoints"
THREAD_ID = "demo-thread-1"


class State(TypedDict):
    messages: Annotated[list, add_messages]


def build_saver() -> TypeDBSaver:
    addr = os.environ.get("TYPEDB_ADDR", "localhost:1729")
    driver = TypeDB.driver(
        addr,
        Credentials("admin", "password"),
        DriverOptions(DriverTlsConfig.disabled()),
    )
    saver = TypeDBSaver(driver, database=DATABASE)
    saver.ensure_database()
    saver.ensure_schema()
    return saver


def main() -> None:
    saver = build_saver()
    llm = ChatAnthropic(model="claude-sonnet-4-6")

    def chat(state: State) -> State:
        return {"messages": [llm.invoke(state["messages"])]}

    graph = (
        StateGraph(State)
        .add_node("chat", chat)
        .add_edge(START, "chat")
        .compile(checkpointer=saver)
    )

    config = {"configurable": {"thread_id": THREAD_ID}}

    prompts = [
        "My name is Ganesh. Remember it.",
        "What's my name?",
    ]
    for prompt in prompts:
        print(f"\n--- user: {prompt}")
        result = graph.invoke({"messages": [HumanMessage(content=prompt)]}, config)
        print(result["messages"][-1].content)


if __name__ == "__main__":
    main()
