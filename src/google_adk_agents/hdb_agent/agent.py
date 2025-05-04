import os
import sys

from google.adk.agents import Agent, SequentialAgent
from google.adk.tools import google_search

sys.path.append(os.path.dirname(__file__))
from sub_agents.hdb_data_query_agent import hdb_data_query_agent


# root_agent = Agent(
#     name="hdb_agent",
#     description="[HDB Agent] An agent that can answer questions about HDB data.",
#     model="gemini-2.0-flash",
#     instruction="""
#     You are a helpful assistant that can answer questions about HDB data.
    
#     You can use the following tools to answer questions:
#     - google_search: to search the web for information
#     """,
#     tools=[google_search],
# )

root_agent = SequentialAgent(
    name="hdb_agent",
    description="[HDB Agent] An agent that can answer questions about HDB data.",
    sub_agents=[
        hdb_data_query_agent,
    ],
)


