from mcp.server.fastmcp import FastMCP
import pandas as pd
from thefuzz import fuzz, process
import matplotlib.pyplot as plt

# Create the MCP instance
mcp = FastMCP("Demo")

# Import and register all agents
from agents import (
    register_steward, register_emma, register_oskar, 
    register_mathias, register_james, register_gina, 
    register_mike
)

# Register all agents with the MCP instance
register_steward(mcp)
register_emma(mcp)
register_oskar(mcp)
register_mathias(mcp)
register_james(mcp)
register_gina(mcp)
register_mike(mcp)

# No duplicate function definitions here - they're now in the agent modules
