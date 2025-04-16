from mcp.server.fastmcp import FastMCP

# Create a simple MCP instance
mcp = FastMCP("Test Server")

# Define a simple tool
@mcp.tool(name="Simple Test Tool")
def test_tool(message: str) -> dict:
    """
    A simple test tool to verify MCP is working
    
    Args:
        message: A test message
        
    Returns:
        A dictionary with the echoed message
    """
    return {
        "status": "success",
        "message": f"Received: {message}"
    }

print("Test server started with one simple tool") 