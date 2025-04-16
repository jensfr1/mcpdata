from mcp.server.fastmcp import FastMCP
from fastapi import FastAPI, Request
from fastapi.responses import StreamingResponse
import asyncio
import json
import uvicorn
from typing import AsyncGenerator
import os
from datetime import datetime

# Importiere die Agenten-Register-Funktionen
from agents import steward, emma, oskar, mathias, james, gina, mike

# Erstelle den MCP-Server
mcp = FastMCP()

# Registriere alle Agenten
steward.register(mcp)
emma.register(mcp)
oskar.register(mcp)
mathias.register(mcp)
james.register(mcp)
gina.register(mcp)
mike.register(mcp)

# Erstelle eine FastAPI-App
app = FastAPI(title="MCP Server mit SSE-Unterstützung")

# Anstatt zu versuchen, die MCP-App zu mounten, erstellen wir eigene Endpunkte,
# die die MCP-Tools aufrufen

# Erstelle einen Event-Stream für SSE
event_queue = asyncio.Queue()

# Funktion zum Senden von Ereignissen an alle verbundenen Clients
async def send_event(event_type: str, data: dict):
    await event_queue.put({"event": event_type, "data": data})

# SSE-Endpunkt für n8n
@app.get("/events")
async def events(request: Request) -> StreamingResponse:
    async def event_generator() -> AsyncGenerator[str, None]:
        # Sende ein initiales Ereignis
        timestamp = datetime.now().isoformat()
        yield f"event: connected\ndata: {{\"status\": \"connected\", \"timestamp\": \"{timestamp}\"}}\n\n"
        
        # Erstelle eine eigene Queue für diesen Client
        client_queue = asyncio.Queue()
        
        # Task zum Weiterleiten von Ereignissen aus der Haupt-Queue an die Client-Queue
        async def forward_events():
            try:
                while True:
                    event = await event_queue.get()
                    await client_queue.put(event)
            except asyncio.CancelledError:
                pass
        
        # Starte die Weiterleitung im Hintergrund
        task = asyncio.create_task(forward_events())
        
        try:
            while True:
                # Warte auf Ereignisse in der Client-Queue
                event = await client_queue.get()
                # Formatiere das Ereignis als SSE
                yield f"event: {event['event']}\ndata: {json.dumps(event['data'])}\n\n"
                
                # Prüfe, ob der Client noch verbunden ist
                if await request.is_disconnected():
                    break
        finally:
            # Bereinige, wenn der Client die Verbindung trennt
            task.cancel()
    
    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache", 
            "Connection": "keep-alive",
            "Access-Control-Allow-Origin": "*",  # CORS für n8n
            "Access-Control-Allow-Methods": "GET",
            "Access-Control-Allow-Headers": "Content-Type"
        }
    )

# Füge einen Hook hinzu, um Ereignisse zu senden, wenn Tools ausgeführt werden
# Wir können keinen direkten Hook verwenden, da wir die Tools manuell aufrufen
# Stattdessen senden wir Ereignisse nach jedem Tool-Aufruf

# Füge einen Endpunkt hinzu, um Nachrichten an den Server zu senden (für n8n)
@app.post("/messages")
async def post_message(message: dict):
    """
    Endpunkt zum Senden von Nachrichten an den Server.
    Erwartet ein JSON-Objekt mit:
    {
        "tool": "tool_name",
        "args": { ... Argumente für das Tool ... }
    }
    """
    try:
        tool_name = message.get("tool")
        args = message.get("args", {})
        
        if not tool_name:
            return {"error": "Tool name is required"}
        
        # Finde das Tool
        tool = None
        for t in mcp.tools:
            if t.name == tool_name:
                tool = t
                break
        
        if not tool:
            return {"error": f"Tool '{tool_name}' not found"}
        
        # Führe das Tool aus
        result = tool.function(**args)
        
        # Sende ein Ereignis über SSE
        await send_event("message_processed", {
            "tool": tool_name,
            "args": {k: v for k, v in args.items() if k != "context"},
            "result": result,
            "timestamp": datetime.now().isoformat()
        })
        
        return {"status": "success", "result": result}
    
    except Exception as e:
        error_message = str(e)
        # Sende ein Fehler-Ereignis über SSE
        await send_event("error", {
            "message": error_message,
            "timestamp": datetime.now().isoformat()
        })
        return {"error": error_message}

# Füge einen Endpunkt hinzu, um verfügbare Tools aufzulisten
@app.get("/tools")
async def list_tools():
    """Listet alle verfügbaren Tools auf"""
    tools = []
    for tool in mcp.tools:
        tools.append({
            "name": tool.name,
            "description": tool.description,
            "parameters": [
                {"name": param.name, "type": str(param.annotation), "default": param.default}
                for param in tool.parameters
            ]
        })
    return {"tools": tools}

# Füge einen Endpunkt hinzu, der die Standard-MCP-API-Endpunkte nachbildet
@app.post("/api/v1/tools/{agent_name}/{tool_name}/run")
async def run_tool(agent_name: str, tool_name: str, args: dict):
    """
    Endpunkt zum Ausführen eines Tools über den Standard-MCP-API-Pfad.
    """
    try:
        # Finde das Tool
        tool = None
        for t in mcp.tools:
            # Extrahiere den Agentennamen und Tool-Namen aus dem vollständigen Namen
            full_name = t.name
            if agent_name.lower() in full_name.lower() and tool_name.lower() in full_name.lower():
                tool = t
                break
        
        if not tool:
            return {"error": f"Tool for agent '{agent_name}' and name '{tool_name}' not found"}
        
        # Führe das Tool aus
        result = tool.function(**args)
        
        # Sende ein Ereignis über SSE
        await send_event("tool_run", {
            "agent": agent_name,
            "tool": tool_name,
            "args": {k: v for k, v in args.items() if k != "context"},
            "result": result,
            "timestamp": datetime.now().isoformat()
        })
        
        return result
    
    except Exception as e:
        error_message = str(e)
        # Sende ein Fehler-Ereignis über SSE
        await send_event("error", {
            "message": error_message,
            "agent": agent_name,
            "tool": tool_name,
            "timestamp": datetime.now().isoformat()
        })
        return {"error": error_message}

# Füge einen Root-Endpunkt hinzu, der Informationen über den Server bereitstellt
@app.get("/")
async def root():
    """Gibt Informationen über den Server zurück"""
    return {
        "name": "MCP Server mit SSE-Unterstützung",
        "version": "1.0.0",
        "endpoints": {
            "sse": "/events",
            "messages": "/messages",
            "tools": "/tools",
            "api": "/api/v1/tools/{agent_name}/{tool_name}/run"
        },
        "timestamp": datetime.now().isoformat()
    }

# Starte den Server, wenn diese Datei direkt ausgeführt wird
if __name__ == "__main__":
    # Hole den Port aus der Umgebungsvariable oder verwende 8000 als Standard
    port = int(os.environ.get("PORT", 8000))
    
    print(f"🚀 Starting MCP Server with SSE support on port {port}")
    print(f"📡 SSE events available at: /events")
    print(f"📨 Messages endpoint: /messages")
    print(f"🔧 Tools list: /tools")
    print(f"🔍 MCP API: /api/v1/tools/{agent_name}/{tool_name}/run")
    
    uvicorn.run(app, host="0.0.0.0", port=port) 