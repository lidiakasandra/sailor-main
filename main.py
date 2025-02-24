from fastapi import FastAPI, WebSocket
from fastapi.websockets import WebSocketState
import os
import websockets

app = FastAPI()

@app.get("/")
def read_root():
    return {"Hello World!"}

@app.websocket("/ws")

async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    while True:
        if websocket.client_state == WebSocketState.CONNECTED:
            data = await websocket.receive_text()
            print(f"Received data: {data}")
            task_id = await delegate_task_to_runner(data)
            await websocket.send_text(f"Task {task_id} started")
            # Listen for updates from runner
            async for update in get_task_updates(task_id):
                await websocket.send_text(update)

async def delegate_task_to_runner(task_data):
    runner_uri = os.getenv("RUNNER_URI", "localhost:8002")
    uri = f"ws://{runner_uri}/ws"
    async with websockets.connect(uri) as websocket:
        await websocket.send(task_data)
        task_id = await websocket.recv()
        print(f"Received data")
        return task_id

async def check_task_updates_ready(task_id):
    # Check if the task update WebSocket is ready to connect on the runner
    runner_uri = os.getenv("RUNNER_URI", "localhost:8002")
    uri = f"ws://{runner_uri}/ws/{task_id}/updates"
    try:
        async with websockets.connect(uri) as websocket:
            return websocket  # Return WebSocket if successful
    except websockets.exceptions.ConnectionClosedError:
        # WebSocket not available yet, task not started or update ws not created
        return None
    except Exception as e:
        print(f"Unexpected error while checking WebSocket: {e}")
        return None

async def get_task_updates(task_id):

    websocket = await check_task_updates_ready(task_id)
    
    # If the WebSocket is ready, start receiving updates
    if websocket:
        try:
            print(f"Connected to task {task_id} updates WebSocket.")
            while True:
                update = await websocket.recv()
                yield update
        except websockets.exceptions.ConnectionClosedError as e:
            print(f"Error: WebSocket connection closed unexpectedly: {e}")
        except Exception as e:
            print(f"Unexpected error: {e}")
    else:
        print(f"Task {task_id} updates WebSocket not ready yet.")

if __name__ == "__main__":
    import uvicorn
    host = str(os.getenv("HOST", "localhost"))
    port = int(os.getenv("PORT", "8001"))
    uvicorn.run(app, host=host, port=port)