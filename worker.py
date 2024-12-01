import argparse
import asyncio
import aiohttp
import re
from datetime import datetime
from typing import Dict
from aiohttp import web

class Worker:
    """Processes log chunks and reports results."""

    def __init__(self, worker_id: str, port: int, coordinator_url: str):
        self.worker_id = worker_id
        self.coordinator_url = coordinator_url
        self.port = port

    async def start(self):
        """Start the worker server."""
        app = web.Application()
        app.router.add_post("/process_chunk", self.process_chunk_handler)
        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, 'localhost', self.port)
        await site.start()
        print(f"Worker {self.worker_id} running on port {self.port}...")

        # Register worker with Coordinator
        await self.register_with_coordinator()

        # Periodically send heartbeat
        asyncio.create_task(self.send_heartbeat())

        # Keep the server running
        while True:
            await asyncio.sleep(10)

    async def register_with_coordinator(self):
        """Register worker with the coordinator."""
        async with aiohttp.ClientSession() as session:
            try:
                response = await session.post(
                    f"{self.coordinator_url}/register",
                    json={"worker_id": self.worker_id, "port": self.port},
                )
                print(f"Registration response: {response.status}")  
                if response.status == 200:
                    print(f"Worker {self.worker_id} registered successfully.")
                else:
                    print(f"Worker {self.worker_id} failed to register.")
            except Exception as e:
                print(f"Error registering worker {self.worker_id}: {e}")

    async def send_heartbeat(self):
        """Send periodic heartbeat to the coordinator."""
        while True:
            async with aiohttp.ClientSession() as session:
                try:
                    response = await session.post(
                        f"{self.coordinator_url}/health",
                        json={"worker_id": self.worker_id},
                    )
                    if response.status == 200:
                        print(f"")
                    else:
                        print(f"Worker {self.worker_id} heartbeat failed.")
                except Exception as e:
                    print(f"Error sending heartbeat for worker {self.worker_id}: {e}")
            await asyncio.sleep(5) 

    async def process_chunk_handler(self, request):
        """Handle file chunk processing request."""
        data = await request.json()
        print(f"Received task: {data}")  
        filepath = data["filepath"]
        start = data["start"]
        size = data["size"]

        metrics = await self.process_chunk(filepath, start, size)
        await self.send_metrics(metrics)

        return web.Response(text="Chunk processed successfully")

    async def process_chunk(self, filepath: str, start: int, size: int) -> Dict:
        """Process a chunk of the log file and return metrics."""
        metrics = {
            "error_count": 0,
            "total_response_time": 0,
            "request_count": 0,
        }

        with open(filepath, 'r') as file:
            file.seek(start)
            lines = file.read(size).splitlines()

        for line in lines:
            # Match log entry pattern: "<timestamp> <level> <message>"
            match = re.match(r"(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{3}) (\w+) (.+)", line)
            if match:
                timestamp_str, level, message = match.groups()
                timestamp = datetime.strptime(timestamp_str, "%Y-%m-%d %H:%M:%S.%f")
                
                # Extract response time if present
                if "Request processed in" in message:
                    response_time = int(message.split("processed in ")[-1].replace("ms", ""))
                    metrics["total_response_time"] += response_time
                    metrics["request_count"] += 1

                # Count errors
                if level == "ERROR":
                    metrics["error_count"] += 1

        # Calculate average response time
        metrics["avg_response_time"] = (
            metrics["total_response_time"] / metrics["request_count"]
            if metrics["request_count"] > 0
            else 0
        )

        return metrics

    async def send_metrics(self, metrics: Dict):
        """Send processed metrics back to the coordinator."""
        async with aiohttp.ClientSession() as session:
            try:
                await session.post(
                    f"{self.coordinator_url}/metrics",
                    json={"worker_id": self.worker_id, "metrics": metrics},
                )
                print(f"Worker {self.worker_id}: Sent metrics to coordinator.")
            except Exception as e:
                print(f"Worker {self.worker_id}: Failed to send metrics - {e}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Log Analyzer Worker")
    parser.add_argument("--port", type=int, default=8001, help="Worker port")
    parser.add_argument("--id", type=str, default="worker1", help="Worker ID")
    parser.add_argument("--coordinator", type=str, default="http://localhost:8000", help="Coordinator URL")
    args = parser.parse_args()

    worker = Worker(worker_id=args.id, port=args.port, coordinator_url=args.coordinator)
    asyncio.run(worker.start())
