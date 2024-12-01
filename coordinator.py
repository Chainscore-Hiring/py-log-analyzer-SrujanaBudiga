import argparse
import os
import asyncio
from aiohttp import web, ClientSession
from analyzer import Analyzer

class Coordinator:
    """Manages workers and aggregates results."""

    def __init__(self, port: int):
        self.port = port
        self.workers = {}  # Dictionary to store worker information
        self.results = {}  # Dictionary to store metrics from workers
        self.analyzer = Analyzer()  # Initialize the Analyzer

    async def start(self):
        """Start the coordinator server."""
        app = web.Application()
        app.router.add_post("/metrics", self.metrics_handler)
        app.router.add_post("/health", self.health_handler)
        app.router.add_post("/register", self.register_worker)
        app.router.add_get("/metrics", self.get_metrics_handler)  
        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, 'localhost', self.port)
        await site.start()
        print(f"Coordinator running on port {self.port}...")

        # Periodically check worker health
        asyncio.create_task(self.monitor_workers())

        while True:
            await asyncio.sleep(10)

    async def register_worker(self, request):
        """Register a new worker."""
        data = await request.json()
        worker_id = data["worker_id"]
        worker_port = data["port"]
        self.workers[worker_id] = {"port": worker_port, "healthy": True}
        print(f"Worker {worker_id} registered on port {worker_port}.")
        return web.Response(text="Worker registered")

    async def health_handler(self, request):
        """Receive heartbeat from workers."""
        data = await request.json()
        worker_id = data["worker_id"]

        if worker_id in self.workers:
            self.workers[worker_id]["healthy"] = True
            print(f"")
        else:
            print(f"")

        return web.Response(text="Health received")


    async def metrics_handler(self, request):
        """Receive metrics from workers."""
        data = await request.json()
        worker_id = data["worker_id"]
        worker_metrics = data["metrics"]

        self.results[worker_id] = data["metrics"]
        print(f"Received metrics from {worker_id}: {data['metrics']}")

        # Update the analyzer with new metrics
        self.analyzer.update_metrics(worker_metrics)

        return web.Response(text="Metrics received")
    
    async def get_metrics_handler(self, request):
        """Serve real-time metrics as a JSON response."""
        current_metrics = self.analyzer.get_current_metrics()
        return web.json_response(current_metrics)

    async def distribute_work(self, filepath: str):
        """Split file and assign chunks to workers."""
        if not self.workers:
            print("No workers available.")
            return

        file_size = os.path.getsize(filepath)
        num_workers = len(self.workers)
        chunk_size = file_size // num_workers

        tasks = []
        for i, worker_id in enumerate(self.workers):
            start = i * chunk_size
            size = chunk_size if i < num_workers - 1 else file_size - start
            print(f"Assigning chunk to {worker_id}: start={start}, size={size}")  
            task = self.assign_chunk(worker_id, filepath, start, size)
            tasks.append(task)

        await asyncio.gather(*tasks)

    async def assign_chunk(self, worker_id: str, filepath: str, start: int, size: int):
        """Assign a chunk of the file to a worker."""
        async with ClientSession() as session:
            worker_port = self.workers[worker_id]["port"]
            try:
                await session.post(
                    f"http://localhost:{worker_port}/process_chunk",
                    json={"filepath": filepath, "start": start, "size": size},
                )
                print(f"Assigned chunk ({start}, {size}) to Worker {worker_id}.")
            except Exception as e:
                print(f"Failed to assign chunk to Worker {worker_id}: {e}")
                await self.handle_worker_failure(worker_id)

    async def handle_worker_failure(self, worker_id: str):
        """Handle worker failure and redistribute work."""
        print(f"Worker {worker_id} failed. Reassigning work...")
        if worker_id in self.workers:
            del self.workers[worker_id]

        # Redistribute work
        await self.distribute_work("test_vectors/logs/normal.log")

    async def monitor_workers(self):
        """Monitor workers for health and handle failures."""
        while True:
            for worker_id in list(self.workers.keys()):
                if not self.workers[worker_id].get("healthy", False):
                    print(f"Worker {worker_id} is unresponsive.")
                    await self.handle_worker_failure(worker_id)
                else:
                    self.workers[worker_id]["healthy"] = False  
            await asyncio.sleep(5)

    async def register_worker(self, request):
        """Register a new worker."""
        data = await request.json()
        worker_id = data["worker_id"]
        port = data.get("port", "unknown")
        print(f"Received registration request from Worker {worker_id} on port {port}.")
        
        # Register worker
        self.workers[worker_id] = {"healthy": True, "port": port, "current_task": None}
        return web.Response(text="Worker registered")



if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Log Analyzer Coordinator")
    parser.add_argument("--port", type=int, default=8000, help="Coordinator port")
    args = parser.parse_args()

    coordinator = Coordinator(port=args.port)
    asyncio.run(coordinator.start())
