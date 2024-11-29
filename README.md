# Distributed Log Analyzer - Technical Assessment
**Time: 6-8 hours | Python 3.9+ | Focus: Systems Programming**

## Your Task
Build a distributed log processing system with one coordinator and multiple workers that can:
1. Process large log files efficiently
2. Calculate real-time metrics
3. Handle worker failures gracefully

## Core Requirements

### 1. Basic Processing
Build a worker node that can:
- Read and parse log files efficiently
- Extract timestamp, level, message, and metrics
- Process logs in chunks to manage memory
- Send results to coordinator

### 2. Distribution
Build a coordinator that can:
- Split work among multiple workers
- Track processing progress
- Handle worker failures
- Aggregate results

### 3. Real-time Analysis
Calculate these metrics in real-time:
- Error rate per minute
- Average response time
- Request count per second
- Resource usage patterns

## What We Provide
```python
# starter/log_entry.py
from datetime import datetime
from typing import Dict, Optional

class LogEntry:
    def __init__(
        self,
        timestamp: datetime,
        level: str,
        message: str,
        metrics: Optional[Dict[str, float]] = None
    ):
        self.timestamp = timestamp
        self.level = level
        self.message = message
        self.metrics = metrics or {}

# starter/sample_logs.py
SAMPLE_LOGS = """
2024-01-24 10:15:32.123 INFO Request processed in 127ms
2024-01-24 10:15:33.001 ERROR Database connection failed
2024-01-24 10:15:34.042 INFO Request processed in 95ms
"""

# starter/test_data.py
def generate_test_logs(size_mb: int, path: str):
    """Generates sample log files for testing"""
    pass
```

## What You Need to Build

1. Worker Node
```python
class Worker:
    """Processes log chunks and reports results"""
    
    def __init__(self, worker_id: str, coordinator_url: str):
        self.worker_id = worker_id
        self.coordinator_url = coordinator_url

    async def process_chunk(self, filepath: str, start: int, size: int) -> Dict:
        """Process a chunk of log file and return metrics"""
        pass

    async def report_health(self) -> None:
        """Send heartbeat to coordinator"""
        pass
```

2. Coordinator
```python
class Coordinator:
    """Manages workers and aggregates results"""
    
    def __init__(self, port: int):
        self.workers = {}
        self.results = {}

    async def distribute_work(self, filepath: str) -> None:
        """Split file and assign chunks to workers"""
        pass

    async def handle_worker_failure(self, worker_id: str) -> None:
        """Reassign work from failed worker"""
        pass
```

3. Analysis Engine
```python
class Analyzer:
    """Calculates real-time metrics from results"""
    
    def __init__(self):
        self.metrics = {}

    def update_metrics(self, new_data: Dict) -> None:
        """Update metrics with new data from workers"""
        pass

    def get_current_metrics(self) -> Dict:
        """Return current calculated metrics"""
        pass
```

## Testing Requirements
1. Test with provided sample log files in `test_vectors/logs/`
2. Run with 3 worker nodes
3. Simulate worker failure
4. Measure processing speed
5. Monitor memory usage

## Evaluation Criteria

1. Code Quality (40%)
- Clean, well-structured code
- Error handling
- Documentation
- Testing

2. System Design (30%)
- Worker management
- Failure handling
- Resource efficiency
- Communication protocol

3. Performance (30%)
- Processing speed
- Memory usage
- Failure recovery time
- Result accuracy

## Deliverables
1. Source code with:
   - Worker implementation
   - Coordinator implementation
   - Analysis engine
   - Tests

2. Brief documentation:
   - Setup instructions
   - Design decisions
   - Performance results

## Getting Started
```bash
# Setup project
git clone https://github.com/chainscore-hiring/log-analyzer-assessment
cd log-analyzer-assessment
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt

# Start system (in separate terminals)
python coordinator.py --port 8000
python worker.py --id alice --port 8001 --coordinator http://localhost:8000
python worker.py --id bob --port 8002 --coordinator http://localhost:8000
python worker.py --id charlie --port 8003 --coordinator http://localhost:8000
```