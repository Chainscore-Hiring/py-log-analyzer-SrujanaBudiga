class Analyzer:
    """Calculates real-time metrics from results."""

    def __init__(self):
        self.metrics = {
            "error_count": 0,
            "total_response_time": 0,
            "request_count": 0,
        }

    def update_metrics(self, new_data: dict):
        """Update metrics with new data from a worker."""
        self.metrics["error_count"] += new_data["error_count"]
        self.metrics["total_response_time"] += new_data["total_response_time"]
        self.metrics["request_count"] += new_data["request_count"]

    def get_current_metrics(self) -> dict:
        """Calculate and return current metrics."""
        avg_response_time = (
            self.metrics["total_response_time"] / self.metrics["request_count"]
            if self.metrics["request_count"] > 0
            else 0
        )
        error_rate = (
            self.metrics["error_count"] / self.metrics["request_count"]
            if self.metrics["request_count"] > 0
            else 0
        )
        return {
            "error_count": self.metrics["error_count"],
            "request_count": self.metrics["request_count"],
            "average_response_time_ms": avg_response_time,
            "error_rate": error_rate,
        }
