class NetworkScenarios:
    @staticmethod
    async def normal():
        """All workers responsive"""
        return {
            "worker1": {"healthy": True, "latency": 10},
            "worker2": {"healthy": True, "latency": 15},
            "worker3": {"healthy": True, "latency": 12}
        }

    @staticmethod
    async def worker_failure():
        """Worker 2 fails after 50% processing"""
        return {
            "worker1": {"healthy": True, "latency": 10},
            "worker2": {"healthy": False, "fail_at": 0.5},
            "worker3": {"healthy": True, "latency": 12}
        }

    @staticmethod
    async def high_latency():
        """Worker 3 has high latency"""
        return {
            "worker1": {"healthy": True, "latency": 10},
            "worker2": {"healthy": True, "latency": 15},
            "worker3": {"healthy": True, "latency": 1000}
        }