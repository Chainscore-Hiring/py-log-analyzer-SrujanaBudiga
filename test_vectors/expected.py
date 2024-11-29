EXPECTED_METRICS = {
    "normal.log": {
        "avg_response_time": 109.0,  # ms
        "error_rate": 0.0,  # errors/minute
        "requests_per_second": 50.0,
        "total_requests": 3000
    },
    "error_spike.log": {
        "avg_response_time": 127.0,
        "error_rate": 30.0,  # 50 errors/100 seconds
        "requests_per_second": 33.33,
        "total_requests": 200
    },
    "mixed_format.log": {
        "avg_response_time": 109.0,
        "error_rate": 0.0,
        "requests_per_second": 50.0,
        "total_requests": 3000
    },
    "malformed.log": {
        "avg_response_time": 127.0,
        "error_rate": 0.0,
        "requests_per_second": 33.33,
        "total_requests": 200,
        "malformed_lines": 30
    }
}