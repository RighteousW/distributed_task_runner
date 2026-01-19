# Distributed Task Runner

A socket-based distributed task execution system showcasing core distributed systems concepts.

## Key Distributed Systems Concepts

### 1. Dynamic Port Assignment
- Workers automatically find and bind to available ports
- Retry mechanism if ports are occupied
- Eliminates port conflicts in multi-instance scenarios

### 2. Service Discovery
- Master discovers workers by scanning port ranges
- Workers register with master on first contact
- Dynamic worker pool management

### 3. Failure Detection & Recovery
- Workers monitor master health via heartbeat
- Workers self-terminate if master is unreachable (30s timeout)
- Master detects and removes unresponsive workers
- Automatic task retry on worker failure

### 4. Socket-Based Communication
- TCP sockets for reliable message delivery
- JSON protocol for task serialization
- Async I/O for non-blocking communication
- Connection pooling and management

### 5. Fault Tolerance
- Configurable retry logic per task
- Timeout detection for hung workers
- Graceful degradation when workers fail
- Task reallocation on failure

## Architecture

```
Master Node (Port 4999)
    ↓ discovers
Worker-1 (Port 5001)  ←→  Socket Communication
Worker-2 (Port 5003)  ←→  Task Distribution
Worker-3 (Port 5017)  ←→  Health Monitoring
```

## Running the System

```bash
python3 src/main.py
```

**Expected Flow:**
1. Master starts on first available port (4999+)
2. Workers start and self-assign ports (5000-5100 range)
3. Master discovers workers via port scanning
4. Tasks distributed with priority-based scheduling
5. Workers execute and report results
6. Master shutdown triggers worker self-termination

## Log Files

- `master.log` - Task distribution, worker discovery, health checks
- `worker-1.log` - Task execution, master monitoring
- `worker-2.log` - Worker activity and failures
- `worker-3.log` - Port assignment, task processing

## Configuration

Adjust worker count:
```python
workers = await start_workers(num_workers=5)
```

Change port ranges:
```python
WorkerNode(worker_id="worker-1", port_range=(6000, 6100))
```

Modify master timeout for worker self-termination:
```python
worker.master_timeout = 60  # seconds
```

## Distributed Systems Features Demonstrated

1. **Network Partitioning**: Workers handle master disconnection
2. **Leader Election**: Single master coordinates workers
3. **Load Balancing**: Tasks distributed across available workers
4. **Health Monitoring**: Continuous heartbeat mechanism
5. **Graceful Degradation**: System continues with available workers
6. **Retry Logic**: Automatic recovery from transient failures
7. **Port Management**: Dynamic resource allocation
8. **Service Discovery**: Automatic worker registration