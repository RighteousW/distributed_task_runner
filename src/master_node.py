"""Master node with worker discovery and health monitoring."""

import asyncio
import json
import logging
import socket
from collections import deque
from typing import List, Dict, Any, Optional
from datetime import datetime
import time

from task import Task

class MasterNode:
    """Master node that discovers workers and distributes tasks.
    
    Attributes:
        host: Master host address
        port: Master listening port
        discovered_workers: Dict of discovered worker info
        task_queue: Queue of pending tasks
        completed_tasks: List of completed tasks
        failed_tasks: List of failed tasks
    """
    def __init__(self, host: str = "localhost", port: int = 4999):
        self.host = host
        self.port = self._find_available_port(port)
        self.discovered_workers = {}  # {worker_id: (host, port, last_seen)}
        self.task_queue = deque()
        self.completed_tasks = []
        self.failed_tasks = []
        self.busy_workers = set()
        self.logger = self._setup_logger()
        self.is_alive = True
        self.server = None
        
    def _find_available_port(self, preferred_port: int) -> int:
        """Find available port for master, try preferred first."""
        for port in range(preferred_port, preferred_port + 100):
            try:
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                sock.bind((self.host, port))
                sock.close()
                return port
            except OSError:
                continue
        raise RuntimeError("No available ports for master")
        
    def _setup_logger(self) -> logging.Logger:
        """Setup master node logger."""
        logger = logging.getLogger("MasterNode")
        logger.setLevel(logging.INFO)
        handler = logging.FileHandler("../logs/master.log")
        handler.setFormatter(logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        ))
        logger.addHandler(handler)
        
        console = logging.StreamHandler()
        console.setLevel(logging.INFO)
        console.setFormatter(logging.Formatter('%(levelname)s - %(message)s'))
        logger.addHandler(console)
        
        return logger
    
    async def start(self):
        """Start master server for worker registration."""
        self.server = await asyncio.start_server(
            self._handle_worker_registration, self.host, self.port
        )
        self.logger.info(f"Master started on {self.host}:{self.port}")
        print(f"✓ Master node listening on {self.host}:{self.port}")
    
    async def _handle_worker_registration(self, reader: asyncio.StreamReader,
                                          writer: asyncio.StreamWriter):
        """Handle worker registration requests."""
        addr = writer.get_extra_info('peername')
        self.logger.info(f"Connection from {addr}")
        writer.close()
        await writer.wait_closed()
    
    async def discover_workers(self, worker_ports: List[int]):
        """Discover and register workers on specified ports."""
        self.logger.info(f"Discovering workers on ports {worker_ports}")
        
        for port in worker_ports:
            try:
                result = await self._send_message(self.host, port, {
                    "type": "register",
                    "master_port": self.port
                })
                
                if result.get("status") == "registered":
                    worker_id = result["worker_id"]
                    self.discovered_workers[worker_id] = (self.host, port, time.time())
                    self.logger.info(f"Discovered worker {worker_id} on port {port}")
                    print(f"✓ Registered worker {worker_id} at {self.host}:{port}")
            except Exception as e:
                self.logger.debug(f"No worker on port {port}: {e}")
    
    async def _send_message(self, host: str, port: int, message: Dict[str, Any]) -> Dict[str, Any]:
        """Send message to worker and receive response."""
        try:
            reader, writer = await asyncio.open_connection(host, port)
            message["master_port"] = self.port
            data = json.dumps(message).encode()
            writer.write(data)
            await writer.drain()
            
            response = await asyncio.wait_for(reader.read(4096), timeout=5.0)
            writer.close()
            await writer.wait_closed()
            
            return json.loads(response.decode())
        except Exception as e:
            self.logger.error(f"Communication error with {host}:{port} - {str(e)}")
            return {"status": "error", "error": str(e)}
    
    async def distribute_task(self, task: Task) -> Dict[str, Any]:
        """Distribute task to available worker with retry logic."""
        max_retries = task.task_config.max_retries
        self.logger.info(f"Distributing task {task.task_config.task_id} (priority: {task.task_config.priority.name})")
        
        while task.retry_count < max_retries:
            worker = await self._find_available_worker()
            
            if not worker:
                self.logger.warning(f"No workers available for task {task.task_config.task_id}")
                await asyncio.sleep(1)
                continue
            
            worker_id, (host, port, last_seen) = worker
            task.assigned_worker = worker_id
            self.busy_workers.add(worker_id)
            
            self.logger.info(f"Task {task.task_config.task_id} → {worker_id} (attempt {task.retry_count + 1}/{max_retries})")
            
            try:
                message = {"type": "task", "task": task.to_dict()}
                result = await asyncio.wait_for(
                    self._send_message(host, port, message),
                    timeout=task.task_config.timeout
                )
                
                self.busy_workers.discard(worker_id)
                
                if result["status"] == "success":
                    self.completed_tasks.append(task)
                    self.discovered_workers[worker_id] = (host, port, time.time())
                    self.logger.info(f"Task {task.task_config.task_id} completed by {worker_id}")
                    return result
                else:
                    task.retry_count += 1
                    self.logger.warning(f"Task {task.task_config.task_id} failed: {result.get('error')}")
                    
            except asyncio.TimeoutError:
                task.retry_count += 1
                self.busy_workers.discard(worker_id)
                self.logger.error(f"Task {task.task_config.task_id} timed out on {worker_id}")
                # Remove dead worker
                del self.discovered_workers[worker_id]
                
            await asyncio.sleep(0.5)
        
        task.status = "failed"
        self.failed_tasks.append(task)
        self.logger.error(f"Task {task.task_config.task_id} failed after {max_retries} attempts")
        return {"status": "failed", "task_id": task.task_config.task_id, "error": "Max retries exceeded"}
    
    async def _find_available_worker(self) -> Optional[tuple]:
        """Find available worker from discovered workers."""
        for worker_id, (host, port, last_seen) in list(self.discovered_workers.items()):
            if worker_id in self.busy_workers:
                continue
            
            # Check if worker is still alive
            try:
                heartbeat = await self._send_message(host, port, {"type": "heartbeat"})
                if heartbeat.get("is_alive"):
                    self.logger.debug(f"Worker {worker_id} available")
                    self.discovered_workers[worker_id] = (host, port, time.time())
                    return (worker_id, (host, port, time.time()))
            except:
                self.logger.warning(f"Worker {worker_id} unresponsive")
                del self.discovered_workers[worker_id]
        
        return None
    
    async def distribute_tasks(self, tasks: List[Task]) -> List[Dict[str, Any]]:
        """Distribute multiple tasks concurrently."""
        sorted_tasks = sorted(tasks, key=lambda t: t.task_config.priority.value, reverse=True)
        self.logger.info(f"Distributing {len(tasks)} tasks to {len(self.discovered_workers)} workers")
        results = await asyncio.gather(*[self.distribute_task(task) for task in sorted_tasks])
        self.logger.info("Task distribution complete")
        return results
    
    def get_statistics(self) -> Dict[str, Any]:
        """Get system statistics."""
        return {
            "total_workers": len(self.discovered_workers),
            "active_workers": len([w for w in self.discovered_workers.keys() if w not in self.busy_workers]),
            "completed_tasks": len(self.completed_tasks),
            "failed_tasks": len(self.failed_tasks),
            "workers": list(self.discovered_workers.keys())
        }