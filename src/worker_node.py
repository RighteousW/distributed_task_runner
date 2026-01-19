"""Worker node implementation with port assignment and master detection."""

import asyncio
import json
import random
import logging
import socket
import time
from typing import Dict, Any, Optional
from datetime import datetime

from task import Task

class WorkerNode:
    """Worker node that self-assigns ports and monitors master health.
    
    Attributes:
        worker_id: Unique worker identifier
        host: Network host address
        port: Assigned network port (dynamically allocated)
        failure_rate: Simulated failure probability
        master_check_interval: Seconds between master health checks
    """
    def __init__(self, worker_id: str, host: str = "localhost", 
                 port_range: tuple = (5000, 5100), failure_rate: float = 0.1):
        self.worker_id = worker_id
        self.host = host
        self.port = None
        self.port_range = port_range
        self.is_alive = True
        self.current_task = None
        self.completed_tasks = 0
        self.failure_rate = failure_rate
        self.server = None
        self.master_host = None
        self.master_port = None
        self.logger = self._setup_logger()
        self.last_master_contact = time.time()
        self.master_timeout = 30  # Kill self if no contact for 30s
        
    def _setup_logger(self) -> logging.Logger:
        """Setup worker-specific logger."""
        logger = logging.getLogger(self.worker_id)
        logger.setLevel(logging.INFO)
        handler = logging.FileHandler(f"../logs/{self.worker_id}.log")
        handler.setFormatter(logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        ))
        logger.addHandler(handler)
        return logger
    
    def _find_available_port(self) -> int:
        """Find and bind to an available port in range."""
        for port in range(self.port_range[0], self.port_range[1]):
            try:
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                sock.bind((self.host, port))
                sock.close()
                self.logger.info(f"Found available port: {port}")
                return port
            except OSError:
                self.logger.debug(f"Port {port} unavailable, trying next")
                continue
        raise RuntimeError(f"No available ports in range {self.port_range}")
    
    async def start(self):
        """Start worker server with dynamic port assignment."""
        try:
            self.port = self._find_available_port()
            self.server = await asyncio.start_server(
                self._handle_connection, self.host, self.port
            )
            self.logger.info(f"Started on {self.host}:{self.port}")
            print(f"✓ Worker {self.worker_id} listening on {self.host}:{self.port}")
            
            # Start master health monitor
            asyncio.create_task(self._monitor_master())
        except Exception as e:
            self.logger.error(f"Failed to start: {e}")
            raise
    
    async def _monitor_master(self):
        """Monitor master node health and self-terminate if disconnected."""
        while self.is_alive:
            await asyncio.sleep(5)
            
            if self.master_host is None:
                continue
                
            time_since_contact = time.time() - self.last_master_contact
            
            if time_since_contact > self.master_timeout:
                self.logger.warning(f"No master contact for {time_since_contact:.1f}s")
                await self._check_master_alive()
    
    async def _check_master_alive(self):
        """Verify master is still reachable, kill self if not."""
        try:
            reader, writer = await asyncio.wait_for(
                asyncio.open_connection(self.master_host, self.master_port),
                timeout=3.0
            )
            writer.close()
            await writer.wait_closed()
            self.logger.info("Master is still alive")
            self.last_master_contact = time.time()
        except:
            self.logger.critical("Master node unreachable - terminating worker")
            print(f"✗ Worker {self.worker_id} lost master connection - shutting down")
            await self.shutdown()
    
    async def shutdown(self):
        """Gracefully shutdown the worker."""
        self.is_alive = False
        if self.server:
            self.server.close()
            await self.server.wait_closed()
        self.logger.info("Worker shutdown complete")
    
    async def _handle_connection(self, reader: asyncio.StreamReader, 
                                writer: asyncio.StreamWriter):
        """Handle incoming connection from master node."""
        addr = writer.get_extra_info('peername')
        
        try:
            data = await reader.read(4096)
            message = json.loads(data.decode())
            
            # Record master contact
            if self.master_host is None:
                self.master_host, self.master_port = addr[0], message.get("master_port")
                self.logger.info(f"Registered master at {self.master_host}:{self.master_port}")
            
            self.last_master_contact = time.time()
            self.logger.info(f"Received {message['type']} from {addr}")
            
            if message["type"] == "task":
                task = Task.from_dict(message["task"])
                self.logger.info(f"Processing task {task.task_config.task_id}")
                result = await self.complete_task(task)
                response = json.dumps(result)
            elif message["type"] == "heartbeat":
                self.logger.debug("Heartbeat check")
                response = json.dumps(await self.heartbeat())
            elif message["type"] == "register":
                self.logger.info("Registration request received")
                response = json.dumps({"status": "registered", "worker_id": self.worker_id})
            else:
                self.logger.warning(f"Unknown message type: {message['type']}")
                response = json.dumps({"status": "error", "error": "Unknown message type"})
            
            writer.write(response.encode())
            await writer.drain()
        except Exception as e:
            self.logger.error(f"Connection error: {str(e)}")
            error_response = json.dumps({"status": "error", "error": str(e)})
            writer.write(error_response.encode())
            await writer.drain()
        finally:
            writer.close()
            await writer.wait_closed()
    
    async def complete_task(self, task: Task) -> Dict[str, Any]:
        """Execute a task with simulated processing and failures."""
        self.current_task = task
        task.status = "in_progress"
        start_time = datetime.now()
        
        try:
            processing_time = random.uniform(0.5, 2)
            self.logger.info(f"Executing task {task.task_config.task_id} (~{processing_time:.2f}s)")
            await asyncio.sleep(processing_time)
            
            if random.random() < self.failure_rate:
                raise Exception(f"Worker {self.worker_id} simulated failure")
            
            result = self._process_task(task.task_config.payload)
            task.status = "completed"
            self.completed_tasks += 1
            
            self.logger.info(f"Task {task.task_config.task_id} completed successfully")
            
            return {
                "status": "success",
                "worker_id": self.worker_id,
                "task_id": task.task_config.task_id,
                "result": result,
                "processing_time": processing_time
            }
        except Exception as e:
            task.status = "failed"
            self.logger.error(f"Task {task.task_config.task_id} failed: {str(e)}")
            return {
                "status": "failed",
                "worker_id": self.worker_id,
                "task_id": task.task_config.task_id,
                "error": str(e)
            }
        finally:
            elapsed = (datetime.now() - start_time).total_seconds()
            self.logger.info(f"Task finished in {elapsed:.2f}s")
            self.current_task = None
    
    def _process_task(self, payload: Dict[str, Any]) -> Any:
        """Process task based on type."""
        task_type = payload.get("type", "compute")
        if task_type == "compute":
            return sum(payload.get("numbers", [1, 2, 3]))
        elif task_type == "transform":
            return payload.get("data", "").upper()
        else:
            return {"processed": True, "data": payload}
    
    async def heartbeat(self) -> Dict[str, Any]:
        """Return worker health status."""
        return {
            "worker_id": self.worker_id,
            "is_alive": self.is_alive,
            "completed_tasks": self.completed_tasks,
            "current_task": self.current_task.task_config.task_id if self.current_task else None,
            "port": self.port
        }