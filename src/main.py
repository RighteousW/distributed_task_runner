"""Main entry point demonstrating distributed system concepts."""

import asyncio
from task import Task, TaskConfig, TaskPriority
from worker_node import WorkerNode
from master_node import MasterNode

async def start_workers(num_workers: int = 3):
    """Start worker nodes with dynamic port assignment."""
    workers = []
    
    for i in range(1, num_workers + 1):
        worker = WorkerNode(
            worker_id=f"worker-{i}",
            host="localhost",
            port_range=(5000, 5100),
            failure_rate=0.1 + (i * 0.02)  # Varying failure rates
        )
        await worker.start()
        workers.append(worker)
        await asyncio.sleep(0.1)  # Stagger startup
    
    return workers

async def run_demo():
    """Run distributed task runner demonstration."""
    print("=" * 60)
    print("DISTRIBUTED TASK RUNNER - Socket-Based System")
    print("=" * 60)
    print()
    
    # Start master node
    print("→ Starting master node...")
    master = MasterNode(host="localhost", port=4999)
    await master.start()
    await asyncio.sleep(0.5)
    
    # Start workers (they self-assign ports)
    print("\n→ Starting worker nodes...")
    workers = await start_workers(num_workers=3)
    await asyncio.sleep(1)
    
    # Master discovers workers
    print("\n→ Master discovering workers...")
    worker_ports = [w.port for w in workers]
    await master.discover_workers(worker_ports)
    await asyncio.sleep(0.5)
    
    # Create tasks
    print("\n→ Creating tasks...")
    tasks = [
        Task(TaskConfig(
            task_id=f"compute-{i}",
            priority=TaskPriority.HIGH if i % 3 == 0 else TaskPriority.MEDIUM,
            payload={"type": "compute", "numbers": [i, i+1, i+2]},
            max_retries=3,
            timeout=10.0
        )) for i in range(10)
    ]
    
    tasks.extend([
        Task(TaskConfig(
            task_id=f"transform-{i}",
            priority=TaskPriority.LOW,
            payload={"type": "transform", "data": f"hello world {i}"},
            max_retries=2,
            timeout=10.0
        )) for i in range(5)
    ])
    
    print(f"Created {len(tasks)} tasks")
    
    # Distribute tasks
    print("\n→ Distributing tasks...")
    print("-" * 60)
    start_time = asyncio.get_event_loop().time()
    results = await master.distribute_tasks(tasks)
    end_time = asyncio.get_event_loop().time()
    
    # Print results
    print("\n" + "=" * 60)
    print("EXECUTION RESULTS")
    print("=" * 60)
    print(f"Total time: {end_time - start_time:.2f}s")
    
    stats = master.get_statistics()
    print(f"\nCompleted: {stats['completed_tasks']}")
    print(f"Failed: {stats['failed_tasks']}")
    print(f"Workers: {stats['total_workers']} ({', '.join(stats['workers'])})")
    
    print("\n→ Sample successful results:")
    for result in [r for r in results if isinstance(r, dict) and r.get("status") == "success"][:5]:
        print(f"  • {result['task_id']}: {result['result']} (worker: {result['worker_id']}, {result['processing_time']:.2f}s)")
    
    # Test worker self-termination
    print("\n→ Testing worker self-termination...")
    print("  Shutting down master to trigger worker shutdown...")
    master.is_alive = False
    if master.server:
        master.server.close()
        await master.server.wait_closed()
    
    print("  Waiting for workers to detect master failure...")
    await asyncio.sleep(35)  # Wait for workers to timeout
    
    print("\n✓ Demo complete - check log files for details")

async def main():
    """Main entry point."""
    try:
        await run_demo()
    except KeyboardInterrupt:
        print("\n\n✗ Interrupted by user")

if __name__ == "__main__":
    asyncio.run(main())
