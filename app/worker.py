from typing import List
from celery import Celery
import os
from dotenv import load_dotenv
from celery.result import AsyncResult

load_dotenv()
mbp = os.getenv("MESSAGE_BROKER_PATH", '127.0.0.1')
app = Celery(
    '.',
    broker='pyamqp://'+mbp+'//',
    backend='rpc://',
    include=['importer']
)


def get_running_tasks() -> List[str]:
    inspector = app.control.inspect()
    # Get the active tasks
    active_tasks = inspector.active()

    # Extract the UUIDs of the running tasks
    running_task_ids: List[str] = [task
                                   for worker_tasks in active_tasks.values() for task in worker_tasks]
    return running_task_ids


def get_running_task_ids() -> List[str]:
    inspector = app.control.inspect()
    # Get the active tasks
    active_tasks = inspector.active()

    # Extract the UUIDs of the running tasks
    running_task_ids: List[str] = [task['id']
                                   for worker_tasks in active_tasks.values() for task in worker_tasks]
    return running_task_ids


def terminate_task(task_id: str):
    if str(task_id) in get_running_task_ids():
        task = AsyncResult(id=str(task_id))
        try:
            task.revoke(terminate=True, signal='SIGKILL')
            return
        except Exception as e:
            raise RuntimeError(f"Execution Error: {e}")
    else:
        # print(f"given id: {task_id}, type: {type(task_id)} ")
        # print(f"running task ids: {get_running_task_ids()}")
        raise ValueError(f"No running Task with given ID found!")
