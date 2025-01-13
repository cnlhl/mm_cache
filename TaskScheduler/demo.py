from task import Task
from taskscheduler import TaskScheduler

if __name__ == "__main__":
    # 设置最大内存使用量（例如 8GB）
    max_memory = 8 * 1024 * 1024 * 1024
    scheduler = TaskScheduler(max_memory)

    # 模拟添加任务
    task1 = Task("task1", 1, ["20230103_sh600030_order"], "strategy_module", "strategy_func1")
    scheduler.add_task(task1)

    # 执行任务
    scheduler.execute_tasks()