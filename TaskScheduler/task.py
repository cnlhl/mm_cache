class Task:
    def __init__(self, task_id, priority, data_requirements, module_name, func_name):
        self.task_id = task_id
        self.priority = priority
        self.data_requirements = data_requirements
        self.status = "待执行"
        self.module_name = module_name
        self.func_name = func_name

    def start(self):
        self.status = "执行中"

    def complete(self):
        self.status = "已完成"