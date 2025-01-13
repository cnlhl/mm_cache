import heapq

class PriorityQueue:
    def __init__(self, min_queue=True):
        self.heap = []  # 存储堆元素
        self.entry_finder = {}  # 存储键到堆中元素的映射
        self.REMOVED = '<removed>'  # 用于标记已删除的元素
        self.counter = 0  # 用于保持堆中元素的顺序
        self.min_queue = min_queue  # 是否为最小队列

    def _add_entry(self, key, weight):
        """将新的元素添加到堆中"""
        if key in self.entry_finder:
            self._remove_entry(key)
        if not self.min_queue:
            weight = -weight  # 如果是最大队列，权重取反
        entry = [weight, self.counter, key]
        self.entry_finder[key] = entry
        heapq.heappush(self.heap, entry)
        self.counter += 1

    def _remove_entry(self, key):
        """标记元素为删除，而不是从堆中直接删除"""
        entry = self.entry_finder.pop(key)
        entry[-1] = self.REMOVED

    def empty(self):
        """检查堆是否为空"""
        return len(self.entry_finder) == 0

    def decrease(self, key):
        """权重减1"""
        if key not in self.entry_finder:
            self._add_entry(key, 0)
        entry = self.entry_finder[key]
        weight, counter, key = entry
        if self.min_queue:
            if weight == 0:
                raise ValueError("Weight cannot go below 0")
            new_weight = weight - 1
        else:
            new_weight = weight + 1
        # 更新权重并重新加入堆
        self._remove_entry(key)
        self._add_entry(key, new_weight)

    def increase(self, key, optional_weight=1):
        """权重增加指定值"""
        if key not in self.entry_finder:
            self._add_entry(key, 0)
        entry = self.entry_finder[key]
        weight, counter, key = entry
        if self.min_queue:
            new_weight = weight + optional_weight
        else:
            new_weight = weight - optional_weight
        # 更新权重并重新加入堆
        self._remove_entry(key)
        self._add_entry(key, new_weight)

    def pop(self):
        """弹出权重最小的元素"""
        while self.heap:
            weight, counter, key = heapq.heappop(self.heap)
            if key is not self.REMOVED:
                del self.entry_finder[key]
                return key, weight if self.min_queue else -weight
        raise KeyError('pop from an empty priority queue')

    def front(self):
        """返回最小权重元素的键值和权重"""
        while self.heap:
            weight, counter, key = self.heap[0]
            if key is not self.REMOVED:
                return key, weight if self.min_queue else -weight
            else:
                heapq.heappop(self.heap)
        raise KeyError('getmin from an empty priority queue')

    def __contains__(self, key):
        """检查键是否存在"""
        return key in self.entry_finder and self.entry_finder[key][-1] is not self.REMOVED
    
    def print_queue(self):
        print(self.heap)
        print(self.entry_finder)