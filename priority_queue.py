import heapq

class PriorityQueue:
    def __init__(self):
        self.heap = []  # 存储堆元素
        self.entry_finder = {}  # 存储键到堆中元素的映射
        self.REMOVED = '<removed>'  # 用于标记已删除的元素
        self.counter = 0  # 用于保持堆中元素的顺序

    def _add_entry(self, key, weight):
        """将新的元素添加到堆中"""
        if key in self.entry_finder:
            self.remove(key)
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
        if weight == 0:
            raise ValueError("Weight cannot go below 0")
        # 更新权重并重新加入堆
        self._remove_entry(key)
        self._add_entry(key, weight - 1)

    def increase(self, key):
        """权重加1"""
        if key not in self.entry_finder:
            self._add_entry(key, 0)
        entry = self.entry_finder[key]
        weight, counter, key = entry
        # 更新权重并重新加入堆
        self._remove_entry(key)
        self._add_entry(key, weight + 1)

    def pop(self):
        """弹出权重最小的元素"""
        while self.heap:
            weight, counter, key = heapq.heappop(self.heap)
            if key is not self.REMOVED:
                del self.entry_finder[key]
                return key
        raise KeyError('pop from an empty priority queue')

    def getmin(self):
        """返回最小权重元素的键值和权重"""
        while self.heap:
            weight, counter, key = self.heap[0]
            if key is not self.REMOVED:
                return key, weight
            else:
                heapq.heappop(self.heap)
        raise KeyError('getmin from an empty priority queue')

    def __contains__(self, key):
        """检查键是否存在"""
        return key in self.entry_finder and self.entry_finder[key][-1] is not self.REMOVED
    
    def print_queue(self):
        print(self.heap)
        print(self.entry_finder)
    
# pq = PriorityQueue()

# # 添加元素
# pq.increase('a')  # a权重为1
# pq.print_queue()
# pq.increase('a')  # a权重变为2
# pq.print_queue()
# pq.increase('b')  # b权重为1
# pq.print_queue()
# pq.decrease('b')  # b权重变为0
# pq.print_queue()

# print(pq.pop())  # 输出 b
# pq.print_queue()
# print(pq.getmin())  # 输出 ('a', 1)
# pq.print_queue()

# pq.increase('a')  # a权重变为2
# pq.print_queue()
# pq.increase('c')  # c权重为1
# pq.print_queue()
# print(pq.getmin())  # 输出 ('c', 1)
# pq.print_queue()

# pq.decrease('a')  # a权重变为1
# pq.print_queue()
# print(pq.getmin())  # 输出 ('a', 1)
# pq.print_queue()