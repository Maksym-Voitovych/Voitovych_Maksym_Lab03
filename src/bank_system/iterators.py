from typing import Iterator, List, Any, Dict

class AccountRangeIterator:
    """Власний ітератор для генерації діапазону номерів рахунків (протокол __iter__ / __next__)."""
    def __init__(self, start_id: int, count: int) -> None:
        self.current = start_id
        self.end = start_id + count

    def __iter__(self) -> 'AccountRangeIterator':
        return self

    def __next__(self) -> int:
        if self.current >= self.end:
            raise StopIteration
        val = self.current
        self.current += 1
        return val


def recursive_flatten_operations(batch_groups: List[Any]) -> Iterator[Dict[str, Any]]:
    """Рекурсивний генератор із yield from для розгортання вкладених структур даних."""
    for group in batch_groups:
        if isinstance(group, list):
            yield from recursive_flatten_operations(group)
        else:
            yield group