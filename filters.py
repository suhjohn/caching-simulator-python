from abc import ABC


class BaseFilter(ABC):
    pass


class BypassFilter(BaseFilter):
    pass


class BloomFilter(BaseFilter):
    pass
