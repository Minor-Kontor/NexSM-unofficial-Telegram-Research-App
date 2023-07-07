from enum import Enum, auto


class Gender(Enum):
    female = auto()
    male = auto()
    unknown = auto()


class Action(Enum):
    join = auto()
    leave = auto()
    undefined = auto()
