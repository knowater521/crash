"""

create:   2018-12-12
modified:
"""

from typing import Tuple, Dict, FrozenSet, Optional, Union, Pattern, List, Iterator, Type

__all__ = [
    'Tuple', 'List', 'Dict', 'FrozenSet', 'Type', 'Optional',
    'Iterator', 'Union', 'Pattern',
    'MysqlConfig', 'ProductDetailDict'
]

MysqlConfig = Dict[str, Union[str, int]]
ProductDetailDict = Dict[str, Union[str, int, None]]
