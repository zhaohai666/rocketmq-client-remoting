# -*- coding: utf-8 -*-
from __future__ import annotations

from enum import Enum


class BoundaryType(Enum):
    pass

    LOWER = "LOWER"
    UPPER = "UPPER"

    @property
    def lowercase_name(self) -> str:
        pass
        return self.value.lower()

    @staticmethod
    def get_type(name) -> "BoundaryType":
        pass
        if isinstance(name, str) and name.lower() == BoundaryType.UPPER.lowercase_name:
            return BoundaryType.UPPER
        return BoundaryType.LOWER


__all__ = ["BoundaryType"]
