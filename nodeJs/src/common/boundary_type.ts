// -*- coding: utf-8 -*-
// BoundaryType (org.apache.rocketmq.common.BoundaryType).
// Faithful port of python/common/boundary_type.py.
//
// The wire value is the upper-case enum name ("LOWER"/"UPPER"); getType() is lenient — only
// "upper" (case-insensitive) maps to UPPER, everything else (null / blank / "lower") is LOWER.

export const BoundaryType = {
  LOWER: 'LOWER',
  UPPER: 'UPPER',

  // Java BoundaryType.getName(): lower-case name, used only for comparison/logging.
  lowercaseName(value: string): string {
    return value.toLowerCase();
  },

  // Java BoundaryType.getType(String): non-"upper" (case-insensitive) => LOWER.
  get_type(name: any): string {
    if (typeof name === 'string' && name.toLowerCase() === BoundaryType.UPPER.toLowerCase()) {
      return BoundaryType.UPPER;
    }
    return BoundaryType.LOWER;
  },
};

export default BoundaryType;
