// The wire value is the upper-case enum name ("LOWER"/"UPPER"); getType() is lenient — only
// "upper" (case-insensitive) maps to UPPER, everything else (null / blank / "lower") is LOWER.

export const BoundaryType = {
  LOWER: 'LOWER',
  UPPER: 'UPPER',

  lowercaseName(value: string): string {
    return value.toLowerCase();
  },

  get_type(name: any): string {
    if (typeof name === 'string' && name.toLowerCase() === BoundaryType.UPPER.toLowerCase()) {
      return BoundaryType.UPPER;
    }
    return BoundaryType.LOWER;
  },
};

export default BoundaryType;
