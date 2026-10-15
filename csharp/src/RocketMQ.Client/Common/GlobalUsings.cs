// Global basic type aliases.
//
// PropertyMap uses SortedDictionary instead of Dictionary:
//   an ordered dictionary outputs keys in order, so the output order of properties2String is deterministic;
//   although the broker-side line-by-line parsing does not depend on order, a deterministic order aids testing and troubleshooting.
global using PropertyMap = System.Collections.Generic.SortedDictionary<string, string>;
