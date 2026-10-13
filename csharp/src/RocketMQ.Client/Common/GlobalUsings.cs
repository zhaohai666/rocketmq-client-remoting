// 全局基础类型别名。
//
// PropertyMap 用 SortedDictionary 而不是 Dictionary：
//   有序字典按键输出，properties2String 的输出顺序因此是确定的；
//   broker 侧按行解析虽不依赖顺序，但确定序便于测试与排查。
global using PropertyMap = System.Collections.Generic.SortedDictionary<string, string>;
