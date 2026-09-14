// 全局基础类型别名（对应 C++ 的 types.h）。
//
// PropertyMap 用 SortedDictionary 而不是 Dictionary：
//   C++ 侧是 std::map（按键有序），properties2String 的输出顺序因此是确定的；
//   broker 侧按行解析虽不依赖顺序，但确定序便于测试与排查。
global using PropertyMap = System.Collections.Generic.SortedDictionary<string, string>;
