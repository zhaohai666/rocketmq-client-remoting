#!/usr/bin/env python3
# 检查 C++ 端每个编译单元是否**直接包含**了自己用到的标准库头。
#
# 为什么要有这一步：CI 用的标准库实现裁剪了头文件之间的传递包含，
# 「包含 <vector> 就顺带拿到 <algorithm>」在 CI 上不成立，本地却能过 ——
# 表现就是编译到某个目标才报 “‘xxx’ is not a member of ‘std’”，一次只暴露一个。
# 这里静态扫一遍，把这类缺口一次性列出来。
#
# 判定口径：只有「本文件用到该符号」且「沿本文件的包含闭包（含本项目头文件里写的
# 标准库包含）也取不到对应头」才算缺口。所以给项目头文件补一条 #include 之后，
# 所有包含它的编译单元就不再报错，这与真实预处理结果一致。
#
# 用法：
#   python3 scripts/check_cpp_portable_includes.py cpp          # 只报告，非零退出即有缺口
#   python3 scripts/check_cpp_portable_includes.py cpp --apply  # 按字母序插入缺失的 #include
import pathlib
import re
import sys

# 头文件 -> 该头提供、且容易被当成「别处顺带包含」的符号写法。
NEED = {
    "algorithm": r"std::(sort|stable_sort|for_each|min_element|max_element|minmax|find_if|find_end"
                 r"|find_first_of|find|copy_if|copy_n|copy|transform|count_if|count|reverse_copy|reverse"
                 r"|remove_copy|remove_if|remove|any_of|all_of|none_of|equal_range|lower_bound|upper_bound"
                 r"|binary_search|adjacent_find|adjacent_difference|generate_n|generate|nth_element"
                 r"|partial_sort_copy|partial_sort|partition|set_union|set_intersection|set_difference"
                 r"|set_symmetric_difference|merge|unique_copy|unique|inplace_merge|push_heap|pop_heap"
                 r"|make_heap|sort_heap|clamp|search_n|search|includes|is_heap|is_sorted"
                 r"|lexicographical_compare)\b",
    "cstring": r"(std::|::)?\b(strlen|strcmp|strncmp|strcasecmp|strncasecmp|strcpy|strncpy|strcat"
               r"|strncat|strchr|strrchr|strstr|strtok|strspn|strcspn|memcpy|memmove|memset|memcmp"
               r"|memchr|strerror)\s*\(",
    "cstdio": r"\bstd::(printf|fprintf|sprintf|snprintf|vsnprintf|sscanf|vfprintf|fopen|freopen|fclose"
              r"|fread|fwrite|fgetc|fputc|fgets|fputs|getc|putc|puts|getchar|fflush|perror|rename)\b"
              r"|\bFILE\s*\*|\b(stdout|stderr|stdin)\b",
    "cstdlib": r"\bstd::(atoi|atol|atoll|strtod|strtof|strtol|strtoll|strtoul|strtoull|abort|exit"
               r"|_Exit|quick_exit|qsort|bsearch|getenv|setenv|malloc|calloc|realloc|free|aligned_alloc"
               r"|div|ldiv|abs|labs|llabs|rand|srand)\b",
    "cstddef": r"\bstd::(size_t|ptrdiff_t|max_align_t|nullptr_t)\b",
    "cstdint": r"\b(u?int(8|16|32|64)_t|u?int_least(8|16|32|64)_t|u?int_fast(8|16|32|64)_t"
               r"|intptr_t|uintptr_t)\b",
    "cmath": r"\bstd::(sqrt|cbrt|hypot|pow|fabs|floor|ceil|trunc|round|lround|llround|log|log2|log10"
             r"|exp|exp2|expm1|log1p|sin|cos|tan|asin|acos|atan|atan2|sinh|cosh|tanh|fmod|remainder"
             r"|isfinite|isinf|isnan|erf|erfc|tgamma|lgamma)\s*\(",
    "numeric": r"\bstd::(accumulate|iota|inner_product|partial_sum|reduce|transform_reduce"
               r"|inclusive_scan|exclusive_scan)\b",
    "sstream": r"\bstd::(w?stringstream|w?istringstream|w?ostringstream)\b",
    "iostream": r"\bstd::(cout|cerr|clog|cin|endl|flush)\b",
    "fstream": r"\bstd::(w?ifstream|w?ofstream|w?fstream)\b",
    "iomanip": r"\bstd::(setw|setprecision|setfill|setbase|hexfloat|hex|dec|oct|fixed|scientific"
               r"|showbase|showpoint|showpos|left|right|internal|quoted|ws)\b",
    "iterator": r"\bstd::(back_inserter|front_inserter|inserter|advance|distance|next|prev"
                r"|ostream_iterator|ostreambuf_iterator|istream_iterator|istreambuf_iterator"
                r"|insert_iterator|move_iterator|make_move_iterator)\b",
    "functional": r"\bstd::(function|bind|bind_front|ref|creref|reference_wrapper|less|greater"
                  r"|less_equal|greater_equal|equal_to|not_equal_to|plus|minus|multiplies|divides|hash"
                  r"|default_delete|invoke|not_fn|identity)\b",
    "memory": r"\bstd::(shared_ptr|weak_ptr|unique_ptr|make_shared|make_unique|allocate_shared"
              r"|enable_shared_from_this|bad_weak_ptr|get_deleter|pointer_traits|owner_less)\b",
    "utility": r"\bstd::(move|move_if_noexcept|forward|declval|swap|iter_swap|exchange|pair|make_pair"
               r"|integer_sequence|index_sequence|make_index_sequence|in_place|in_place_t|in_place_type"
               r"|in_place_index|piecewise_construct|apply|as_const)\b",
    "limits": r"\bstd::numeric_limits\b",
    "typeinfo": r"\bstd::(type_info|bad_cast|bad_typeid)\b",
    "type_traits": r"\bstd::(is_[a-z_]+|enable_if|conditional|decay|remove_[a-z_]+|add_[a-z_]+)\s*<",
    "chrono": r"\bstd::(chrono|steady_clock|system_clock|high_resolution_clock|duration"
              r"|duration_cast|time_point_cast|time_point|duration_values|seconds|milliseconds"
              r"|microseconds|nanoseconds|minutes|hours)\b",
    "thread": r"\bstd::thread\b",
    "mutex": r"\bstd::(mutex|timed_mutex|recursive_mutex|recursive_timed_mutex|shared_mutex"
             r"|shared_timed_mutex|lock_guard|unique_lock|scoped_lock|shared_lock|condition_variable"
             r"|condition_variable_any|once_flag|call_once|try_lock|defer_lock|try_to_lock|adopt_lock)\b",
    "atomic": r"\bstd::(atomic_[a-z_]+|atomic\b|memory_order\w*|atomic_load|atomic_store"
              r"|atomic_exchange|atomic_compare_exchange\w*|atomic_fetch_\w+)\b",
    "future": r"\bstd::(future|shared_future|promise|packaged_task|async|launch|future_status"
              r"|future_errc|future_error)\b",
    "stdexcept": r"\bstd::(runtime_error|range_error|overflow_error|underflow_error|logic_error"
                 r"|domain_error|invalid_argument|length_error|out_of_range)\b",
    "exception": r"\bstd::(exception_ptr|make_exception_ptr|current_exception|rethrow_exception"
                 r"|uncaught_exceptions|nested_exception|terminate|set_terminate)\b|\bstd::exception\b",
    "tuple": r"\bstd::(tuple|make_tuple|tie|forward_as_tuple|tuple_size|tuple_element)\b",
    "optional": r"\bstd::(optional|make_optional|nullopt)\b",
    "variant": r"\bstd::(variant|visit|monostate|holds_alternative|get_if|variant_size"
               r"|variant_alternative)\b",
    "any": r"\bstd::(any|any_cast|make_any)\b",
    "bitset": r"\bstd::bitset\b",
    "array": r"\bstd::array\b",
    "deque": r"\bstd::deque\b",
    "list": r"\bstd::(forward_)?list\b",
    "vector": r"\bstd::vector\b",
    "string": r"\bstd::(string|wstring|u16string|u32string|string_view|to_string|stoi|stol|stoll"
              r"|stof|stod)\b",
    "map": r"\bstd::(multimap|map)\b",
    "set": r"\bstd::(multi)?set\b",
    "unordered_map": r"\bstd::(unordered_multimap|unordered_map)\b",
    "unordered_set": r"\bstd::(unordered_multiset|unordered_set)\b",
    "queue": r"\bstd::(queue|priority_queue)\b",
    "stack": r"\bstd::stack\b",
    "random": r"\bstd::(mt19937_64|mt19937|minstd_rand\w*|ranlux\w*|knuth_b|default_random_engine"
              r"|random_device|uniform_int_distribution|uniform_real_distribution|bernoulli_distribution"
              r"|binomial_distribution|geometric_distribution|poisson_distribution"
              r"|exponential_distribution|normal_distribution|lognormal_distribution"
              r"|chi_squared_distribution|cauchy_distribution|shuffle|sample)\b",
    "ctime": r"\bstd::(time|clock|difftime|mktime|asctime|ctime|gmtime|localtime|strftime|clock_t"
             r"|time_t|tm)\b",
}

COMMENT = re.compile(r"//[^\n]*|/\*.*?\*/", re.S)
STR = re.compile(r'"(?:[^"\\\n]|\\.)*"|\'(?:[^\'\\\n]|\\.)*\'')
INCLUDE = re.compile(r"^\s*#\s*include\s*([<\"])([^\">]+)[\">]", re.M)
SEARCH_DIRS = []


def resolve(quoted, from_dir):
    """Locate a quoted include the way the compiler would (own dir, then -I dirs)."""
    for base in [from_dir] + SEARCH_DIRS:
        p = pathlib.Path(base) / quoted
        if p.is_file():
            return p
    return None


def closure(path, seen):
    """Standard headers reachable through this file's whole include closure."""
    try:
        text = path.read_text(encoding="utf-8", errors="replace")
    except OSError:
        return set()
    std = set()
    for m in INCLUDE.finditer(text):
        kind, name = m.group(1), m.group(2)
        if kind == "<":
            std.add(name.split("/")[0])
            continue
        if name in seen:
            continue
        seen.add(name)
        sub = resolve(name, path.parent)
        if sub is not None:
            std |= closure(sub, seen)
    return std


def insert(text, headers):
    """Put the headers into the file's <...> block, keeping it sorted."""
    lines = text.split("\n")
    idx = [i for i, ln in enumerate(lines) if re.match(r"^\s*#\s*include\s*<", ln)]
    if not idx:
        for i, ln in enumerate(lines):
            if re.match(r"^\s*#\s*include\s*\"", ln):
                ins = ["#include <%s>" % h for h in sorted(headers)]
                return "\n".join(lines[:i] + ins + lines[i:])
        return text
    for h in sorted(headers):
        pos = idx[-1] + 1
        for i in idx:
            cur = re.match(r"^\s*#\s*include\s*<([^>/]+)", lines[i]).group(1)
            if cur > h:
                pos = i
                break
        lines.insert(pos, "#include <%s>" % h)
        idx = [j + (1 if j >= pos else 0) for j in idx]
    return "\n".join(lines)


def main():
    apply = "--apply" in sys.argv
    arg = next((a for a in sys.argv[1:] if not a.startswith("--")), "cpp")
    root = pathlib.Path(arg).resolve()
    if not root.is_dir():
        print("no such directory: %s" % root, file=sys.stderr)
        return 2
    global SEARCH_DIRS
    SEARCH_DIRS = [root, root / "include", root / "tests", root / "src", root / "examples"]

    skip = re.compile(r"/(build[^/]*|out|third_party|external)/")
    files = sorted(f for pat in ("*.cpp", "*.cc", "*.h", "*.hpp")
                   for f in root.glob("**/" + pat))
    bad = 0
    missing_total = 0
    for f in files:
        if skip.search(str(f)):
            continue
        text = f.read_text(encoding="utf-8", errors="replace")
        code = STR.sub(" ", COMMENT.sub(" ", text))
        reach = closure(f, set())
        missing = sorted(h for h, rx in NEED.items()
                         if h not in reach and re.search(rx, code))
        if not missing:
            continue
        bad += 1
        missing_total += len(missing)
        print("%s: missing #include <%s>" % (f.relative_to(root.parent), "> / <".join(missing)))
        if apply:
            f.write_text(insert(text, missing), encoding="utf-8")

    if bad:
        print("\n%d file(s) use standard library symbols they do not include directly "
              "(%d header%s). CI's standard library prunes transitive includes, so those "
              "files fail there even though they compile locally.\n"
              "Fix with: python3 scripts/%s %s --apply"
              % (bad, missing_total, "" if missing_total == 1 else "s",
                 pathlib.Path(sys.argv[0]).name, arg))
        return 1
    return 0


sys.exit(main())
