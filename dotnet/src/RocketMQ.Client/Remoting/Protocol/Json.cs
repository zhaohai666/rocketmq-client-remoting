// 极简 JSON 解析 / 生成（仅满足 RemotingSerializable 的需求，零第三方依赖）。
//
// 之所以自带实现而不用 System.Text.Json：RocketMQ 服务端（fastjson2）会写出
// **不合法的 JSON**，标准解析器一律拒收，而管理端大量 body 恰恰是这些形状：
//   - map 的 key 是对象：{"offsetTable":{{"brokerName":"b","queueId":3,"topic":"t"}:{...}}}
//   - map 的 key 是裸数字：{"brokerAddrs":{0:"127.0.0.1"}}
//   - NaN / Infinity / -Infinity 字面量
//   - 尾随逗号
// 另外数字必须区分 int64 与 double：long 型 timestamp 若被写成浮点，broker 侧字段就对不上。
//
// 与 python/rocketmq 的 _FastJsonParser、cpp 的 json.cpp 行为逐条对齐：
//   - Object 保序：set() 覆盖同名键时**保留原位置**（与 Python dict 一致）；
//   - 字符串按 UTF-8 原样输出（不解码为 \uXXXX），解析时兼容 \uXXXX 与代理对；
//   - 解析对 fastjson2 的非标准写法宽容。
using System.Globalization;
using System.Text;

namespace RocketMQ.Remoting.Protocol;

public sealed class JsonValue
{
    public enum JsonType
    {
        Null,
        Bool,
        Number,
        String,
        Array,
        Object,
    }

    private static readonly JsonValue NullInstance = new();

    private JsonType _type = JsonType.Null;
    private bool _bool;
    private double _num;
    private long _intVal;
    private bool _intIsInt64;
    private string _str = string.Empty;
    private List<JsonValue> _arr = new();
    private List<KeyValuePair<string, JsonValue>> _obj = new();

    private JsonValue()
    {
    }

    public static JsonValue MakeNull() => new();

    public static JsonValue MakeBool(bool v) => new() { _type = JsonType.Bool, _bool = v };

    public static JsonValue MakeInt(long v) => new()
    {
        _type = JsonType.Number,
        _intIsInt64 = true,
        _intVal = v,
        _num = v,
    };

    public static JsonValue MakeDouble(double v) => new()
    {
        _type = JsonType.Number,
        _intIsInt64 = false,
        _num = v,
        _intVal = unchecked((long)v),
    };

    public static JsonValue MakeString(string v) => new() { _type = JsonType.String, _str = v };

    public static JsonValue MakeArray() => new() { _type = JsonType.Array };

    public static JsonValue MakeObject() => new() { _type = JsonType.Object };

    /// <summary>共享的 Null 实例（只读，勿 Set）。供链式取值回落用。</summary>
    public static JsonValue Null => NullInstance;

    public JsonType Type => _type;
    public bool IsNull => _type == JsonType.Null;
    public bool IsBool => _type == JsonType.Bool;
    public bool IsNumber => _type == JsonType.Number;
    public bool IsString => _type == JsonType.String;
    public bool IsArray => _type == JsonType.Array;
    public bool IsObject => _type == JsonType.Object;

    public bool BoolValue(bool def = false) =>
        _type == JsonType.Bool ? _bool : (_type == JsonType.Number ? _num != 0 : def);

    public long IntValue(long def = 0) =>
        _type == JsonType.Number ? (_intIsInt64 ? _intVal : unchecked((long)_num)) : def;

    public double DoubleValue(double def = 0.0) =>
        _type == JsonType.Number ? (_intIsInt64 ? _intVal : _num) : def;

    public string StringValue(string def = "") => _type == JsonType.String ? _str : def;

    // ---- array ----
    public int Size() => _arr.Count;

    public JsonValue At(int i) => (uint)i >= (uint)_arr.Count ? NullInstance : _arr[i];

    public void PushArray(JsonValue v)
    {
        _type = JsonType.Array;
        _arr.Add(v);
    }

    // ---- object ----
    /// <summary>保序的键值对视图（供遍历；顺序与插入/解析顺序一致）。</summary>
    public IReadOnlyList<KeyValuePair<string, JsonValue>> ObjectItems() => _obj;

    public bool Contains(string key)
    {
        foreach (var kv in _obj)
        {
            if (kv.Key == key)
            {
                return true;
            }
        }

        return false;
    }

    public JsonValue? Find(string key)
    {
        foreach (var kv in _obj)
        {
            if (kv.Key == key)
            {
                return kv.Value;
            }
        }

        return null;
    }

    /// <summary>找不到时返回共享 Null，便于链式取值。</summary>
    public JsonValue Get(string key) => Find(key) ?? NullInstance;

    public void Set(string key, JsonValue v)
    {
        _type = JsonType.Object;
        for (int i = 0; i < _obj.Count; i++)
        {
            if (_obj[i].Key == key)
            {
                // 覆盖式插入：保留原位置
                _obj[i] = new KeyValuePair<string, JsonValue>(key, v);
                return;
            }
        }

        _obj.Add(new KeyValuePair<string, JsonValue>(key, v));
    }

    // 便捷取值：key 存在且类型匹配才返回 true
    public bool TryGetString(string key, out string value)
    {
        JsonValue? p = Find(key);
        if (p is null || !p.IsString)
        {
            value = string.Empty;
            return false;
        }

        value = p.StringValue();
        return true;
    }

    public bool TryGetInt(string key, out long value)
    {
        JsonValue? p = Find(key);
        if (p is null || !p.IsNumber)
        {
            value = 0;
            return false;
        }

        value = p.IntValue();
        return true;
    }

    public bool TryGetBool(string key, out bool value)
    {
        JsonValue? p = Find(key);
        if (p is null || !p.IsBool)
        {
            value = false;
            return false;
        }

        value = p.BoolValue();
        return true;
    }

    public string Dump()
    {
        var sb = new StringBuilder();
        DumpImpl(sb);
        return sb.ToString();
    }

    private void DumpImpl(StringBuilder sb)
    {
        switch (_type)
        {
            case JsonType.Null:
                sb.Append("null");
                break;
            case JsonType.Bool:
                sb.Append(_bool ? "true" : "false");
                break;
            case JsonType.Number:
                if (_intIsInt64)
                {
                    sb.Append(_intVal.ToString(CultureInfo.InvariantCulture));
                }
                else if ((double)_intVal == _num)
                {
                    // double 恰好是整数值时按整数输出（与 C++ 行为一致）
                    sb.Append(_intVal.ToString(CultureInfo.InvariantCulture));
                }
                else
                {
                    AppendDouble(sb, _num);
                }

                break;
            case JsonType.String:
                AppendUtf8Escaped(sb, _str);
                break;
            case JsonType.Array:
                sb.Append('[');
                for (int i = 0; i < _arr.Count; i++)
                {
                    if (i > 0)
                    {
                        sb.Append(',');
                    }

                    _arr[i].DumpImpl(sb);
                }

                sb.Append(']');
                break;
            case JsonType.Object:
                sb.Append('{');
                bool first = true;
                foreach (var kv in _obj)
                {
                    if (!first)
                    {
                        sb.Append(',');
                    }

                    first = false;
                    AppendUtf8Escaped(sb, kv.Key);
                    sb.Append(':');
                    kv.Value.DumpImpl(sb);
                }

                sb.Append('}');
                break;
        }
    }

    private static void AppendUtf8Escaped(StringBuilder sb, string s)
    {
        sb.Append('"');
        foreach (char c in s)
        {
            switch (c)
            {
                case '"': sb.Append("\\\""); break;
                case '\\': sb.Append("\\\\"); break;
                case '\b': sb.Append("\\b"); break;
                case '\f': sb.Append("\\f"); break;
                case '\n': sb.Append("\\n"); break;
                case '\r': sb.Append("\\r"); break;
                case '\t': sb.Append("\\t"); break;
                default:
                    if (c < 0x20)
                    {
                        sb.Append("\\u").Append(((int)c).ToString("x4", CultureInfo.InvariantCulture));
                    }
                    else
                    {
                        // 非 ASCII 直接按 UTF-16 char 输出（.NET string 即 UTF-16，
                        // 写文件/发网络时统一由 UTF-8 编码，与 Python ensure_ascii=False 一致）
                        sb.Append(c);
                    }

                    break;
            }
        }

        sb.Append('"');
    }

    private static void AppendDouble(StringBuilder sb, double v)
    {
        if (double.IsNaN(v) || double.IsInfinity(v))
        {
            sb.Append("null"); // JSON 无 NaN/Inf
            return;
        }

        // G17 等价于 C 的 %.17g（17 位有效数字，往返无损）
        string s = v.ToString("G17", CultureInfo.InvariantCulture);
        if (s.IndexOfAny(new[] { '.', 'e', 'E' }) < 0)
        {
            s += ".0"; // 确保带小数/指数点，避免被当成整数解析
        }

        sb.Append(s);
    }
}

public static class Json
{
    /// <summary>
    /// 解析失败返回 false（对应 C++ jsonParse 的 bool 返回 + 可选 err）。
    /// 对 fastjson2 的非标准写法宽容，见类注释。
    /// </summary>
    public static bool TryParse(string text, out JsonValue value, out string? error)
    {
        value = JsonValue.Null;
        error = null;
        if (string.IsNullOrEmpty(text))
        {
            error = "empty input";
            return false;
        }

        var parser = new Parser(text);
        if (!parser.Parse(out value))
        {
            error = "json parse error";
            return false;
        }

        return true;
    }

    /// <summary>解析失败抛异常版本（内部便利方法）。</summary>
    public static JsonValue Parse(string text)
    {
        if (!TryParse(text, out JsonValue v, out string? err))
        {
            throw new FormatException(err ?? "json parse error");
        }

        return v;
    }

    public static string Dump(JsonValue v) => v.Dump();

    private sealed class Parser
    {
        private readonly string _s;
        private int _pos;

        internal Parser(string text) => _s = text;

        internal bool Parse(out JsonValue value)
        {
            SkipWs();
            if (!ParseValue(out value))
            {
                return false;
            }

            SkipWs();
            return _pos == _s.Length;
        }

        private void SkipWs()
        {
            while (_pos < _s.Length)
            {
                char c = _s[_pos];
                if (c == ' ' || c == '\t' || c == '\n' || c == '\r')
                {
                    _pos++;
                }
                else
                {
                    break;
                }
            }
        }

        private bool Eof => _pos >= _s.Length;
        private char Peek => _pos < _s.Length ? _s[_pos] : '\0';

        private bool ParseValue(out JsonValue value)
        {
            value = JsonValue.Null;
            SkipWs();
            if (Eof)
            {
                return false;
            }

            char c = Peek;
            switch (c)
            {
                case '{': return ParseObject(out value);
                case '[': return ParseArray(out value);
                case '"':
                    if (!ParseString(out string str))
                    {
                        return false;
                    }

                    value = JsonValue.MakeString(str);
                    return true;
                case 't':
                case 'f': return ParseLiteral(out value);
                case 'n': return ParseNull(out value);
                // fastjson2 在 WriteMapNullValue=false / 浮点越界时会写出这三种非标准字面量，
                // 标准 JSON 没有它们。
                case 'N': return ParseNonFinite(out value, "NaN", double.NaN);
                case 'I': return ParseNonFinite(out value, "Infinity", double.PositiveInfinity);
                case '-':
                    if (_pos + 9 <= _s.Length && string.CompareOrdinal(_s, _pos, "-Infinity", 0, 9) == 0)
                    {
                        _pos += 9;
                        value = JsonValue.MakeDouble(double.NegativeInfinity);
                        return true;
                    }

                    return ParseNumber(out value);
                default: return ParseNumber(out value);
            }
        }

        private bool ParseNonFinite(out JsonValue value, string token, double v)
        {
            value = JsonValue.Null;
            if (_pos + token.Length > _s.Length ||
                string.CompareOrdinal(_s, _pos, token, 0, token.Length) != 0)
            {
                return false;
            }

            _pos += token.Length;
            value = JsonValue.MakeDouble(v);
            return true;
        }

        private bool ParseLiteral(out JsonValue value)
        {
            value = JsonValue.Null;
            if (_pos + 4 <= _s.Length && string.CompareOrdinal(_s, _pos, "true", 0, 4) == 0)
            {
                _pos += 4;
                value = JsonValue.MakeBool(true);
                return true;
            }

            if (_pos + 5 <= _s.Length && string.CompareOrdinal(_s, _pos, "false", 0, 5) == 0)
            {
                _pos += 5;
                value = JsonValue.MakeBool(false);
                return true;
            }

            return false;
        }

        private bool ParseNull(out JsonValue value)
        {
            value = JsonValue.Null;
            if (_pos + 4 <= _s.Length && string.CompareOrdinal(_s, _pos, "null", 0, 4) == 0)
            {
                _pos += 4;
                value = JsonValue.MakeNull();
                return true;
            }

            return false;
        }

        private bool ParseNumber(out JsonValue value)
        {
            value = JsonValue.Null;
            int start = _pos;
            while (_pos < _s.Length)
            {
                char c = _s[_pos];
                if ((c >= '0' && c <= '9') || c == '-' || c == '+' || c == '.' || c == 'e' || c == 'E')
                {
                    _pos++;
                }
                else
                {
                    break;
                }
            }

            if (_pos == start)
            {
                return false;
            }

            string tok = _s[start.._pos];
            try
            {
                if (tok.IndexOfAny(new[] { '.', 'e', 'E' }) >= 0)
                {
                    value = JsonValue.MakeDouble(double.Parse(
                        tok, NumberStyles.Float, CultureInfo.InvariantCulture));
                }
                else
                {
                    value = JsonValue.MakeInt(long.Parse(
                        tok, NumberStyles.Integer, CultureInfo.InvariantCulture));
                }
            }
            catch (Exception)
            {
                return false;
            }

            return true;
        }

        /// <summary>解析带引号字符串；\uXXXX 转 UTF-16（含代理对，.NET string 天然是 UTF-16）。</summary>
        private bool ParseString(out string value)
        {
            value = string.Empty;
            if (Peek != '"')
            {
                return false;
            }

            _pos++;
            var sb = new StringBuilder();
            while (_pos < _s.Length)
            {
                char c = _s[_pos++];
                if (c == '"')
                {
                    value = sb.ToString();
                    return true;
                }

                if (c != '\\')
                {
                    sb.Append(c);
                    continue;
                }

                if (Eof)
                {
                    return false;
                }

                char esc = _s[_pos++];
                switch (esc)
                {
                    case '"': sb.Append('"'); break;
                    case '\\': sb.Append('\\'); break;
                    case '/': sb.Append('/'); break;
                    case 'b': sb.Append('\b'); break;
                    case 'f': sb.Append('\f'); break;
                    case 'n': sb.Append('\n'); break;
                    case 'r': sb.Append('\r'); break;
                    case 't': sb.Append('\t'); break;
                    case 'u':
                        if (!ParseHex4(out uint cp))
                        {
                            return false;
                        }

                        // 代理对处理
                        if (cp >= 0xD800 && cp <= 0xDBFF && _pos + 2 <= _s.Length &&
                            _s[_pos] == '\\' && _s[_pos + 1] == 'u')
                        {
                            int save = _pos;
                            _pos += 2;
                            if (ParseHex4(out uint lo) && lo >= 0xDC00 && lo <= 0xDFFF)
                            {
                                cp = 0x10000 + ((cp - 0xD800) << 10) + (lo - 0xDC00);
                                sb.Append(char.ConvertFromUtf32(unchecked((int)cp)));
                                break;
                            }

                            _pos = save;
                        }

                        if (cp is >= 0xD800 and <= 0xDFFF)
                        {
                            // 孤立代理：按原样放回（.NET string 允许）
                            sb.Append(unchecked((char)cp));
                        }
                        else
                        {
                            sb.Append((char)cp);
                        }

                        break;
                    default: return false;
                }
            }

            return false;
        }

        private bool ParseHex4(out uint cp)
        {
            cp = 0;
            if (_pos + 4 > _s.Length)
            {
                return false;
            }

            for (int i = 0; i < 4; i++)
            {
                char c = _s[_pos++];
                cp <<= 4;
                if (c >= '0' && c <= '9')
                {
                    cp |= (uint)(c - '0');
                }
                else if (c >= 'a' && c <= 'f')
                {
                    cp |= (uint)(c - 'a' + 10);
                }
                else if (c >= 'A' && c <= 'F')
                {
                    cp |= (uint)(c - 'A' + 10);
                }
                else
                {
                    return false;
                }
            }

            return true;
        }

        /// <summary>
        /// 宽容读取键名，覆盖 fastjson2 的两种非标准写法（对齐 Python _FastJsonParser._key）：
        ///   1) 无引号数字键：{"offsetTable":{0:1,1:2}}；
        ///   2) 对象/数组作为 map 键（非法 JSON，管理端必现）：MessageQueue 会被内联。
        /// 内联键保留**原始 JSON 文本**作为键名，调用方再用 ParseMessageQueueKey() 还原。
        /// </summary>
        private bool ParseKey(out string key)
        {
            key = string.Empty;
            SkipWs();
            char c = Peek;
            if (c == '"')
            {
                return ParseString(out key);
            }

            if (c == '{' || c == '[')
            {
                int start = _pos;
                if (!ParseValue(out JsonValue ignored))
                {
                    return false;
                }

                key = _s[start.._pos];
                return true;
            }

            // 无引号：读到冒号或空白前（数字键 / true / false / null 等裸字面量）
            int keyStart = _pos;
            while (_pos < _s.Length)
            {
                char k = _s[_pos];
                if (k == ':' || k == ' ' || k == '\t' || k == '\n' || k == '\r')
                {
                    break;
                }

                _pos++;
            }

            if (_pos == keyStart)
            {
                return false;
            }

            key = _s[keyStart.._pos];
            return true;
        }

        private bool ParseObject(out JsonValue value)
        {
            value = JsonValue.Null;
            if (Peek != '{')
            {
                return false;
            }

            _pos++;
            value = JsonValue.MakeObject();
            SkipWs();
            if (Peek == '}')
            {
                _pos++;
                return true;
            }

            while (true)
            {
                if (!ParseKey(out string key))
                {
                    return false;
                }

                SkipWs();
                if (Peek != ':')
                {
                    return false;
                }

                _pos++;
                if (!ParseValue(out JsonValue val))
                {
                    return false;
                }

                value.Set(key, val);
                SkipWs();
                char c = Peek;
                if (c == ',')
                {
                    _pos++;
                    SkipWs();
                    if (Peek == '}')
                    {
                        _pos++;
                        return true; // 容忍尾随逗号
                    }

                    continue;
                }

                if (c == '}')
                {
                    _pos++;
                    return true;
                }

                return false;
            }
        }

        private bool ParseArray(out JsonValue value)
        {
            value = JsonValue.Null;
            if (Peek != '[')
            {
                return false;
            }

            _pos++;
            value = JsonValue.MakeArray();
            SkipWs();
            if (Peek == ']')
            {
                _pos++;
                return true;
            }

            while (true)
            {
                if (!ParseValue(out JsonValue val))
                {
                    return false;
                }

                value.PushArray(val);
                SkipWs();
                char c = Peek;
                if (c == ',')
                {
                    _pos++;
                    SkipWs();
                    if (Peek == ']')
                    {
                        _pos++;
                        return true; // 容忍尾随逗号
                    }

                    continue;
                }

                if (c == ']')
                {
                    _pos++;
                    return true;
                }

                return false;
            }
        }
    }
}
