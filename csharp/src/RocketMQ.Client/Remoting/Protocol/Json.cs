// Minimal JSON parse/generate (only what RemotingSerializable needs, zero third-party
// dependencies).
//
// Why a hand-rolled implementation instead of System.Text.Json: the RocketMQ server
// (fastjson2) writes **invalid JSON** that standard parsers all reject, and many admin
// bodies are exactly these shapes:
//   - object keys for maps: {"offsetTable":{{"brokerName":"b","queueId":3,"topic":"t"}:{...}}}
//   - bare numeric keys for maps: {"brokerAddrs":{0:"127.0.0.1"}}
//   - NaN / Infinity / -Infinity literals
//   - trailing commas
// Also, int64 and double must be distinguished: if a long timestamp were written as a float,
// the broker-side field would not line up.
//
// Implemented item by item per the protocol (the server's fastjson2 output shape dictates the
// behavior below):
//   - Objects keep order: set() overwriting an existing key **keeps its original position**
//     (insertion order; overwriting does not reorder);
//   - Strings are output verbatim as UTF-8 (not decoded to \uXXXX); parsing accepts both
//     \uXXXX and surrogate pairs;
//   - Parsing is lenient toward fastjson2's non-standard forms.
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

    /// <summary>Shared Null instance (read-only, do not Set). For chained value fallback.</summary>
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
    /// <summary>Order-preserving key/value view (for iteration; order matches insertion/parse order).</summary>
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

    /// <summary>Returns the shared Null when not found, for chained access.</summary>
    public JsonValue Get(string key) => Find(key) ?? NullInstance;

    public void Set(string key, JsonValue v)
    {
        _type = JsonType.Object;
        for (int i = 0; i < _obj.Count; i++)
        {
            if (_obj[i].Key == key)
            {
                // Overwrite-style insert: keep the original position
                _obj[i] = new KeyValuePair<string, JsonValue>(key, v);
                return;
            }
        }

        _obj.Add(new KeyValuePair<string, JsonValue>(key, v));
    }

    // Convenience getters: return true only when the key exists and the type matches
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
                    // double that happens to be integral is output as an integer, so long
                    // fields never receive a float shape
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
                        // Non-ASCII is output directly as a UTF-16 char (C# string is UTF-16;
                        // writing files / sending over the network uniformly uses UTF-8
                        // encoding, preserving the original text with no ASCII escaping)
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
            sb.Append("null"); // JSON has no NaN/Inf
            return;
        }

        // G17 is equivalent to C's %.17g (17 significant digits, lossless round-trip)
        string s = v.ToString("G17", CultureInfo.InvariantCulture);
        if (s.IndexOfAny(new[] { '.', 'e', 'E' }) < 0)
        {
            s += ".0"; // ensure a decimal/exponent point so it is not parsed as an integer
        }

        sb.Append(s);
    }
}

public static class Json
{
    /// <summary>
    /// Returns false on parse failure, with the error reason written through the optional out
    /// parameter. Lenient toward fastjson2's non-standard forms; see the class comment.
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

    /// <summary>Throwing version of parse failure (internal convenience method).</summary>
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
                // fastjson2 writes these three non-standard literals when
                // WriteMapNullValue=false / floating point goes out of range; standard JSON
                // has none of them.
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

        /// <summary>Parses a quoted string; \uXXXX to UTF-16 (including surrogate pairs; a C# string is naturally UTF-16).</summary>
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

                        // surrogate pair handling
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
                            // lone surrogate: put it back as-is (C# string allows it)
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
        /// Lenient key parsing, covering fastjson2's two non-standard forms:
        ///   1) unquoted numeric keys: {"offsetTable":{0:1,1:2}};
        ///   2) objects/arrays as map keys (invalid JSON, always present in admin bodies):
        ///      MessageQueue is inlined.
        /// Inline keys keep the **original JSON text** as the key name; callers restore it via
        /// ParseMessageQueueKey().
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

            // unquoted: read until a colon or whitespace (numeric keys / true / false / null
            // and other bare literals)
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
                        return true; // tolerate a trailing comma
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
                        return true; // tolerate a trailing comma
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
