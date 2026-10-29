// Expression-type-aware SubscriptionData building for the PULL path (pull consumer and
// lite pull consumer). The push side already solved this in its selector subscribe; the
// pull side had to reproduce the same wire shape: a TAG selector must be byte-identical to
// what the plain string path produces today (tagsSet/codeSet/subString from the tag
// parser), while a non-TAG expression (SQL92, class filter) travels verbatim with its own
// expressionType -- hashing it as a tag list would corrupt the filter.
using System;

using RocketMQ.Common;

namespace RocketMQ.Client;

/// <summary>Turns a <see cref="MessageSelector"/> into the SubscriptionData the pull path
/// registers with the heartbeat and describes on the PULL/LITE_PULL request.</summary>
public static class SubscriptionSelectors
{
    /// <summary>Build the subscription for one selector-flavoured pull. An empty
    /// expression means "all messages" (<c>*</c>) for a TAG selector and an empty type
    /// means TAG, so callers never send a blank type. Any other language is passed through
    /// verbatim, so a blank expression at that type would ask the broker to filter with
    /// nothing at all -- it is refused instead of silently widening to every message.</summary>
    public static SubscriptionData Build(string topic, MessageSelector selector)
    {
        ArgumentNullException.ThrowIfNull(selector);
        string type = string.IsNullOrEmpty(selector.Type) ? ExpressionType.TAG : selector.Type;
        if (type == ExpressionType.TAG)
        {
            string expr = string.IsNullOrEmpty(selector.Expression) ? "*" : selector.Expression;
            // Identical to what the string-based pull path builds today.
            return FilterAPI.BuildSubscriptionData(topic, expr);
        }

        if (string.IsNullOrEmpty(selector.Expression))
        {
            throw new ArgumentException("Expression can't be null! " + type);
        }

        return new SubscriptionData(topic, selector.Expression)
        {
            ExpressionType = type,
            ClassFilterMode = type == ExpressionType.ClassFilter,
        };
    }
}
