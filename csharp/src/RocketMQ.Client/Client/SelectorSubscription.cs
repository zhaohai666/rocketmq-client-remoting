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
    /// expression means "all messages" (<c>*</c>) and an empty type means TAG, so callers
    /// never send a blank expression/type pair.</summary>
    public static SubscriptionData Build(string topic, MessageSelector selector)
    {
        ArgumentNullException.ThrowIfNull(selector);
        string expr = string.IsNullOrEmpty(selector.Expression) ? "*" : selector.Expression;
        string type = string.IsNullOrEmpty(selector.Type) ? ExpressionType.TAG : selector.Type;
        if (type == ExpressionType.TAG)
        {
            // Identical to what the string-based pull path builds today.
            return FilterAPI.BuildSubscriptionData(topic, expr);
        }

        return new SubscriptionData(topic, expr)
        {
            ExpressionType = type,
            ClassFilterMode = type == ExpressionType.ClassFilter,
        };
    }
}
