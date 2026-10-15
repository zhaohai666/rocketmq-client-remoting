// The reference-count decision must be atomic ("decrement to 0 -> remove from table -> decide shutdown" must not interleave
// with a concurrent getOrCreate: otherwise one side just got the instance while the other already shut it down), and register/unregister
// are low-frequency lifecycle calls, so we simply guard the whole table with one lock instead of
// ConcurrentDictionary + scattered Interlocked — the latter splits "check 0" and "remove from table" apart,
// then needs a second lock to stitch them back together.
using RocketMQ.Common;

namespace RocketMQ.Client;

public sealed class MQClientManager
{
    private static readonly MQClientManager Singleton = new();

    public static MQClientManager Instance => Singleton;

    private readonly object _gate = new();
    private readonly Dictionary<string, Entry> _factoryTable = new(StringComparer.Ordinal);

    private sealed class Entry(MQClientInstance instance)
    {
        internal MQClientInstance Instance { get; } = instance;
        internal int RefCount;
    }

    private MQClientManager()
    {
    }

    /// After returning, the caller must call <see cref="MQClientInstance.Start"/> itself (calling Start again on an already-started
    /// shared instance is idempotent: periodic tasks all have a "don't start if already running" guard), and return it on Shutdown via
    /// <see cref="UnregisterMQClient"/>.
    /// </summary>
    public MQClientInstance GetOrCreateMQClientInstance(string clientId, Func<MQClientInstance> factory)
    {
        if (string.IsNullOrEmpty(clientId))
        {
            throw new MQClientException("clientId is empty");
        }

        lock (_gate)
        {
            if (!_factoryTable.TryGetValue(clientId, out Entry? entry))
            {
                MQClientInstance created = factory()
                    ?? throw new MQClientException("MQClientInstance factory returned null");
                entry = new Entry(created);
                _factoryTable[clientId] = entry;
                ClientLog.Info("Created new MQClientInstance for clientId:[" + clientId + "]");
            }

            entry.RefCount++;
            return entry.Instance;
        }
    }

    /// Returns true if this call actually triggered (or will trigger in the future) instance teardown: when the count has not
    /// reached zero the instance stays alive; false does not mean the instance was shut down.
    /// </summary>
    public bool UnregisterMQClient(string clientId, MQClientInstance? instance)
    {
        if (instance is null)
        {
            return false;
        }

        bool last;
        lock (_gate)
        {
            if (!_factoryTable.TryGetValue(clientId, out Entry? entry)
                || !ReferenceEquals(entry.Instance, instance))
            {
                return false;
            }

            last = --entry.RefCount <= 0;
            if (last)
            {
                _factoryTable.Remove(clientId);
            }
        }

        if (last)
        {
            // Shut down outside the lock: Shutdown must join the periodic threads (each with a 2s budget); holding the table lock would block
            // concurrent getOrCreate. The table entry is already removed now, so later callers only get a fresh instance.
            ClientLog.Info("the client factory [" + clientId + "] shutdown OK, refcount=0");
            instance.Shutdown();
        }

        return true;
    }

    // ---------------- Diagnostics / test observability surface ----------------

    /// <summary>Number of currently alive instances (those whose reference count reached zero are removed).</summary>
    public int InstanceCount
    {
        get
        {
            lock (_gate)
            {
                return _factoryTable.Count;
            }
        }
    }

    /// <summary>How many facades are currently using a given clientId; returns 0 if not in the table.</summary>
    public int RefCountOf(string clientId)
    {
        lock (_gate)
        {
            return _factoryTable.TryGetValue(clientId, out Entry? entry) ? entry.RefCount : 0;
        }
    }

    /// <summary>Take the current instance from the table (for diagnostics; returns false when nobody is using it).</summary>
    public bool TryGetMQClientInstance(string clientId, out MQClientInstance? instance)
    {
        lock (_gate)
        {
            if (_factoryTable.TryGetValue(clientId, out Entry? entry))
            {
                instance = entry.Instance;
                return true;
            }
        }

        instance = null;
        return false;
    }
}
