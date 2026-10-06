using System.Threading.Channels;
using Ecs.Client;
using Frent;

namespace Ecs.Loop.Frent;

// The game loop: owns the World and is the only code that touches it.
//
// Frent's World is not thread safe (4 threads sharing one crash the process), so
// AskAsync only enqueues; everything else happens inside Tick, on one thread.
//
// Contract for systems: answer a request by adding the response component to the
// request's own entity and removing the request. The loop then hands the response
// to the waiting adapter and despawns the entity.
//
// Systems can also emit events for the outside world: they add a notification
// struct (plain data from a <Feature>.Data module) as a component on an entity.
// The composition root calls AddNotificationDelivery<T>() once, which appends a
// delivery pass to the end of the tick; it hands each notification to the
// subscribers registered through IWorldClient.Subscribe<T>().
public sealed class FrentWorldLoop(World world, params Action<World>[] systems) : IWorldClient
{
    private readonly Channel<Action> _inbox = Channel.CreateUnbounded<Action>(new() { SingleReader = true });
    private readonly List<Func<bool>> _pendingReplies = [];
    private readonly List<Action<World>> _systems = [.. systems];

    // Adapters subscribe at host startup, from any thread.
    private readonly object _subscriptionLock = new();
    private readonly Dictionary<Type, List<Delegate>> _subscribers = [];

    public Task<TResponse> AskAsync<TRequest, TResponse>(
        TRequest request,
        CancellationToken cancellationToken = default)
    {
        // Async continuations: the adapter's code after 'await' must never run on the loop thread
        var reply = new TaskCompletionSource<TResponse>(TaskCreationOptions.RunContinuationsAsynchronously);
        var registration = cancellationToken.Register(() => reply.TrySetCanceled(cancellationToken));

        _inbox.Writer.TryWrite(() =>
        {
            var entity = world.Create(request);
            _pendingReplies.Add(() =>
            {
                // A system despawned the request without answering it
                if (!entity.IsAlive) reply.TrySetCanceled();
                else if (entity.Has<TResponse>()) reply.TrySetResult(entity.Get<TResponse>());

                if (!reply.Task.IsCompleted) return false;
                registration.Dispose();
                if (entity.IsAlive) entity.Delete();
                return true;
            });
        });

        return reply.Task;
    }

    public IDisposable Subscribe<TNotification>(Func<TNotification, Task> handler)
    {
        lock (_subscriptionLock)
        {
            if (!_subscribers.TryGetValue(typeof(TNotification), out var list))
                _subscribers[typeof(TNotification)] = list = [];
            list.Add(handler);
        }
        return new Subscription(this, typeof(TNotification), handler);
    }

    // Not on IWorldClient: only the composition root and the delivery pass emit notifications.
    // Dispatches on the thread pool; a throwing handler is logged and can never kill a tick.
    public void Publish<TNotification>(TNotification notification)
    {
        Delegate[] handlers;
        lock (_subscriptionLock)
        {
            if (!_subscribers.TryGetValue(typeof(TNotification), out var list) || list.Count == 0) return;
            handlers = [.. list];
        }

        foreach (var handler in handlers.Cast<Func<TNotification, Task>>())
        {
            _ = Task.Run(async () =>
            {
                try { await handler(notification); }
                catch (Exception ex)
                {
                    Console.WriteLine($"[world] notification handler failed: {ex.GetType().Name}: {ex.Message}");
                }
            });
        }
    }

    // Call once after construction, before the loop runs. Appends a delivery pass to the
    // end of the tick: every TNotification component added by a system is removed from its
    // entity and published to the subscribers. This is how a system "emits" an event.
    public void AddNotificationDelivery<TNotification>() =>
        _systems.Add(deliveryWorld =>
        {
            foreach (var row in deliveryWorld
                         .Query<TNotification>()
                         .EnumerateWithEntities<TNotification>())
            {
                var entity = row.Entity;
                var notification = row.Item1.Value;
                entity.Remove<TNotification>();
                Publish(notification);
            }
        });

    // One frame: 1. apply inputs  2. run systems in order  3. deliver replies.
    // Call from one thread only: RunAsync in a host, or directly in tests.
    public void Tick()
    {
        while (_inbox.Reader.TryRead(out var input)) input();
        foreach (var system in _systems) system(world);
        _pendingReplies.RemoveAll(tryDeliver => tryDeliver());
    }

    public async Task RunAsync(TimeSpan tickInterval, CancellationToken cancellationToken)
    {
        using var timer = new PeriodicTimer(tickInterval);
        while (await timer.WaitForNextTickAsync(cancellationToken)) Tick();
    }

    private sealed class Subscription(FrentWorldLoop loop, Type type, Delegate handler) : IDisposable
    {
        public void Dispose()
        {
            lock (loop._subscriptionLock)
            {
                if (!loop._subscribers.TryGetValue(type, out var list)) return;
                list.Remove(handler);
                if (list.Count == 0) loop._subscribers.Remove(type);
            }
        }
    }
}
