using System.Threading.Channels;
using Ecs.Client;
using Frent;

namespace Ecs.Loop.Frent;

public sealed class FrentWorldLoop(World world, params Action<World>[] systems) : IWorldClient
{
    private readonly Channel<Action> _inbox = Channel.CreateUnbounded<Action>(new() { SingleReader = true });
    private readonly List<Func<bool>> _pendingReplies = [];
    private readonly List<Action<World>> _systems = [.. systems];

    private readonly object _subscriptionLock = new();
    private readonly Dictionary<Type, List<Delegate>> _subscribers = [];

    public Task<TResponse> AskAsync<TRequest, TResponse>(
        TRequest request,
        CancellationToken cancellationToken = default)
    {
        var reply = new TaskCompletionSource<TResponse>(TaskCreationOptions.RunContinuationsAsynchronously);
        var registration = cancellationToken.Register(() => reply.TrySetCanceled(cancellationToken));

        _inbox.Writer.TryWrite(() =>
        {
            var entity = world.Create(request);
            _pendingReplies.Add(() =>
            {
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
