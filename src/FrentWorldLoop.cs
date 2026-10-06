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
public sealed class FrentWorldLoop(World world, params Action<World>[] systems) : IWorldClient
{
    private readonly Channel<Action> _inbox = Channel.CreateUnbounded<Action>(new() { SingleReader = true });
    private readonly List<Func<bool>> _pendingReplies = [];

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

    // One frame: 1. apply inputs  2. run systems in order  3. deliver replies.
    // Call from one thread only: RunAsync in a host, or directly in tests.
    public void Tick()
    {
        while (_inbox.Reader.TryRead(out var input)) input();
        foreach (var system in systems) system(world);
        _pendingReplies.RemoveAll(tryDeliver => tryDeliver());
    }

    public async Task RunAsync(TimeSpan tickInterval, CancellationToken cancellationToken)
    {
        using var timer = new PeriodicTimer(tickInterval);
        while (await timer.WaitForNextTickAsync(cancellationToken)) Tick();
    }
}
