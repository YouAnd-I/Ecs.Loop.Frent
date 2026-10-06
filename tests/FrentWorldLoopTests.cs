using Frent;
using Xunit;

namespace Ecs.Loop.Frent.Tests;

public record struct Question(int Number);
public record struct Answer(int Number);
public record struct Alert(string Text);

public class FrentWorldLoopTests
{
    private static void DoubleSystem(World world)
    {
        foreach (var row in world.Query<Question>().EnumerateWithEntities<Question>())
        {
            var entity = row.Entity;
            entity.Add(new Answer(row.Item1.Value.Number * 2));
            entity.Remove<Question>();
        }
    }

    private static void DropSystem(World world)
    {
        foreach (var row in world.Query<Question>().EnumerateWithEntities<Question>())
        {
            var entity = row.Entity;
            entity.Delete();
        }
    }

    // Doubles the question like DoubleSystem and also "emits" an event:
    // a system emits by adding the notification struct as a component
    private static void AlertSystem(World world)
    {
        foreach (var row in world.Query<Question>().EnumerateWithEntities<Question>())
        {
            var entity = row.Entity;
            entity.Add(new Answer(row.Item1.Value.Number * 2));
            entity.Add(new Alert($"#{row.Item1.Value.Number}"));
            entity.Remove<Question>();
        }
    }

    [Fact]
    public async Task Ask_IsAnsweredOnTick_AndRequestIsDespawned()
    {
        using var world = new World();
        var loop = new FrentWorldLoop(world, DoubleSystem);

        var answer = loop.AskAsync<Question, Answer>(new Question(21));
        Assert.False(answer.IsCompleted);

        loop.Tick();

        Assert.Equal(new Answer(42), await answer);
        Assert.Equal(0, world.EntityCount);
    }

    [Fact]
    public async Task Ask_FromManyThreads_AllAnswered()
    {
        using var world = new World();
        var loop = new FrentWorldLoop(world, DoubleSystem);
        using var stop = new CancellationTokenSource();
        var running = loop.RunAsync(TimeSpan.FromMilliseconds(1), stop.Token);

        var answers = await Task.WhenAll(Enumerable.Range(0, 2_000).Select(i =>
            Task.Run(() => loop.AskAsync<Question, Answer>(new Question(i)))));

        stop.Cancel();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => running);
        Assert.Equal(Enumerable.Range(0, 2_000).Select(i => new Answer(i * 2)), answers);
        Assert.Equal(0, world.EntityCount);
    }

    [Fact]
    public async Task Ask_CallerGivesUp_RequestIsCleanedUp()
    {
        using var world = new World();
        var loop = new FrentWorldLoop(world);
        using var giveUp = new CancellationTokenSource();

        var answer = loop.AskAsync<Question, Answer>(new Question(1), giveUp.Token);
        loop.Tick();
        giveUp.Cancel();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => answer);
        loop.Tick();
        Assert.Equal(0, world.EntityCount);
    }

    [Fact]
    public async Task Ask_RequestDespawnedWithoutAnswer_IsCanceled()
    {
        using var world = new World();
        var loop = new FrentWorldLoop(world, DropSystem);

        var answer = loop.AskAsync<Question, Answer>(new Question(1));
        loop.Tick();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => answer);
    }

    [Fact]
    public async Task Notification_AddedBySystem_ReachesSubscriber()
    {
        using var world = new World();
        var loop = new FrentWorldLoop(world, AlertSystem);
        loop.AddNotificationDelivery<Alert>();

        var alerts = new List<Alert>();
        using var _ = loop.Subscribe<Alert>(a => { alerts.Add(a); return Task.CompletedTask; });

        var answer = loop.AskAsync<Question, Answer>(new Question(7));
        loop.Tick();
        await answer;
        await Task.Delay(100); // handlers run on the thread pool

        Assert.Equal([new Alert("#7")], alerts);
        Assert.Equal(0, world.EntityCount); // the notification component was consumed
    }

    [Fact]
    public async Task Notification_SubscriberUnsubscribed_StopsReceiving()
    {
        using var world = new World();
        var loop = new FrentWorldLoop(world, AlertSystem);
        loop.AddNotificationDelivery<Alert>();

        var alerts = new List<Alert>();
        var subscription = loop.Subscribe<Alert>(a => { alerts.Add(a); return Task.CompletedTask; });
        subscription.Dispose();

        var answer = loop.AskAsync<Question, Answer>(new Question(1));
        loop.Tick();
        await answer;
        await Task.Delay(100);

        Assert.Empty(alerts);
        Assert.Equal(0, world.EntityCount); // delivered to nobody, still cleaned up
    }

    [Fact]
    public async Task Notification_ThrowingHandler_DoesNotBreakLaterTicks()
    {
        using var world = new World();
        var loop = new FrentWorldLoop(world, AlertSystem);
        loop.AddNotificationDelivery<Alert>();

        var alerts = new List<Alert>();
        using var _ = loop.Subscribe<Alert>(a =>
        {
            alerts.Add(a);
            throw new InvalidOperationException("bad adapter");
        });

        var first = loop.AskAsync<Question, Answer>(new Question(1));
        loop.Tick();
        await first;
        await Task.Delay(100);
        Assert.Equal([new Alert("#1")], alerts);

        // The next tick still runs, and the handler is called again
        var second = loop.AskAsync<Question, Answer>(new Question(2));
        loop.Tick();
        await second;
        await Task.Delay(100);
        Assert.Equal([new Alert("#1"), new Alert("#2")], alerts);
    }
}
