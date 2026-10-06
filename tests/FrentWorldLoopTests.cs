using Frent;
using Xunit;

namespace Ecs.Loop.Frent.Tests;

public record struct Question(int Number);
public record struct Answer(int Number);

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
}
