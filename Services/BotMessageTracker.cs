using System.Collections.Concurrent;

namespace LioBot.Services;

/// <summary>
/// Хранит ID всех сообщений бота по каждому чату.
/// Позволяет удалять их разом при новом действии пользователя.
/// </summary>
public static class BotMessageTracker
{
    private static readonly ConcurrentDictionary<long, List<int>> _messages = new();

    /// <summary>
    /// Опциональный персистор последнего ID — выставляется в Program.cs,
    /// чтобы пережить рестарты процесса (нужно для утренней рассылки).
    /// </summary>
    public static Action<long, int>? OnPersistLast { get; set; }

    public static void Track(long chatId, int messageId)
    {
        _messages.AddOrUpdate(
            chatId,
            _ => new List<int> { messageId },
            (_, list) => { lock (list) { list.Add(messageId); } return list; });
        try { OnPersistLast?.Invoke(chatId, messageId); } catch { /* not critical */ }
    }

    public static void SetCurrent(long chatId, int messageId)
    {
        _messages[chatId] = new List<int> { messageId };
        try { OnPersistLast?.Invoke(chatId, messageId); } catch { /* not critical */ }
    }

    public static int[] TakeAll(long chatId)
    {
        if (!_messages.TryRemove(chatId, out var list)) return [];
        lock (list) return list.ToArray();
    }
}
