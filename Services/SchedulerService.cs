using System.Text.RegularExpressions;
using LioBot.Data;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Quartz;
using Telegram.Bot;
using Telegram.Bot.Exceptions;
using Telegram.Bot.Types.Enums;
using Telegram.Bot.Types.ReplyMarkups;

namespace LioBot.Services;

/// <summary>
/// Quartz-задача: каждое утро рассылает вдохновляющее сообщение.
/// Уважает настройку NotifyMode пользователя: daily | weekly (пн) | off.
/// </summary>
[DisallowConcurrentExecution]
public class MorningMessageJob : IJob
{
    private readonly DatabaseContext _db;
    private readonly ClaudeService _claude;
    private readonly ITelegramBotClient _bot;
    private readonly ILogger<MorningMessageJob> _logger;

    // Пул fallback-стихов для случая, когда AI недоступен
    private static readonly (string Text, string Ref)[] FallbackVerses =
    {
        ("Уповай на Господа всем сердцем твоим и не полагайся на разум твой.", "Притчи 3:5"),
        ("Всё могу в укрепляющем меня Иисусе Христе.",                         "Филиппийцам 4:13"),
        ("Господь — Пастырь мой; я ни в чём не буду нуждаться.",               "Псалом 22:1"),
        ("Не бойся, ибо Я с тобою; не смущайся, ибо Я Бог твой.",              "Исаия 41:10"),
        ("Просите, и дано будет вам; ищите, и найдёте.",                       "Матфея 7:7"),
        ("Господь — крепость жизни моей: кого мне страшиться?",                "Псалом 26:1"),
        ("Блажен муж, который переносит искушение.",                           "Иакова 1:12")
    };

    public MorningMessageJob(
        DatabaseContext db,
        ClaudeService claude,
        ITelegramBotClient bot,
        ILogger<MorningMessageJob> logger)
    {
        _db = db;
        _claude = claude;
        _bot = bot;
        _logger = logger;
    }

    public async Task Execute(IJobExecutionContext context)
    {
        _logger.LogInformation("[Scheduler] Запуск утренней рассылки...");

        var today      = DateTime.Now.ToString("dd MMMM yyyy", new System.Globalization.CultureInfo("ru-RU"));
        var dayOfYear  = DateTime.Now.DayOfYear;
        var isMonday   = DateTime.Now.DayOfWeek == DayOfWeek.Monday;

        string? genericMessage = null;

        var users = _db.GetAllUsers();
        _logger.LogInformation("[Scheduler] Всего пользователей: {Count}", users.Count);

        var sent = 0;
        var blocked = 0;
        foreach (var user in users)
        {
            if (user.NotifyMode == "off") continue;
            if (user.NotifyMode == "weekly" && !isMonday) continue;

            try
            {
                var history = _db.GetHistory(user.TelegramId, limit: 6);
                var userMessages = history
                    .Where(h => h.Role == "user")
                    .Select(h => h.Content)
                    .ToList();

                string message;
                if (userMessages.Count >= 2)
                {
                    var avoid = _db.GetRecentSentVerses(user.TelegramId, limit: 30);
                    message = await GeneratePersonalizedMessageAsync(today, dayOfYear, user.FirstName, userMessages, avoid);
                }
                else
                {
                    genericMessage ??= await GenerateGenericMessageAsync(today, dayOfYear);
                    message = genericMessage;
                }

                await _bot.SendMessage(user.TelegramId, message,
                    linkPreviewOptions: new Telegram.Bot.Types.LinkPreviewOptions { IsDisabled = true });

                var verseRef = ExtractVerseRef(message);
                if (!string.IsNullOrEmpty(verseRef))
                    _db.RecordVerseSent(user.TelegramId, verseRef);

                sent++;
                await Task.Delay(350);
            }
            catch (ApiRequestException apiEx) when (
                apiEx.ErrorCode == 403 ||
                apiEx.Message.Contains("blocked", StringComparison.OrdinalIgnoreCase) ||
                apiEx.Message.Contains("deactivated", StringComparison.OrdinalIgnoreCase))
            {
                _db.SetNotifyMode(user.TelegramId, "off");
                blocked++;
                _logger.LogInformation("[Scheduler] Пользователь {Id} заблокировал бота — отключаю рассылку", user.TelegramId);
            }
            catch (Exception ex)
            {
                _logger.LogWarning("[Scheduler] Не удалось отправить {UserId}: {Error}", user.TelegramId, ex.Message);
            }
        }

        _logger.LogInformation("[Scheduler] Рассылка завершена. Отправлено: {Sent}, заблокировано: {Blocked}", sent, blocked);
    }

    private string PickFallbackVerse(int dayOfYear, string firstName)
    {
        var (text, reference) = FallbackVerses[dayOfYear % FallbackVerses.Length];
        var greeting = string.IsNullOrEmpty(firstName) ? "Благословенного дня! 🙏" : $"Благословенного дня, {firstName}! 🙏";
        return $"{greeting}\n\n«{text}» ({reference})";
    }

    private async Task<string> GenerateGenericMessageAsync(string today, int dayOfYear)
    {
        var system = """
            Ты — тёплый помощник христианского книжного клуба LioBot.
            Твоя задача — каждое утро отправлять короткое, живое и вдохновляющее сообщение.
            Правила:
            1. Пиши только на русском языке.
            2. Тон — тёплый, как у заботливого пастора или друга.
            3. Включи 1 библейский стих по Синодальному переводу — дословно — и 2-3 предложения вдохновения.
               Ссылку оформляй в круглых скобках в конце цитаты: «текст стиха» (Книга Глава:Стих).
            4. Не повторяй стихи из прошлых сообщений.
            5. Максимум 5 предложений. Не начинай с "Доброе утро!".
            6. Ты бот — не пиши "я молюсь за вас". Вместо этого пожелай Божьего благословения.
            """;
        try
        {
            return await _claude.AskAsync(system,
                $"Сегодня {today}. День года: {dayOfYear}. Напиши уникальное утреннее вдохновение.", maxTokens: 400);
        }
        catch
        {
            return PickFallbackVerse(dayOfYear, "");
        }
    }

    private async Task<string> GeneratePersonalizedMessageAsync(
        string today, int dayOfYear, string firstName, List<string> recentTopics, List<string> avoidVerses)
    {
        var topics = string.Join("; ", recentTopics.TakeLast(3));
        var avoidLine = avoidVerses.Count > 0
            ? $"\n7. Не используй эти стихи (были отправлены ранее): {string.Join("; ", avoidVerses)}."
            : "";
        var system = $"""
            Ты — тёплый помощник христианского книжного клуба LioBot.
            Отправь {firstName} персонализированное утреннее вдохновение.
            Правила:
            1. Пиши на русском, обращайся по имени.
            2. Тон — тёплый, как у заботливого пастора.
            3. Включи 1 библейский стих по Синодальному переводу — дословно — связанный с темами из истории.
               Ссылку оформляй в круглых скобках в конце цитаты: «текст стиха» (Книга Глава:Стих).
            4. 1-2 предложения, перекликающиеся с тем, что человека занимало.
            5. Максимум 5 предложений. Не начинай с "Доброе утро!".
            6. Не пиши "я молюсь за тебя". Пожелай Божьего благословения.{avoidLine}
            """;
        try
        {
            return await _claude.AskAsync(system,
                $"Сегодня {today}. День года: {dayOfYear}. Недавние темы: {topics}", maxTokens: 450);
        }
        catch
        {
            return PickFallbackVerse(dayOfYear, firstName);
        }
    }

    // Извлекает ссылку на стих из сообщения: ищет скобочную конструкцию вида
    // (Книга Глава:Стих) или (1 Коринфянам 13:4-7)
    private static readonly Regex VerseRefRegex =
        new(@"\(\s*((?:\d+\s)?[А-ЯЁA-Zа-яёa-z]+\s+\d+[:\.]\d+(?:[-–]\d+)?)\s*\)", RegexOptions.Compiled);

    private static string ExtractVerseRef(string message)
    {
        var match = VerseRefRegex.Match(message ?? "");
        return match.Success ? match.Groups[1].Value.Trim() : string.Empty;
    }
}

/// <summary>
/// Ежедневная чистка устаревших записей SeenBooks (> 14 дней).
/// </summary>
[DisallowConcurrentExecution]
public class CleanupJob : IJob
{
    private readonly DatabaseContext _db;
    private readonly ILogger<CleanupJob> _logger;

    public CleanupJob(DatabaseContext db, ILogger<CleanupJob> logger)
    {
        _db = db;
        _logger = logger;
    }

    public Task Execute(IJobExecutionContext context)
    {
        var removed = _db.CleanOldSeenBooks(TimeSpan.FromDays(14));
        if (removed > 0)
            _logger.LogInformation("[Cleanup] Удалено {Count} старых записей SeenBooks", removed);
        return Task.CompletedTask;
    }
}

/// <summary>
/// Мягкое напоминание пользователям, которые отметили «читаю», но не завершили за 7 дней.
/// </summary>
[DisallowConcurrentExecution]
public class ReadingReminderJob : IJob
{
    private readonly DatabaseContext _db;
    private readonly ITelegramBotClient _bot;
    private readonly ILogger<ReadingReminderJob> _logger;

    public ReadingReminderJob(DatabaseContext db, ITelegramBotClient bot, ILogger<ReadingReminderJob> logger)
    {
        _db = db;
        _bot = bot;
        _logger = logger;
    }

    public async Task Execute(IJobExecutionContext context)
    {
        var stale = _db.GetStaleReading(TimeSpan.FromDays(7));
        if (stale.Count == 0) return;

        _logger.LogInformation("[ReadingReminder] К напоминанию: {Count}", stale.Count);

        foreach (var (telegramId, bookId, title, _) in stale)
        {
            try
            {
                var keyboard = new InlineKeyboardMarkup(new[]
                {
                    new[]
                    {
                        InlineKeyboardButton.WithCallbackData("✅ Прочитал", $"book:read:{bookId}"),
                        InlineKeyboardButton.WithCallbackData("⏸ Отложить", $"book:stopreading:{bookId}")
                    },
                    new[] { InlineKeyboardButton.WithCallbackData("📖 Открыть карточку", $"book:card:{bookId}") }
                });

                await _bot.SendMessage(telegramId,
                    $"📖 Как продвигается книга «{title}»? Если завершил — отметь, или можно отложить.",
                    parseMode: ParseMode.Html,
                    replyMarkup: keyboard);

                _db.TouchReadingReminded(telegramId, bookId);
                await Task.Delay(200);
            }
            catch (ApiRequestException apiEx) when (apiEx.ErrorCode == 403)
            {
                _db.StopReading(telegramId, bookId);
                _db.SetNotifyMode(telegramId, "off");
            }
            catch (Exception ex)
            {
                _logger.LogWarning("[ReadingReminder] {Id}: {Err}", telegramId, ex.Message);
            }
        }
    }
}

public static class SchedulerConfigurator
{
    public static void AddMorningSchedule(this IServiceCollectionQuartzConfigurator quartz, IConfiguration config)
    {
        var timeStr = config["ScheduleTime"] ?? "08:00";
        var parts  = timeStr.Split(':');
        var hour   = int.Parse(parts[0]);
        var minute = int.Parse(parts[1]);
        var tz     = GetTimeZone(config["TimeZone"] ?? "Russian Standard Time");

        var morningKey = new JobKey("MorningMessageJob");
        quartz.AddJob<MorningMessageJob>(opts => opts.WithIdentity(morningKey));
        quartz.AddTrigger(opts => opts
            .ForJob(morningKey)
            .WithIdentity("MorningTrigger")
            .WithCronSchedule($"0 {minute} {hour} * * ?", x => x.InTimeZone(tz))
        );

        // Чистка — каждый день в 03:30
        var cleanupKey = new JobKey("CleanupJob");
        quartz.AddJob<CleanupJob>(opts => opts.WithIdentity(cleanupKey));
        quartz.AddTrigger(opts => opts
            .ForJob(cleanupKey)
            .WithIdentity("CleanupTrigger")
            .WithCronSchedule("0 30 3 * * ?", x => x.InTimeZone(tz))
        );

        // Напоминания читающим — каждый день в 19:00
        var reminderKey = new JobKey("ReadingReminderJob");
        quartz.AddJob<ReadingReminderJob>(opts => opts.WithIdentity(reminderKey));
        quartz.AddTrigger(opts => opts
            .ForJob(reminderKey)
            .WithIdentity("ReadingReminderTrigger")
            .WithCronSchedule("0 0 19 * * ?", x => x.InTimeZone(tz))
        );
    }

    private static TimeZoneInfo GetTimeZone(string id)
    {
        try { return TimeZoneInfo.FindSystemTimeZoneById(id); }
        catch { return TimeZoneInfo.Utc; }
    }
}
