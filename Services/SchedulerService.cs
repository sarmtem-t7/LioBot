using System.Text.RegularExpressions;
using LioBot.Data;
using LioBot.Models;
using LioBot.Services;
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
        var dow        = DateTime.Now.DayOfWeek;
        var isMonday   = dow == DayOfWeek.Monday;

        string? genericMessage = null;
        var articleOfDay = PickArticleOfTheDay(dayOfYear);
        string? sundayWord = dow == DayOfWeek.Sunday ? await GenerateSundayWordAsync(dayOfYear) : null;

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

                if (!string.IsNullOrEmpty(sundayWord))
                    message = message.TrimEnd() + "\n\n⛪ <b>Слово перед служением:</b>\n" + sundayWord;

                var greetingMsg = await _bot.SendMessage(user.TelegramId, message,
                    parseMode: ParseMode.Html,
                    linkPreviewOptions: new Telegram.Bot.Types.LinkPreviewOptions { IsDisabled = true });
                BotMessageTracker.Track(user.TelegramId, greetingMsg.MessageId);

                if (articleOfDay != null)
                {
                    await Task.Delay(200);
                    var (cardText, cardKeyboard) = BuildArticleCard(articleOfDay);
                    var articleMsg = await _bot.SendMessage(user.TelegramId,
                        "📰 <b>Статья дня:</b>\n\n" + cardText,
                        parseMode: ParseMode.Html,
                        replyMarkup: cardKeyboard,
                        linkPreviewOptions: new Telegram.Bot.Types.LinkPreviewOptions { IsDisabled = true });
                    BotMessageTracker.Track(user.TelegramId, articleMsg.MessageId);
                }

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

    private Book? PickArticleOfTheDay(int dayOfYear)
    {
        var items = _db.GetByType("article");
        return items.Count == 0 ? null : items[dayOfYear % items.Count];
    }

    private static (string Text, InlineKeyboardMarkup Keyboard) BuildArticleCard(Book article)
    {
        var text = BookService.FormatBookCard(article);

        var rows = new List<InlineKeyboardButton[]>();
        if (!string.IsNullOrEmpty(article.Url))
            rows.Add([InlineKeyboardButton.WithUrl("📰 Читать статью", article.Url)]);
        rows.Add([InlineKeyboardButton.WithCallbackData("🏠 На главную", "menu:back")]);

        return (text, new InlineKeyboardMarkup(rows));
    }

    private static readonly string[] FallbackSundayWords =
    {
        "«Как хорошо и как приятно жить братьям вместе!» (Псалом 132:1). Войдите в дом Господень с открытым сердцем — пусть сегодняшнее служение станет живой встречей с Богом.",
        "Сегодня день собрания. Приходите не как зрители, но как участники — Господь посреди вас. Благословенного воскресного служения!",
        "«Не будем оставлять собрания своего» (Евреям 10:25). Каждое воскресенье — это дар. Пусть сегодня Его слово коснётся вашего сердца.",
        "Воскресное служение — это не ритуал, это встреча. Приходите с ожиданием, и Господь не оставит вас без ответа.",
    };

    private async Task<string> GenerateSundayWordAsync(int dayOfYear)
    {
        var system = """
            Ты — тёплый помощник христианского книжного клуба LioBot.
            Сегодня воскресенье. Напиши короткое слово наставления перед воскресным служением.
            Правила:
            1. Пиши только на русском языке.
            2. Тон — тёплый, пасторский, вдохновляющий.
            3. Включи 1 библейский стих по Синодальному переводу, связанный с поклонением, собранием или служением.
               Ссылку оформляй в круглых скобках: «текст» (Книга Глава:Стих).
            4. Максимум 3-4 предложения. Не начинай с "Доброе утро!".
            5. Пожелай благословенного воскресного служения.
            """;
        try
        {
            return await _claude.AskAsync(system,
                $"День года: {dayOfYear}. Напиши уникальное слово наставления перед воскресным служением.", maxTokens: 300);
        }
        catch
        {
            return FallbackSundayWords[dayOfYear % FallbackSundayWords.Length];
        }
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

/// <summary>
/// Каждую ночь импортирует свежий контент с lio-int.com и lio-blog.com,
/// затем шлёт сводку всем админам в Telegram.
/// </summary>
[DisallowConcurrentExecution]
public class DailyImportJob : IJob
{
    private readonly ContentImportService _importer;
    private readonly ITelegramBotClient _bot;
    private readonly IConfiguration _config;
    private readonly ILogger<DailyImportJob> _logger;

    public DailyImportJob(
        ContentImportService importer,
        ITelegramBotClient bot,
        IConfiguration config,
        ILogger<DailyImportJob> logger)
    {
        _importer = importer;
        _bot = bot;
        _config = config;
        _logger = logger;
    }

    public async Task Execute(IJobExecutionContext context)
    {
        var ct = context.CancellationToken;
        _logger.LogInformation("[DailyImport] Запуск ночного автоимпорта…");
        try
        {
            var summary = await _importer.ImportAllAsync(ct);
            _logger.LogInformation("[DailyImport] Готово: {Total} новых единиц", summary.Total);
            if (summary.Total > 0)
                await NotifyAdminsAsync(summary, ct);
        }
        catch (InvalidOperationException)
        {
            _logger.LogInformation("[DailyImport] Уже выполняется — пропуск");
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "[DailyImport] Ошибка автоимпорта");
            await NotifyAdminsErrorAsync(ex, ct);
        }
    }

    private async Task NotifyAdminsAsync(ImportSummary s, CancellationToken ct)
    {
        var lines = new List<string> { "<b>🌙 Ночной автоимпорт завершён</b>", "" };
        if (s.Audio    > 0) lines.Add($"🎧 Аудиокниги: <b>{s.Audio}</b>");
        if (s.Articles > 0) lines.Add($"📰 Статьи: <b>{s.Articles}</b>");
        if (s.Radio    > 0) lines.Add($"🎙 Радио: <b>{s.Radio}</b>");
        if (s.Magazines> 0) lines.Add($"📖 Журналы: <b>{s.Magazines}</b>");
        if (s.Issues   > 0) lines.Add($"📰 Выпуски: <b>{s.Issues}</b>");
        lines.Add("");
        lines.Add($"Итого новых: <b>{s.Total}</b>");
        var text = string.Join("\n", lines);
        foreach (var id in ParseAdminIds())
        {
            try { await _bot.SendMessage(id, text, parseMode: ParseMode.Html, cancellationToken: ct); }
            catch (Exception ex) { _logger.LogWarning("[DailyImport] Не доставлено {Id}: {Err}", id, ex.Message); }
        }
    }

    private async Task NotifyAdminsErrorAsync(Exception ex, CancellationToken ct)
    {
        var text = $"⚠️ Автоимпорт упал: <code>{System.Net.WebUtility.HtmlEncode(ex.Message)}</code>";
        foreach (var id in ParseAdminIds())
        {
            try { await _bot.SendMessage(id, text, parseMode: ParseMode.Html, cancellationToken: ct); }
            catch { /* ignore */ }
        }
    }

    private IEnumerable<long> ParseAdminIds() =>
        (_config["AdminIds"] ?? "")
            .Split(',', StringSplitOptions.RemoveEmptyEntries)
            .Select(s => long.TryParse(s.Trim(), out var id) ? id : 0L)
            .Where(id => id != 0);
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

        // Ночной автоимпорт — каждый день в 04:00
        var importKey = new JobKey("DailyImportJob");
        quartz.AddJob<DailyImportJob>(opts => opts.WithIdentity(importKey));
        quartz.AddTrigger(opts => opts
            .ForJob(importKey)
            .WithIdentity("DailyImportTrigger")
            .WithCronSchedule("0 0 4 * * ?", x => x.InTimeZone(tz))
        );
    }

    private static TimeZoneInfo GetTimeZone(string id)
    {
        try { return TimeZoneInfo.FindSystemTimeZoneById(id); }
        catch { return TimeZoneInfo.Utc; }
    }
}
