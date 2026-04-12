using LioBot.Data;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Quartz;
using Telegram.Bot;

namespace LioBot.Services;

/// <summary>
/// Quartz-задача: каждое утро рассылает вдохновляющее сообщение.
/// Уважает настройку NotifyMode пользователя: daily | weekly (пн) | off.
/// Пользователи с историей получают персонализированное сообщение.
/// </summary>
[DisallowConcurrentExecution]
public class MorningMessageJob : IJob
{
    private readonly DatabaseContext _db;
    private readonly GroqService _groq;
    private readonly ITelegramBotClient _bot;
    private readonly ILogger<MorningMessageJob> _logger;

    public MorningMessageJob(
        DatabaseContext db,
        GroqService groq,
        ITelegramBotClient bot,
        ILogger<MorningMessageJob> logger)
    {
        _db = db;
        _groq = groq;
        _bot = bot;
        _logger = logger;
    }

    public async Task Execute(IJobExecutionContext context)
    {
        _logger.LogInformation("[Scheduler] Запуск утренней рассылки...");

        var today      = DateTime.Now.ToString("dd MMMM yyyy", new System.Globalization.CultureInfo("ru-RU"));
        var dayOfYear  = DateTime.Now.DayOfYear;
        var isMonday   = DateTime.Now.DayOfWeek == DayOfWeek.Monday;

        // Общее сообщение для пользователей без истории — один вызов AI на всех
        string? genericMessage = null;

        var users = _db.GetAllUsers();
        _logger.LogInformation("[Scheduler] Всего пользователей: {Count}", users.Count);

        var sent = 0;
        foreach (var user in users)
        {
            // Уважаем настройки рассылки
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
                    message = await GeneratePersonalizedMessageAsync(today, dayOfYear, user.FirstName, userMessages);
                }
                else
                {
                    genericMessage ??= await GenerateGenericMessageAsync(today, dayOfYear);
                    message = genericMessage;
                }

                await _bot.SendMessage(user.TelegramId, message,
                    linkPreviewOptions: new Telegram.Bot.Types.LinkPreviewOptions { IsDisabled = true });

                sent++;
                await Task.Delay(350); // соблюдаем лимиты Telegram
            }
            catch (Exception ex)
            {
                _logger.LogWarning("[Scheduler] Не удалось отправить {UserId}: {Error}", user.TelegramId, ex.Message);
            }
        }

        _logger.LogInformation("[Scheduler] Рассылка завершена. Отправлено: {Sent}", sent);
    }

    private async Task<string> GenerateGenericMessageAsync(string today, int dayOfYear)
    {
        var system = """
            Ты — тёплый помощник христианского книжного клуба LioBot.
            Твоя задача — каждое утро отправлять короткое, живое и вдохновляющее сообщение.
            Правила:
            1. Пиши только на русском языке.
            2. Тон — тёплый, как у заботливого пастора или друга.
            3. Включи 1 библейский стих (книга, глава) по Синодальному переводу — дословно — и 2-3 предложения вдохновения.
            4. Не повторяй стихи из прошлых сообщений.
            5. Максимум 5 предложений. Не начинай с "Доброе утро!".
            6. Ты бот — не пиши "я молюсь за вас". Вместо этого пожелай Божьего благословения.
            """;
        try
        {
            return await _groq.AskAsync(system,
                $"Сегодня {today}. День года: {dayOfYear}. Напиши уникальное утреннее вдохновение.", maxTokens: 400);
        }
        catch
        {
            return $"Благословенного дня! 🙏\n\n«Уповай на Господа всем сердцем твоим и не полагайся на разум твой.» (Притчи 3:5)";
        }
    }

    private async Task<string> GeneratePersonalizedMessageAsync(
        string today, int dayOfYear, string firstName, List<string> recentTopics)
    {
        var topics = string.Join("; ", recentTopics.TakeLast(3));
        var system = $"""
            Ты — тёплый помощник христианского книжного клуба LioBot.
            Отправь {firstName} персонализированное утреннее вдохновение.
            Правила:
            1. Пиши на русском, обращайся по имени.
            2. Тон — тёплый, как у заботливого пастора.
            3. Включи 1 библейский стих по Синодальному переводу — дословно — связанный с темами из истории.
            4. 1-2 предложения, перекликающиеся с тем, что человека занимало.
            5. Максимум 5 предложений. Не начинай с "Доброе утро!".
            6. Не пиши "я молюсь за тебя". Пожелай Божьего благословения.
            """;
        try
        {
            return await _groq.AskAsync(system,
                $"Сегодня {today}. День года: {dayOfYear}. Недавние темы: {topics}", maxTokens: 450);
        }
        catch
        {
            return $"Благословенного дня, {firstName}! 🙏\n\n«Уповай на Господа всем сердцем твоим и не полагайся на разум твой.» (Притчи 3:5)";
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

        var jobKey = new JobKey("MorningMessageJob");
        quartz.AddJob<MorningMessageJob>(opts => opts.WithIdentity(jobKey));
        quartz.AddTrigger(opts => opts
            .ForJob(jobKey)
            .WithIdentity("MorningTrigger")
            .WithCronSchedule($"0 {minute} {hour} * * ?",
                x => x.InTimeZone(GetTimeZone(config["TimeZone"] ?? "Russian Standard Time")))
        );
    }

    private static TimeZoneInfo GetTimeZone(string id)
    {
        try { return TimeZoneInfo.FindSystemTimeZoneById(id); }
        catch { return TimeZoneInfo.Utc; }
    }
}
