using LioBot.Data;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Quartz;
using Telegram.Bot;

namespace LioBot.Services;

/// <summary>
/// Quartz-задача: каждое утро рассылает вдохновляющее сообщение всем пользователям.
/// </summary>
[DisallowConcurrentExecution]
public class MorningMessageJob : IJob
{
    private readonly DatabaseContext _db;
    private readonly ClaudeService _claude;
    private readonly ITelegramBotClient _bot;
    private readonly ILogger<MorningMessageJob> _logger;

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

        var today = DateTime.Now.ToString("dd MMMM yyyy", new System.Globalization.CultureInfo("ru-RU"));
        var dayOfYear = DateTime.Now.DayOfYear;

        var systemPrompt = """
            Ты — тёплый помощник христианского книжного клуба LioBot.
            Твоя задача — каждое утро отправлять короткое, живое и вдохновляющее сообщение.

            Правила:
            1. Пиши только на русском языке.
            2. Тон — тёплый, дружелюбный, как у заботливого пастора или друга.
            3. Включи в сообщение: 1 библейский стих (с указанием книги и главы) и 2-3 предложения наставления или вдохновения. Цитируй стих ТОЛЬКО по Синодальному переводу Библии — дословно, без изменений.
            4. Не повторяй стихи и темы, которые встречались в прошлых сообщениях.
            5. Сообщение должно быть коротким — до 5 предложений.
            6. Не начинай с "Доброе утро!" — придумай разные приветствия.
            7. Ты бот, поэтому никогда не пиши "я молюсь за вас/тебя" — ты не можешь молиться. Вместо этого пожелай Божьего благословения.
            """;

        var userMessage = $"Сегодня {today}. День года: {dayOfYear}. Напиши уникальное утреннее вдохновение.";

        var message = await _claude.AskAsync(systemPrompt, userMessage, maxTokens: 400);

        var users = _db.GetAllUsers();
        _logger.LogInformation("[Scheduler] Рассылка для {Count} пользователей.", users.Count);

        foreach (var user in users)
        {
            try
            {
                await _bot.SendMessage(user.TelegramId, message,
                    linkPreviewOptions: new Telegram.Bot.Types.LinkPreviewOptions { IsDisabled = true });
                await Task.Delay(50); // небольшая задержка, чтобы не упереться в лимиты Telegram
            }
            catch (Exception ex)
            {
                _logger.LogWarning("[Scheduler] Не удалось отправить {UserId}: {Error}", user.TelegramId, ex.Message);
            }
        }

        _logger.LogInformation("[Scheduler] Рассылка завершена.");
    }
}

/// <summary>
/// Регистрирует и запускает Quartz-расписание на основе ScheduleTime из конфигурации.
/// </summary>
public static class SchedulerConfigurator
{
    public static void AddMorningSchedule(this IServiceCollectionQuartzConfigurator quartz, IConfiguration config)
    {
        var timeStr = config["ScheduleTime"] ?? "08:00";
        var parts = timeStr.Split(':');
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
