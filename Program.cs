using LioBot.Data;
using LioBot.Handlers;
using LioBot.Services;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Quartz;
using Telegram.Bot;
using Telegram.Bot.Polling;
using Telegram.Bot.Types;
using Telegram.Bot.Types.Enums;

var builder = Host.CreateApplicationBuilder(args);

// --- Конфигурация (appsettings.json + переменные окружения Railway) ---
builder.Configuration
    .SetBasePath(AppContext.BaseDirectory)
    .AddJsonFile("appsettings.json", optional: true, reloadOnChange: true)
    .AddEnvironmentVariables();

// --- HTTP-клиент для Anthropic ---
builder.Services.AddHttpClient("anthropic");

// --- Сервисы приложения ---
builder.Services.AddSingleton<DatabaseContext>();
builder.Services.AddSingleton<ClaudeService>();
builder.Services.AddSingleton<BookService>();
builder.Services.AddSingleton<ContentImportService>();
builder.Services.AddSingleton<MessageHandler>();

// --- Telegram Bot Client ---
builder.Services.AddSingleton<ITelegramBotClient>(sp =>
{
    var config = sp.GetRequiredService<IConfiguration>();
    var token = config["TelegramBotToken"]
        ?? throw new InvalidOperationException("TelegramBotToken не задан в конфигурации.");
    return new TelegramBotClient(token);
});

// --- Quartz Scheduler ---
builder.Services.AddQuartz(q =>
{
    q.AddMorningSchedule(builder.Configuration);
});
builder.Services.AddQuartzHostedService(opt =>
{
    opt.WaitForJobsToComplete = true;
});

// --- Hosted Service для Telegram Polling ---
builder.Services.AddHostedService<BotPollingService>();

var host = builder.Build();

// --- Инициализация БД ---
var db = host.Services.GetRequiredService<DatabaseContext>();
db.Initialize();
LioBot.Data.BookSeeder.SeedIfEmpty(db);

// Персистим последний bot-message ID, чтобы утренняя рассылка могла
// редактировать прошлое сообщение даже после рестарта процесса.
LioBot.Services.BotMessageTracker.OnPersistLast = (tid, mid) =>
{
    try { db.SetLastBotMessageId(tid, mid); } catch { /* not critical */ }
};

await host.RunAsync();

// ============================================================
// Hosted Service — Telegram Long Polling
// ============================================================
public class BotPollingService : BackgroundService
{
    private readonly ITelegramBotClient _bot;
    private readonly MessageHandler _handler;
    private readonly ILogger<BotPollingService> _logger;

    public BotPollingService(
        ITelegramBotClient bot,
        MessageHandler handler,
        ILogger<BotPollingService> logger)
    {
        _bot = bot;
        _handler = handler;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var me = await _bot.GetMe(stoppingToken);
        _logger.LogInformation("[Bot] Запущен как @{Username}", me.Username);

        // Регистрируем список команд + переключаем кнопку «Меню» в углу
        // ввода в режим commands. Тогда нажатие на неё откроет навигацию
        // по разделам прямо у поля ввода.
        try
        {
            await _bot.SetMyCommands(new[]
            {
                new BotCommand { Command = "pick",      Description = "🤖 Подбери материал" },
                new BotCommand { Command = "books",     Description = "📖 Книги" },
                new BotCommand { Command = "audio",     Description = "🎧 Аудио" },
                new BotCommand { Command = "articles",  Description = "📰 Статьи" },
                new BotCommand { Command = "radio",     Description = "🎙 Радио" },
                new BotCommand { Command = "magazines", Description = "📔 Журналы" },
                new BotCommand { Command = "authors",   Description = "👤 Авторы" },
                new BotCommand { Command = "profile",   Description = "📊 Профиль" },
                new BotCommand { Command = "mybooks",   Description = "📚 Мои материалы" },
                new BotCommand { Command = "help",      Description = "ℹ️ Помощь" }
            }, cancellationToken: stoppingToken);
            await _bot.SetChatMenuButton(menuButton: new MenuButtonCommands(),
                cancellationToken: stoppingToken);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "[Bot] Не удалось зарегистрировать команды/кнопку меню");
        }

        var options = new ReceiverOptions
        {
            AllowedUpdates = new[]
            {
                Telegram.Bot.Types.Enums.UpdateType.Message,
                Telegram.Bot.Types.Enums.UpdateType.CallbackQuery,
                Telegram.Bot.Types.Enums.UpdateType.InlineQuery
            },
            DropPendingUpdates = true
        };

        await _bot.ReceiveAsync(
            updateHandler:  _handler.HandleUpdateAsync,
            errorHandler:   _handler.HandlePollingErrorAsync,
            receiverOptions: options,
            cancellationToken: stoppingToken
        );
    }
}
