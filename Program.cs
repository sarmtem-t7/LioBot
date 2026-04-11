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
        var me = await _bot.GetMeAsync(stoppingToken);
        _logger.LogInformation("[Bot] Запущен как @{Username}", me.Username);

        var options = new ReceiverOptions
        {
            AllowedUpdates = new[] { Telegram.Bot.Types.Enums.UpdateType.Message },
            ThrowPendingUpdates = true
        };

        await _bot.ReceiveAsync(
            updateHandler:        _handler.HandleUpdateAsync,
            pollingErrorHandler:  _handler.HandlePollingErrorAsync,
            receiverOptions:      options,
            cancellationToken:    stoppingToken
        );
    }
}
