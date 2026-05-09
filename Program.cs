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
    private readonly LioBot.Services.ContentImportService _importer;
    private readonly LioBot.Data.DatabaseContext _db;
    private readonly ILogger<BotPollingService> _logger;

    public BotPollingService(
        ITelegramBotClient bot,
        MessageHandler handler,
        LioBot.Services.ContentImportService importer,
        LioBot.Data.DatabaseContext db,
        ILogger<BotPollingService> logger)
    {
        _bot = bot;
        _handler = handler;
        _importer = importer;
        _db = db;
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
                new BotCommand { Command = "menu",      Description = "🏠 Главное меню" },
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
                Telegram.Bot.Types.Enums.UpdateType.InlineQuery,
                // Канальные посты нужны, чтобы автоматически переставлять
                // меню «вниз» после каждого нового поста в канале.
                Telegram.Bot.Types.Enums.UpdateType.ChannelPost
            },
            DropPendingUpdates = true
        };

        // Self-healing bootstrap: если у журналов мало выпусков, тихо
        // запускаем переимпорт в фоне. Срабатывает после первого деплоя
        // с новым flipbook-парсером, дальше становится no-op.
        _ = Task.Run(async () =>
        {
            try
            {
                var veraId = _db.GetMagazineIdBySlug("vera");
                var tropId = _db.GetMagazineIdBySlug("tropinka");
                var veraCount = veraId.HasValue ? _db.GetMagazineIssues(veraId.Value).Count : 0;
                var tropCount = tropId.HasValue ? _db.GetMagazineIssues(tropId.Value).Count : 0;
                _logger.LogInformation("[Bootstrap] Журналы: vera={V}, tropinka={T}", veraCount, tropCount);

                if (veraCount < 100 || tropCount < 50)
                {
                    if (_importer.IsRunning)
                    {
                        _logger.LogInformation("[Bootstrap] импорт уже идёт — пропуск");
                        return;
                    }
                    _logger.LogInformation("[Bootstrap] запускаю переимпорт выпусков (vera<100 OR tropinka<50)");
                    var added = await _importer.ImportMagazineIssuesAsync(stoppingToken);
                    _logger.LogInformation("[Bootstrap] добавлено выпусков: {Added}", added);
                }

                // Дедуп: убираем пары «2023 №1» + «Тропинка 2023.1», возникшие
                // на стыке старого парсера обложек и нового flipbook-парсера.
                var dedupCount = _importer.DeduplicateMagazineIssues();
                if (dedupCount > 0)
                    _logger.LogInformation("[Bootstrap] удалено дублей выпусков: {N}", dedupCount);
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "[Bootstrap] переимпорт выпусков упал");
            }
        }, stoppingToken);

        await _bot.ReceiveAsync(
            updateHandler:  _handler.HandleUpdateAsync,
            errorHandler:   _handler.HandlePollingErrorAsync,
            receiverOptions: options,
            cancellationToken: stoppingToken
        );
    }
}
