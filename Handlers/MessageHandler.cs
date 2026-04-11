using LioBot.Data;
using LioBot.Models;
using LioBot.Services;
using Microsoft.Extensions.Logging;
using Telegram.Bot;
using Telegram.Bot.Exceptions;
using Telegram.Bot.Types;
using Telegram.Bot.Types.Enums;

namespace LioBot.Handlers;

public class MessageHandler
{
    private readonly DatabaseContext _db;
    private readonly BookService _bookService;
    private readonly ClaudeService _claude;
    private readonly ILogger<MessageHandler> _logger;

    public MessageHandler(
        DatabaseContext db,
        BookService bookService,
        ClaudeService claude,
        ILogger<MessageHandler> logger)
    {
        _db = db;
        _bookService = bookService;
        _claude = claude;
        _logger = logger;
    }

    public async Task HandleUpdateAsync(ITelegramBotClient bot, Update update, CancellationToken ct)
    {
        if (update.Type != UpdateType.Message) return;

        var message = update.Message;
        if (message?.Text is null) return;

        var telegramUser = message.From!;
        RegisterOrUpdateUser(telegramUser);

        var text = message.Text.Trim();
        var chatId = message.Chat.Id;

        _logger.LogInformation("[Bot] {User} -> {Text}", telegramUser.FirstName, text);

        try
        {
            string reply;
            var isCommand = text.StartsWith("/");

            if (text.StartsWith("/start"))
            {
                reply = BuildWelcomeMessage(telegramUser.FirstName);
            }
            else if (text.StartsWith("/help"))
            {
                reply = BuildHelpMessage();
            }
            else if (text.StartsWith("/books"))
            {
                reply = BuildBookListMessage();
            }
            else if (text.StartsWith("/addbook"))
            {
                var url = text.Replace("/addbook", "").Trim();
                if (string.IsNullOrEmpty(url))
                {
                    reply = "Укажи ссылку после команды. Например:\n/addbook https://www.lio-int.com/knigi/название";
                }
                else
                {
                    await bot.SendMessage(chatId, "Загружаю книгу по ссылке... ⏳",
                        cancellationToken: ct);
                    reply = await _bookService.AddBookFromUrlAsync(url);
                }
            }
            else if (IsBookRequest(text))
            {
                await bot.SendMessage(chatId, "Ищу подходящие книги для тебя... 📖",
                    cancellationToken: ct);
                // Передаём историю чтобы не советовать уже рекомендованные книги
                var history = _db.GetHistory(telegramUser.Id, limit: 10);
                reply = await _bookService.RecommendBooksAsync(text, history);
            }
            else
            {
                // Свободный диалог — с историей
                var history = _db.GetHistory(telegramUser.Id, limit: 10);
                reply = await HandleFreeDialogAsync(text, telegramUser.FirstName, history);
            }

            // Сохраняем сообщения в историю (кроме команд)
            if (!isCommand)
            {
                _db.SaveMessage(telegramUser.Id, "user", text);
                _db.SaveMessage(telegramUser.Id, "assistant", reply);
            }

            await bot.SendMessage(chatId, reply,
                parseMode: Telegram.Bot.Types.Enums.ParseMode.Html,
                linkPreviewOptions: new Telegram.Bot.Types.LinkPreviewOptions { IsDisabled = true },
                cancellationToken: ct);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "[Bot] Ошибка при обработке сообщения от {User}", telegramUser.FirstName);
            await bot.SendMessage(chatId,
                "Прости, что-то пошло не так. Попробуй снова 🙏", cancellationToken: ct);
        }
    }

    public Task HandlePollingErrorAsync(ITelegramBotClient bot, Exception ex, CancellationToken ct)
    {
        var message = ex switch
        {
            ApiRequestException apiEx => $"Telegram API Error [{apiEx.ErrorCode}]: {apiEx.Message}",
            _ => ex.ToString()
        };
        _logger.LogError("[Polling Error] {Message}", message);
        return Task.CompletedTask;
    }

    // --- Приватные методы ---

    private void RegisterOrUpdateUser(Telegram.Bot.Types.User telegramUser)
    {
        var existing = _db.GetUserByTelegramId(telegramUser.Id);
        _db.UpsertUser(new LioBot.Models.User
        {
            TelegramId   = telegramUser.Id,
            Username     = telegramUser.Username,
            FirstName    = telegramUser.FirstName,
            RegisteredAt = existing?.RegisteredAt ?? DateTime.UtcNow
        });
    }

    private static bool IsBookRequest(string text)
    {
        var lower = text.ToLowerInvariant();
        return lower.Contains("книг")
            || lower.Contains("посоветуй")
            || lower.Contains("рекомендуй")
            || lower.Contains("почитать")
            || lower.Contains("подбери")
            || lower.Contains("хочу читать");
    }

    private async Task<string> HandleFreeDialogAsync(
        string userText,
        string firstName,
        IEnumerable<(string Role, string Content)>? history = null)
    {
        var systemPrompt = $"""
            Ты — LioBot, помощник христианского книжного клуба. Общаешься с {firstName}.
            Отвечай по-русски, кратко — 2-3 предложения максимум.
            Используй историю переписки чтобы понимать контекст и не повторяться.
            Библейские стихи цитируй только когда человек явно переживает — и только по Синодальному переводу, дословно. Не вставляй стих в каждый ответ.
            Не пиши "я молюсь за тебя" — ты бот.
            Не добавляй ободрения и пожелания в конце каждого сообщения — только если это уместно по контексту.
            """;

        return await _claude.AskWithHistoryAsync(systemPrompt, userText, history, maxTokens: 512);
    }

    private string BuildWelcomeMessage(string firstName) => $"""
        Привет, {firstName}! 👋

        Рад видеть тебя здесь! Я LioBot — помощник христианского книжного клуба 📚

        Вот что я умею:
        • Подобрать книгу под твою ситуацию или вопрос — просто опиши, что тебя сейчас занимает
        • Ответить на любой вопрос о книгах и вере
        • Каждое утро присылать вдохновляющее слово

        Напиши мне, что тебя сейчас волнует, или просто скажи «посоветуй книгу» — и я помогу найти то, что нужно! 🙏
        """;

    private static string BuildHelpMessage() => """
        📖 Как пользоваться LioBot:

        • Просто напиши, что тебя сейчас занимает — и я подберу книгу
        • «Посоветуй книгу про молитву» — подбор по теме
        • «Мне сейчас трудно, я переживаю потерю» — подбор по ситуации
        • /books — показать весь каталог
        • /addbook <ссылка> — добавить книгу по ссылке с сайта
        • /help — эта подсказка

        Я всегда рад поговорить! Пиши в свободной форме 💬
        """;

    private string BuildBookListMessage()
    {
        var books = _bookService.GetAllBooks();
        if (books.Count == 0) return "Каталог пока пуст. Скоро добавим книги! 📚";

        var list = string.Join("\n", books.Select((b, i) => $"{i + 1}. «{b.Title}» — {b.Author}"));
        return $"📚 Наш каталог книг:\n\n{list}\n\nХочешь узнать подробнее о какой-то книге или подобрать что-то конкретное — просто напиши!";
    }
}
