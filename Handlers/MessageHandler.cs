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
                    await bot.SendTextMessageAsync(chatId, "Загружаю книгу по ссылке... ⏳", cancellationToken: ct);
                    reply = await _bookService.AddBookFromUrlAsync(url);
                }
            }
            else if (IsBookRequest(text))
            {
                await bot.SendTextMessageAsync(chatId,
                    "Ищу подходящие книги для тебя... 📖", cancellationToken: ct);
                reply = await _bookService.RecommendBooksAsync(text);
            }
            else
            {
                // Свободный диалог через Claude
                reply = await HandleFreeDialogAsync(text, telegramUser.FirstName);
            }

            await bot.SendTextMessageAsync(chatId, reply, cancellationToken: ct);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "[Bot] Ошибка при обработке сообщения от {User}", telegramUser.FirstName);
            await bot.SendTextMessageAsync(chatId,
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

    private async Task<string> HandleFreeDialogAsync(string userText, string firstName)
    {
        var systemPrompt = $"""
            Ты — LioBot, тёплый помощник христианского книжного клуба.
            Ты общаешься с человеком по имени {firstName}.
            Отвечай на русском языке, тепло и по-дружески, в христианском духе.
            Если человек спрашивает о книгах — помогай с выбором.
            Если человек делится чем-то личным — поддержи его словом и, при уместности, стихом из Писания. Библейские стихи цитируй ТОЛЬКО по Синодальному переводу — дословно, без изменений.
            Если вопрос не связан с книгами или верой — мягко верни разговор к этим темам.
            Отвечай кратко — не более 3-4 предложений.
            ВАЖНО: ты бот, поэтому никогда не пиши "я молюсь за тебя", "буду молиться", "молюсь о тебе" и подобные фразы — ты не можешь молиться. Вместо этого пожелай чтобы Бог помог, благословил или поддержал человека.
            """;

        return await _claude.AskAsync(systemPrompt, userText, maxTokens: 512);
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
