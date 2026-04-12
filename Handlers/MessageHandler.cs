using LioBot.Data;
using LioBot.Models;
using LioBot.Services;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Telegram.Bot;
using Telegram.Bot.Exceptions;
using Telegram.Bot.Types;
using Telegram.Bot.Types.Enums;
using Telegram.Bot.Types.InlineQueryResults;
using Telegram.Bot.Types.ReplyMarkups;

namespace LioBot.Handlers;

public class MessageHandler
{
    private readonly DatabaseContext _db;
    private readonly BookService _bookService;
    private readonly GroqService _groq;
    private readonly ILogger<MessageHandler> _logger;
    private readonly HashSet<long> _adminIds;

    private const int CatalogPageSize = 8;

    // Быстрые темы для подбора книг
    private static readonly (string Label, string Tag)[] QuickTopics =
    {
        ("🙏 Молитва",          "молитва"),
        ("👨‍👩‍👧 Семья",            "семья"),
        ("💪 Вера в трудностях", "трудности"),
        ("📖 Библеистика",       "библия"),
        ("🌱 Духовный рост",     "духовный рост"),
        ("⛪ История церкви",    "история церкви"),
    };

    public MessageHandler(
        DatabaseContext db,
        BookService bookService,
        GroqService groq,
        IConfiguration configuration,
        ILogger<MessageHandler> logger)
    {
        _db = db;
        _bookService = bookService;
        _groq = groq;
        _logger = logger;

        _adminIds = (configuration["AdminIds"] ?? "")
            .Split(',', StringSplitOptions.RemoveEmptyEntries)
            .Select(s => long.TryParse(s.Trim(), out var id) ? id : 0L)
            .Where(id => id != 0)
            .ToHashSet();
    }

    // ════════════════════════════════════════════════════════════
    // Точка входа
    // ════════════════════════════════════════════════════════════

    public async Task HandleUpdateAsync(ITelegramBotClient bot, Update update, CancellationToken ct)
    {
        if (update.Type == UpdateType.InlineQuery && update.InlineQuery != null)
        {
            await HandleInlineQueryAsync(bot, update.InlineQuery, ct);
            return;
        }

        if (update.Type == UpdateType.CallbackQuery && update.CallbackQuery != null)
        {
            await HandleCallbackQueryAsync(bot, update.CallbackQuery, ct);
            return;
        }

        if (update.Type != UpdateType.Message) return;

        var message = update.Message;
        if (message?.Text is null) return;

        var telegramUser = message.From!;
        RegisterOrUpdateUser(telegramUser);

        var text   = message.Text.Trim();
        var chatId = message.Chat.Id;

        _logger.LogInformation("[Bot] {User} -> {Text}", telegramUser.FirstName, text);

        try
        {
            await bot.SendChatAction(chatId, ChatAction.Typing, cancellationToken: ct);
            var readDelay = Math.Clamp(text.Length * 30, 600, 2500);
            await Task.Delay(readDelay + Random.Shared.Next(0, 400), ct);

            string reply;
            InlineKeyboardMarkup? keyboard = null;
            var isCommand = text.StartsWith("/");

            if (text.StartsWith("/start"))
            {
                reply = BuildWelcomeMessage(telegramUser.FirstName);
                keyboard = MainMenuKeyboard();
            }
            else if (text.StartsWith("/help"))
            {
                reply = BuildHelpMessage();
                keyboard = MainMenuKeyboard();
            }
            else if (text.StartsWith("/books"))
            {
                var (pageText, pageKeyboard) = BuildCatalogPage(0, "all");
                reply = pageText; keyboard = pageKeyboard;
            }
            else if (text.StartsWith("/mybooks"))
            {
                reply = BuildMyBooksMessage(telegramUser.Id);
                keyboard = MainMenuKeyboard();
            }
            else if (text.StartsWith("/notifications"))
            {
                reply = BuildNotifySettingsMessage(telegramUser.Id);
                keyboard = NotifySettingsKeyboard(telegramUser.Id);
            }
            else if (text.StartsWith("/search "))
            {
                var query = text[8..].Trim();
                await bot.SendMessage(chatId, "Ищу... 📖", cancellationToken: ct);
                var result = await _bookService.RecommendBooksAsync(query, null, telegramUser.Id);
                reply = result.Text; keyboard = RecommendationKeyboard(result.Books);
            }
            else if (text == "/search")
            {
                reply = "Напиши что ищешь после команды:\n<code>/search молитва</code>\nили просто опиши ситуацию словами — я пойму.";
                keyboard = MainMenuKeyboard();
            }
            else if (text.StartsWith("/addbook"))
            {
                if (_adminIds.Count > 0 && !_adminIds.Contains(telegramUser.Id))
                {
                    reply = "Эта команда доступна только администраторам.";
                }
                else
                {
                    var url = text.Replace("/addbook", "").Trim();
                    if (string.IsNullOrEmpty(url))
                        reply = "Укажи ссылку после команды:\n<code>/addbook https://lio-int.com/knigi/название</code>";
                    else
                    {
                        await bot.SendMessage(chatId, "Загружаю книгу по ссылке... ⏳", cancellationToken: ct);
                        reply = await _bookService.AddBookFromUrlAsync(url);
                    }
                }
            }
            else if (IsBookRequest(text))
            {
                await bot.SendMessage(chatId, "Ищу подходящие книги... 📖", cancellationToken: ct);
                var history = _db.GetHistory(telegramUser.Id, limit: 10);
                var result  = await _bookService.RecommendBooksAsync(text, history, telegramUser.Id);
                reply = result.Text; keyboard = RecommendationKeyboard(result.Books);
            }
            else
            {
                var history = _db.GetHistory(telegramUser.Id, limit: 10);
                reply = await HandleFreeDialogAsync(text, telegramUser.FirstName, history);
                keyboard = QuickActionsKeyboard();
            }

            if (!isCommand)
            {
                _db.SaveMessage(telegramUser.Id, "user", text);
                _db.SaveMessage(telegramUser.Id, "assistant", reply);
            }

            await SendMessage(bot, chatId, reply, keyboard, ct);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "[Bot] Ошибка при обработке сообщения от {User}", telegramUser.FirstName);
            await bot.SendMessage(chatId, "Что-то пошло не так. Попробуй снова 🙏", cancellationToken: ct);
        }
    }

    // ════════════════════════════════════════════════════════════
    // Callback Query
    // ════════════════════════════════════════════════════════════

    private async Task HandleCallbackQueryAsync(ITelegramBotClient bot, CallbackQuery query, CancellationToken ct)
    {
        var chatId    = query.Message!.Chat.Id;
        var messageId = query.Message.MessageId;
        var user      = query.From;
        var data      = query.Data ?? "";

        _logger.LogInformation("[Bot] Callback {User} -> {Data}", user.FirstName, data);

        try
        {
            // ── Каталог: навигация по страницам + фильтр по типу ──────
            if (data.StartsWith("catalog:"))
            {
                var parts = data.Split(':');
                var type  = parts.Length > 1 ? parts[1] : "all";
                var page  = parts.Length > 2 && int.TryParse(parts[2], out var p) ? p : 0;

                var (pageText, pageKeyboard) = BuildCatalogPage(page, type);
                await bot.AnswerCallbackQuery(query.Id, cancellationToken: ct);
                await bot.EditMessageText(chatId, messageId, pageText,
                    parseMode: ParseMode.Html,
                    replyMarkup: pageKeyboard,
                    linkPreviewOptions: new() { IsDisabled = true },
                    cancellationToken: ct);
                return;
            }

            // ── Карточка книги ─────────────────────────────────────────
            if (data.StartsWith("book:card:") && long.TryParse(data[10..], out var cardId))
            {
                var book = _bookService.GetBookById(cardId);
                if (book == null) { await bot.AnswerCallbackQuery(query.Id, "Книга не найдена", cancellationToken: ct); return; }
                await bot.AnswerCallbackQuery(query.Id, cancellationToken: ct);
                await bot.EditMessageText(chatId, messageId, BookService.FormatBookCard(book),
                    parseMode: ParseMode.Html,
                    replyMarkup: BookCardKeyboard(book),
                    linkPreviewOptions: new() { IsDisabled = true },
                    cancellationToken: ct);
                return;
            }

            // ── Отметить прочитанным ───────────────────────────────────
            if (data.StartsWith("book:read:") && long.TryParse(data[10..], out var readId))
            {
                _db.MarkBookAsRead(user.Id, readId);
                await bot.AnswerCallbackQuery(query.Id, "✅ Добавлено в список прочитанных!", cancellationToken: ct);
                return;
            }

            // ── Скрыть книгу ───────────────────────────────────────────
            if (data.StartsWith("book:ignore:") && long.TryParse(data[12..], out var ignoreId))
            {
                _db.MarkBookAsIgnored(user.Id, ignoreId);
                _db.MarkBooksAsSeen(user.Id, [ignoreId]);
                await bot.AnswerCallbackQuery(query.Id, "❌ Книга скрыта из рекомендаций", cancellationToken: ct);
                return;
            }

            // ── Похожие книги ──────────────────────────────────────────
            if (data.StartsWith("book:similar:") && long.TryParse(data[13..], out var similarId))
            {
                await bot.AnswerCallbackQuery(query.Id, cancellationToken: ct);
                await bot.SendChatAction(chatId, ChatAction.Typing, cancellationToken: ct);
                var result = await _bookService.GetSimilarBooksAsync(similarId, user.Id);
                await SendMessage(bot, chatId, result.Text, RecommendationKeyboard(result.Books), ct);
                return;
            }

            // ── Настройки рассылки ─────────────────────────────────────
            if (data.StartsWith("notify:"))
            {
                var mode = data[7..];
                if (mode is "daily" or "weekly" or "off")
                {
                    _db.SetNotifyMode(user.Id, mode);
                    var label = mode switch
                    {
                        "daily"  => "📅 Каждый день",
                        "weekly" => "📆 Раз в неделю (по понедельникам)",
                        _        => "🔕 Отключена"
                    };
                    await bot.AnswerCallbackQuery(query.Id, cancellationToken: ct);
                    await bot.EditMessageText(chatId, messageId,
                        $"⚙️ <b>Настройки рассылки</b>\n\nРежим обновлён: <b>{label}</b>",
                        parseMode: ParseMode.Html,
                        replyMarkup: NotifySettingsKeyboard(user.Id),
                        cancellationToken: ct);
                    return;
                }
                if (mode == "menu")
                {
                    await bot.AnswerCallbackQuery(query.Id, cancellationToken: ct);
                    await bot.EditMessageText(chatId, messageId,
                        BuildNotifySettingsMessage(user.Id),
                        parseMode: ParseMode.Html,
                        replyMarkup: NotifySettingsKeyboard(user.Id),
                        cancellationToken: ct);
                    return;
                }
            }

            // ── Быстрые темы ───────────────────────────────────────────
            if (data.StartsWith("topic:"))
            {
                var tag = data[6..];
                if (tag == "menu")
                {
                    await bot.AnswerCallbackQuery(query.Id, cancellationToken: ct);
                    await bot.EditMessageText(chatId, messageId,
                        "🏷️ Выбери тему — подберу книги:",
                        replyMarkup: TopicsKeyboard(),
                        cancellationToken: ct);
                    return;
                }
                await bot.AnswerCallbackQuery(query.Id, cancellationToken: ct);
                await bot.SendChatAction(chatId, ChatAction.Typing, cancellationToken: ct);
                await bot.SendMessage(chatId, "Ищу подходящие книги... 📖", cancellationToken: ct);
                var history = _db.GetHistory(user.Id, limit: 10);
                var result  = await _bookService.RecommendBooksAsync(tag, history, user.Id);
                _db.SaveMessage(user.Id, "user",      $"Тема: {tag}");
                _db.SaveMessage(user.Id, "assistant", result.Text);
                await SendMessage(bot, chatId, result.Text, RecommendationKeyboard(result.Books), ct);
                return;
            }

            // ── Остальные команды меню ─────────────────────────────────
            await bot.AnswerCallbackQuery(query.Id, cancellationToken: ct);
            await bot.SendChatAction(chatId, ChatAction.Typing, cancellationToken: ct);
            await Task.Delay(Random.Shared.Next(400, 800), ct);

            string reply;
            InlineKeyboardMarkup? keyboard = null;

            switch (data)
            {
                case "menu:recommend":
                    await bot.SendMessage(chatId, "Ищу подходящие книги... 📖", cancellationToken: ct);
                    var recHistory = _db.GetHistory(user.Id, limit: 10);
                    var recResult  = await _bookService.RecommendBooksAsync("посоветуй книгу", recHistory, user.Id);
                    _db.SaveMessage(user.Id, "user",      "Посоветуй книгу");
                    _db.SaveMessage(user.Id, "assistant", recResult.Text);
                    reply    = recResult.Text;
                    keyboard = RecommendationKeyboard(recResult.Books);
                    break;

                case "menu:catalog":
                    var (ct2, ck2) = BuildCatalogPage(0, "all");
                    reply = ct2; keyboard = ck2;
                    break;

                case "menu:help":
                    reply = BuildHelpMessage(); keyboard = MainMenuKeyboard();
                    break;

                case "menu:topics":
                    reply = "🏷️ Выбери тему — подберу книги:";
                    keyboard = TopicsKeyboard();
                    break;

                case "menu:mybooks":
                    reply = BuildMyBooksMessage(user.Id); keyboard = MainMenuKeyboard();
                    break;

                case "menu:notifications":
                    reply    = BuildNotifySettingsMessage(user.Id);
                    keyboard = NotifySettingsKeyboard(user.Id);
                    break;

                case "book:random":
                    var dayBook = _bookService.GetBookOfDay();
                    if (dayBook == null) { reply = "Каталог пока пуст 📚"; break; }
                    reply    = $"🎲 <b>Книга дня:</b>\n\n{BookService.FormatBookCard(dayBook)}";
                    keyboard = BookCardKeyboard(dayBook);
                    break;

                case "recommend:more":
                    await bot.SendMessage(chatId, "Ищу другие варианты... 📖", cancellationToken: ct);
                    var moreHistory = _db.GetHistory(user.Id, limit: 20);
                    var moreResult  = await _bookService.RecommendBooksAsync("посоветуй другие книги", moreHistory, user.Id);
                    _db.SaveMessage(user.Id, "user",      "Посоветуй другие книги");
                    _db.SaveMessage(user.Id, "assistant", moreResult.Text);
                    reply    = moreResult.Text;
                    keyboard = RecommendationKeyboard(moreResult.Books);
                    break;

                case "menu:back":
                    reply = $"Главное меню"; keyboard = MainMenuKeyboard();
                    break;

                default:
                    return;
            }

            await SendMessage(bot, chatId, reply, keyboard, ct);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "[Bot] Ошибка при обработке callback от {User}", user.FirstName);
        }
    }

    // ════════════════════════════════════════════════════════════
    // Inline Query
    // ════════════════════════════════════════════════════════════

    private async Task HandleInlineQueryAsync(ITelegramBotClient bot, InlineQuery query, CancellationToken ct)
    {
        var searchText = query.Query.Trim();
        var books = string.IsNullOrEmpty(searchText)
            ? _bookService.GetAllBooks().OrderBy(_ => Guid.NewGuid()).Take(5).ToList()
            : _bookService.SearchBooks(searchText).Take(5).ToList();

        var results = books.Select(book =>
        {
            var isAudio  = book.Type == "audio";
            var icon     = isAudio ? "🎧" : "📖";
            var linkPart = string.IsNullOrEmpty(book.Url)
                ? ""
                : $"\n\n<a href=\"{book.Url}\">→ {(isAudio ? "Слушать" : "Читать")}</a>";
            var msgText  =
                $"{icon} <b>«{BookService.EscapeHtml(book.Title)}»</b> — {BookService.EscapeHtml(book.Author)}\n\n" +
                $"{BookService.EscapeHtml(book.Description)}{linkPart}";
            var shortDesc = string.IsNullOrEmpty(book.Description) ? book.Author
                : (book.Description.Length > 60 ? book.Description[..60] + "..." : book.Description);

            return new InlineQueryResultArticle(
                id:                  book.Id.ToString(),
                title:               $"{icon} {book.Title}",
                inputMessageContent: new InputTextMessageContent(msgText)
                {
                    ParseMode           = ParseMode.Html,
                    LinkPreviewOptions  = new() { IsDisabled = true }
                })
            {
                Description = shortDesc
            };
        }).ToArray<InlineQueryResult>();

        await bot.AnswerInlineQuery(query.Id, results, cacheTime: 300, cancellationToken: ct);
    }

    // ════════════════════════════════════════════════════════════
    // Polling error handler
    // ════════════════════════════════════════════════════════════

    public Task HandlePollingErrorAsync(ITelegramBotClient bot, Exception ex, CancellationToken ct)
    {
        var msg = ex is ApiRequestException apiEx
            ? $"Telegram API Error [{apiEx.ErrorCode}]: {apiEx.Message}"
            : ex.ToString();
        _logger.LogError("[Polling Error] {Message}", msg);
        return Task.CompletedTask;
    }

    // ════════════════════════════════════════════════════════════
    // Каталог с пагинацией и фильтром по типу
    // ════════════════════════════════════════════════════════════

    private (string Text, InlineKeyboardMarkup Keyboard) BuildCatalogPage(int page, string typeFilter)
    {
        var all = _bookService.GetAllBooks();
        var filtered = typeFilter switch
        {
            "book"  => all.Where(b => b.Type == "book").ToList(),
            "audio" => all.Where(b => b.Type == "audio").ToList(),
            _       => all
        };

        if (filtered.Count == 0)
        {
            var empty = typeFilter == "audio" ? "аудиокниг" : "книг";
            return ($"В каталоге пока нет {empty}. 📚", MainMenuKeyboard());
        }

        var totalPages = (int)Math.Ceiling(filtered.Count / (double)CatalogPageSize);
        page = Math.Clamp(page, 0, totalPages - 1);
        var pageBooks = filtered.Skip(page * CatalogPageSize).Take(CatalogPageSize).ToList();

        var filterLabel = typeFilter switch
        {
            "book"  => "📖 Книги",
            "audio" => "🎧 Аудиокниги",
            _       => "📚 Все"
        };
        var text = $"{filterLabel} (стр. {page + 1}/{totalPages}) — нажми для подробностей:";

        var buttons = new List<InlineKeyboardButton[]>();

        // Каждая книга — кнопка
        foreach (var b in pageBooks)
        {
            var icon  = b.Type == "audio" ? "🎧" : "📖";
            var label = $"{icon} {b.Title}";
            if (label.Length > 48) label = label[..48] + "…";
            buttons.Add([InlineKeyboardButton.WithCallbackData(label, $"book:card:{b.Id}")]);
        }

        // Навигация + фильтры
        var navRow = new List<InlineKeyboardButton>();
        if (page > 0)
            navRow.Add(InlineKeyboardButton.WithCallbackData("←", $"catalog:{typeFilter}:{page - 1}"));

        navRow.Add(InlineKeyboardButton.WithCallbackData(
            typeFilter == "all" ? "📚 Все ✓" : "📚 Все",    "catalog:all:0"));
        navRow.Add(InlineKeyboardButton.WithCallbackData(
            typeFilter == "book"  ? "📖 ✓" : "📖",           "catalog:book:0"));
        navRow.Add(InlineKeyboardButton.WithCallbackData(
            typeFilter == "audio" ? "🎧 ✓" : "🎧",           "catalog:audio:0"));

        if (page < totalPages - 1)
            navRow.Add(InlineKeyboardButton.WithCallbackData("→", $"catalog:{typeFilter}:{page + 1}"));

        buttons.Add(navRow.ToArray());
        buttons.Add([InlineKeyboardButton.WithCallbackData("📚 Посоветуй книгу", "menu:recommend")]);

        return (text, new InlineKeyboardMarkup(buttons));
    }

    // ════════════════════════════════════════════════════════════
    // Клавиатуры
    // ════════════════════════════════════════════════════════════

    private static InlineKeyboardMarkup MainMenuKeyboard() => new(new[]
    {
        new[] { InlineKeyboardButton.WithCallbackData("📚 Посоветуй книгу", "menu:recommend") },
        new[] { InlineKeyboardButton.WithCallbackData("📋 Каталог",         "menu:catalog"),
                InlineKeyboardButton.WithCallbackData("🎲 Книга дня",       "book:random") },
        new[] { InlineKeyboardButton.WithCallbackData("🏷️ По теме",        "menu:topics"),
                InlineKeyboardButton.WithCallbackData("📖 Мои книги",       "menu:mybooks") },
        new[] { InlineKeyboardButton.WithCallbackData("⚙️ Рассылка",        "menu:notifications") }
    });

    private static InlineKeyboardMarkup RecommendationKeyboard(List<Book> books)
    {
        var rows = new List<InlineKeyboardButton[]>();
        for (var i = 0; i < books.Count; i++)
        {
            var book = books[i];
            var n    = i + 1;
            rows.Add([
                InlineKeyboardButton.WithCallbackData($"✅ Прочитал кн.{n}",  $"book:read:{book.Id}"),
                InlineKeyboardButton.WithCallbackData($"❌ Скрыть кн.{n}",    $"book:ignore:{book.Id}")
            ]);
        }
        rows.Add([InlineKeyboardButton.WithCallbackData("🔄 Другие книги", "recommend:more")]);
        rows.Add([InlineKeyboardButton.WithCallbackData("📋 Весь каталог", "catalog:all:0")]);
        return new InlineKeyboardMarkup(rows);
    }

    private static InlineKeyboardMarkup BookCardKeyboard(Book book)
    {
        var rows = new List<InlineKeyboardButton[]>
        {
            new[]
            {
                InlineKeyboardButton.WithCallbackData("✅ Прочитал",      $"book:read:{book.Id}"),
                InlineKeyboardButton.WithCallbackData("❌ Скрыть",        $"book:ignore:{book.Id}")
            },
            new[] { InlineKeyboardButton.WithCallbackData("🔍 Похожие книги",  $"book:similar:{book.Id}") }
        };
        if (!string.IsNullOrEmpty(book.Url))
        {
            var linkLabel = book.Type == "audio" ? "🎧 Слушать" : "📖 Читать";
            rows.Add([InlineKeyboardButton.WithUrl(linkLabel, book.Url)]);
        }
        rows.Add([InlineKeyboardButton.WithCallbackData("← В каталог", "catalog:all:0")]);
        return new InlineKeyboardMarkup(rows);
    }

    private static InlineKeyboardMarkup TopicsKeyboard()
    {
        var topicRows = QuickTopics
            .Chunk(2)
            .Select(pair => pair
                .Select(t => InlineKeyboardButton.WithCallbackData(t.Label, $"topic:{t.Tag}"))
                .ToArray())
            .ToList();
        topicRows.Add([InlineKeyboardButton.WithCallbackData("← Назад", "menu:back")]);
        return new InlineKeyboardMarkup(topicRows);
    }

    private InlineKeyboardMarkup NotifySettingsKeyboard(long telegramId)
    {
        var user    = _db.GetUserByTelegramId(telegramId);
        var current = user?.NotifyMode ?? "daily";
        return new InlineKeyboardMarkup(new[]
        {
            new[] {
                InlineKeyboardButton.WithCallbackData(
                    current == "daily"  ? "📅 Каждый день ✓" : "📅 Каждый день",  "notify:daily"),
                InlineKeyboardButton.WithCallbackData(
                    current == "weekly" ? "📆 Раз в нед. ✓"  : "📆 Раз в нед.",  "notify:weekly")
            },
            new[] {
                InlineKeyboardButton.WithCallbackData(
                    current == "off" ? "🔕 Выключено ✓" : "🔕 Выключить", "notify:off")
            },
            new[] { InlineKeyboardButton.WithCallbackData("← Назад", "menu:back") }
        });
    }

    private static InlineKeyboardMarkup QuickActionsKeyboard() => new(new[]
    {
        new[] { InlineKeyboardButton.WithCallbackData("📚 Посоветуй книгу", "menu:recommend") },
        new[] { InlineKeyboardButton.WithCallbackData("🏷️ По теме",        "menu:topics"),
                InlineKeyboardButton.WithCallbackData("📋 Каталог",         "menu:catalog") }
    });

    // ════════════════════════════════════════════════════════════
    // Утилиты отправки (защита от 4096 символов)
    // ════════════════════════════════════════════════════════════

    private static async Task SendMessage(
        ITelegramBotClient bot, long chatId, string text,
        InlineKeyboardMarkup? keyboard, CancellationToken ct)
    {
        const int MaxLen = 4000;
        if (text.Length <= MaxLen)
        {
            await bot.SendMessage(chatId, text,
                parseMode: ParseMode.Html,
                replyMarkup: keyboard,
                linkPreviewOptions: new() { IsDisabled = true },
                cancellationToken: ct);
            return;
        }

        // Разбиваем на части по абзацам
        var chunks = SplitIntoChunks(text, MaxLen);
        for (var i = 0; i < chunks.Count; i++)
        {
            var isLast = i == chunks.Count - 1;
            await bot.SendMessage(chatId, chunks[i],
                parseMode: ParseMode.Html,
                replyMarkup: isLast ? keyboard : null,
                linkPreviewOptions: new() { IsDisabled = true },
                cancellationToken: ct);
            if (!isLast) await Task.Delay(200, ct);
        }
    }

    private static List<string> SplitIntoChunks(string text, int maxLen)
    {
        var chunks = new List<string>();
        while (text.Length > maxLen)
        {
            var cut = text.LastIndexOf('\n', maxLen);
            if (cut <= 0) cut = maxLen;
            chunks.Add(text[..cut].Trim());
            text = text[cut..].Trim();
        }
        if (text.Length > 0) chunks.Add(text);
        return chunks;
    }

    // ════════════════════════════════════════════════════════════
    // Сообщения
    // ════════════════════════════════════════════════════════════

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
        var l = text.ToLowerInvariant();
        return l.Contains("книг") || l.Contains("посоветуй") || l.Contains("рекомендуй")
            || l.Contains("почитать") || l.Contains("подбери") || l.Contains("хочу читать");
    }

    private async Task<string> HandleFreeDialogAsync(
        string userText, string firstName,
        IEnumerable<(string Role, string Content)>? history = null)
    {
        var system = $"""
            Ты — Лио, бот-консультант христианского книжного клуба «Лио». Помогаешь {firstName} подобрать книги и ответить на вопросы по теме веры и литературы.
            Ты бот, а не друг. Общайся вежливо и тепло, но в рамках своей роли консультанта.
            — Отвечай кратко — 1-3 предложения
            — Если вопрос выходит за рамки книг и веры, мягко объясни свою роль
            — Используй историю переписки, чтобы не повторяться
            — Библейский стих — только если {firstName} явно переживает что-то серьёзное, только по Синодальному переводу
            — Не заканчивай каждое сообщение ободрением или пожеланиями
            """;
        try
        {
            return await _groq.AskWithHistoryAsync(system, userText, history, maxTokens: 512);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "[Bot] Groq недоступен при свободном диалоге");
            return "Сейчас сервис временно недоступен. Попробуй позже или спроси про книги 📚";
        }
    }

    private string BuildWelcomeMessage(string firstName) => $"""
        Привет, {firstName}! 👋

        Я Лио — бот книжного клуба «Лио». Помогаю подобрать книги по вопросам веры и жизни.

        Опиши, что тебя сейчас занимает — подберу подходящую книгу. Или воспользуйся кнопками ниже.
        """;

    private static string BuildHelpMessage() => """
        📖 <b>Что умеет Лио:</b>

        • Напиши своим словами что тебя волнует — подберу книгу
        • <b>По теме</b> — быстрый выбор: молитва, семья, трудности…
        • <b>Каталог</b> — все книги и аудио с фильтрами
        • <b>Книга дня</b> — одна книга каждый день
        • <b>Мои книги</b> — список прочитанного
        • <b>Рассылка</b> — утреннее вдохновение: ежедневно, раз в неделю или выключить
        • <code>/search молитва</code> — прямой поиск по слову
        • <code>/addbook URL</code> — добавить книгу (администраторы)
        • @LioBot <i>запрос</i> — поиск прямо из любого чата (inline)
        """;

    private string BuildMyBooksMessage(long telegramId)
    {
        var books = _db.GetReadBooks(telegramId);
        if (books.Count == 0)
            return "У тебя пока нет прочитанных книг.\n\nЧтобы добавить — нажми ✅ под рекомендацией или в карточке книги.";

        var list = string.Join("\n", books.Select((b, i) =>
        {
            var icon = b.Type == "audio" ? "🎧" : "📖";
            return $"{i + 1}. {icon} «{b.Title}» — {b.Author}";
        }));
        return $"📖 <b>Прочитанные книги ({books.Count}):</b>\n\n{list}";
    }

    private string BuildNotifySettingsMessage(long telegramId)
    {
        var user    = _db.GetUserByTelegramId(telegramId);
        var current = (user?.NotifyMode ?? "daily") switch
        {
            "daily"  => "📅 Каждый день",
            "weekly" => "📆 Раз в неделю (по понедельникам)",
            _        => "🔕 Выключена"
        };
        return $"⚙️ <b>Настройки утренней рассылки</b>\n\nСейчас: <b>{current}</b>\n\nВыбери режим:";
    }
}
