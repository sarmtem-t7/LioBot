using System.Collections.Concurrent;
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
    private readonly ClaudeService _claude;
    private readonly ContentImportService _importer;
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

    // Этапы веры для онбординга
    private static readonly (string Id, string Label, string Hint)[] FaithStages =
    {
        ("seeker",  "🌱 Ищу Бога",        "только знакомлюсь с верой"),
        ("new",     "✨ Недавно уверовал", "первые шаги в вере"),
        ("growing", "📖 Расту в вере",     "уже какое-то время с Богом"),
        ("mature",  "⛪ Давно в вере",     "зрелый христианин"),
    };

    // Интересы для онбординга (мульти-выбор)
    private static readonly (string Tag, string Label)[] OnboardingInterests =
    {
        ("молитва",       "🙏 Молитва"),
        ("семья",         "👨‍👩‍👧 Семья"),
        ("Библия",        "📖 Библия"),
        ("отношения",     "❤️ Отношения"),
        ("свидетельство", "💬 Свидетельство"),
        ("миссия",        "🌍 Миссия"),
        ("проповедь",     "📣 Проповедь"),
        ("воспитание",    "👶 Воспитание"),
        ("апологетика",   "🛡️ Апологетика"),
        ("история",       "⛪ История"),
    };

    public MessageHandler(
        DatabaseContext db,
        BookService bookService,
        ClaudeService claude,
        ContentImportService importer,
        IConfiguration configuration,
        ILogger<MessageHandler> logger)
    {
        _db = db;
        _bookService = bookService;
        _claude = claude;
        _importer = importer;
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
            // Удаляем все известные сообщения бота + брутфорс по диапазону
            // (чтобы не зависеть от in-memory трекера, который сбрасывается при рестарте)
            var userMsgId = message.MessageId;
            var tracked = BotMessageTracker.TakeAll(chatId);
            var idsToDelete = new HashSet<int>(tracked);
            for (var i = 1; i <= 60; i++) idsToDelete.Add(userMsgId - i);
            foreach (var id in idsToDelete)
                await TryDelete(bot, chatId, id, ct);

            await bot.SendChatAction(chatId, ChatAction.Typing, cancellationToken: ct);
            var readDelay = Math.Clamp(text.Length * 30, 600, 2500);
            await Task.Delay(readDelay + Random.Shared.Next(0, 400), ct);

            string reply;
            InlineKeyboardMarkup? keyboard = null;
            int? tempMsgId = null;
            var isCommand = text.StartsWith("/");

            if (text.StartsWith("/start"))
            {
                var existing = _db.GetUserByTelegramId(telegramUser.Id);
                if (existing is null || !existing.OnboardingDone)
                {
                    reply = BuildOnboardingStageMessage(telegramUser.FirstName);
                    keyboard = OnboardingStageKeyboard();
                }
                else
                {
                    reply = BuildWelcomeMessage(telegramUser.FirstName);
                    keyboard = MainMenuKeyboard();
                }
            }
            else if (text.StartsWith("/onboarding"))
            {
                reply = BuildOnboardingStageMessage(telegramUser.FirstName);
                keyboard = OnboardingStageKeyboard();
            }
            else if (text.StartsWith("/stats"))
            {
                if (_adminIds.Count == 0 || _adminIds.Contains(telegramUser.Id))
                {
                    reply = BuildStatsMessage();
                    keyboard = MainMenuKeyboard();
                }
                else
                {
                    reply = "Эта команда доступна только администраторам.";
                }
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
                var tmp = await bot.SendMessage(chatId, "Ищу... 📖", cancellationToken: ct);
                tempMsgId = tmp.MessageId;
                var result = await _bookService.RecommendBooksAsync(query, null, telegramUser.Id);
                reply = result.Text; keyboard = RecommendationKeyboard(result.Books);
            }
            else if (text == "/search")
            {
                reply = "Напиши что ищешь после команды:\n<code>/search молитва</code>\nили просто опиши ситуацию словами — я пойму.";
                keyboard = MainMenuKeyboard();
            }
            else if (text.StartsWith("/import"))
            {
                if (_adminIds.Count > 0 && !_adminIds.Contains(telegramUser.Id))
                {
                    reply = "Эта команда доступна только администраторам.";
                }
                else if (_importer.IsRunning)
                {
                    reply = "Импорт уже выполняется. Подожди, пока завершится текущий.";
                }
                else
                {
                    var arg = text.Replace("/import", "").Trim().ToLowerInvariant();
                    var startMsg = await bot.SendMessage(chatId,
                        "🔄 Запускаю импорт. Это займёт от пары минут до получаса — отвечу, когда закончу.",
                        cancellationToken: ct);
                    BotMessageTracker.Track(chatId, startMsg.MessageId);
                    _ = Task.Run(async () =>
                    {
                        try
                        {
                            var sw = System.Diagnostics.Stopwatch.StartNew();
                            int audio = 0, articles = 0, radio = 0, mags = 0, issues = 0;
                            switch (arg)
                            {
                                case "audio":     audio    = await _importer.ImportAudiobooksAsync(); break;
                                case "articles":  articles = await _importer.ImportArticlesAsync();   break;
                                case "radio":     radio    = await _importer.ImportRadioAsync();      break;
                                case "magazines": mags     = await _importer.ImportMagazinesAsync();  break;
                                default:
                                    var summary = await _importer.ImportAllAsync();
                                    audio = summary.Audio; articles = summary.Articles;
                                    radio = summary.Radio; mags = summary.Magazines;
                                    issues = summary.Issues;
                                    break;
                            }
                            sw.Stop();
                            foreach (var oldId in BotMessageTracker.TakeAll(chatId))
                                await TryDelete(bot, chatId, oldId, CancellationToken.None);
                            var total = audio + articles + radio + mags + issues;
                            var doneMsg = await bot.SendMessage(chatId,
                                $"✅ Импорт завершён за {sw.Elapsed.TotalMinutes:F1} мин.\n" +
                                $"🎧 Аудио: +{audio}\n📰 Статьи: +{articles}\n" +
                                $"🎙 Радио: +{radio}\n📖 Журналы: +{mags}\n" +
                                $"📰 Выпуски: +{issues}\n\n" +
                                $"Итого новых: {total}",
                                cancellationToken: CancellationToken.None);
                            BotMessageTracker.Track(chatId, doneMsg.MessageId);
                        }
                        catch (Exception ex)
                        {
                            _logger.LogError(ex, "[Import] Ошибка");
                            var errMsg = await bot.SendMessage(chatId, $"❌ Импорт упал: {ex.Message}",
                                cancellationToken: CancellationToken.None);
                            BotMessageTracker.Track(chatId, errMsg.MessageId);
                        }
                    }, CancellationToken.None);
                    return;
                }
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
                        var tmp = await bot.SendMessage(chatId, "Загружаю книгу по ссылке... ⏳", cancellationToken: ct);
                        tempMsgId = tmp.MessageId;
                        reply = await _bookService.AddBookFromUrlAsync(url);
                    }
                }
            }
            else if (IsBookRequest(text))
            {
                var tmp = await bot.SendMessage(chatId, "Подбираю материалы... 📚", cancellationToken: ct);
                tempMsgId = tmp.MessageId;
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

            if (tempMsgId.HasValue)
                await TryDelete(bot, chatId, tempMsgId.Value, ct);

            var sentId = await SendMessage(bot, chatId, reply, keyboard, ct);
            BotMessageTracker.Track(chatId, sentId);
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

        BotMessageTracker.SetCurrent(chatId, messageId);

        try
        {
            // ── Журналы: список изданий и выпуски ─────────────────────
            if (data == "magazines:list")
            {
                var (text, kb) = BuildMagazinesList();
                await bot.AnswerCallbackQuery(query.Id, cancellationToken: ct);
                await bot.EditMessageText(chatId, messageId, text,
                    parseMode: ParseMode.Html,
                    replyMarkup: kb,
                    linkPreviewOptions: new() { IsDisabled = true },
                    cancellationToken: ct);
                return;
            }

            if (data.StartsWith("mag:issues:") && long.TryParse(data[11..], out var magId))
            {
                var (text, kb) = BuildMagazineIssues(magId);
                await bot.AnswerCallbackQuery(query.Id, cancellationToken: ct);
                await bot.EditMessageText(chatId, messageId, text,
                    parseMode: ParseMode.Html,
                    replyMarkup: kb,
                    linkPreviewOptions: new() { IsDisabled = true },
                    cancellationToken: ct);
                return;
            }

            // ── Авторы: список с пагинацией ──────────────────────────
            if (data.StartsWith("authors:"))
            {
                var page = int.TryParse(data[8..], out var ap) ? ap : 0;
                var (text, kb) = BuildAuthorsPage(page);
                await bot.AnswerCallbackQuery(query.Id, cancellationToken: ct);
                await bot.EditMessageText(chatId, messageId, text,
                    parseMode: ParseMode.Html,
                    replyMarkup: kb,
                    linkPreviewOptions: new() { IsDisabled = true },
                    cancellationToken: ct);
                return;
            }

            // ── Карточка автора: его материалы ───────────────────────
            if (data.StartsWith("author:"))
            {
                var slug = data[7..];
                var (text, kb) = BuildAuthorCard(slug);
                await bot.AnswerCallbackQuery(query.Id, cancellationToken: ct);
                await bot.EditMessageText(chatId, messageId, text,
                    parseMode: ParseMode.Html,
                    replyMarkup: kb,
                    linkPreviewOptions: new() { IsDisabled = true },
                    cancellationToken: ct);
                return;
            }

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
                    replyMarkup: BookCardKeyboard(book, user.Id),
                    linkPreviewOptions: new() { IsDisabled = true },
                    cancellationToken: ct);
                return;
            }

            // ── Отметить прочитанным + предложить оценку ──────────────
            if (data.StartsWith("book:read:") && long.TryParse(data[10..], out var readId))
            {
                _db.MarkBookAsRead(user.Id, readId);
                _db.StopReading(user.Id, readId);
                var readBook = _bookService.GetBookById(readId);
                var doneVerb = readBook?.Type is "audio" or "radio" ? "прослушанных" : "прочитанных";
                await bot.AnswerCallbackQuery(query.Id, $"✅ Добавлено в список {doneVerb}!", cancellationToken: ct);

                var existingRating = _db.GetRating(user.Id, readId);
                if (existingRating is null)
                {
                    var book = _bookService.GetBookById(readId);
                    var title = book != null ? BookService.EscapeHtml(book.Title) : "книгу";
                    await bot.SendMessage(chatId,
                        $"⭐ Оцени «{title}» — это поможет подбирать книги точнее:",
                        parseMode: ParseMode.Html,
                        replyMarkup: RatingKeyboard(readId),
                        cancellationToken: ct);
                }
                return;
            }

            // ── Оценка книги (1..5 или пропуск) ───────────────────────
            if (data.StartsWith("book:rate:"))
            {
                var tail = data[10..].Split(':');
                if (tail.Length == 2 && long.TryParse(tail[0], out var rateBookId))
                {
                    if (tail[1] == "skip")
                    {
                        await bot.AnswerCallbackQuery(query.Id, "Пропущено", cancellationToken: ct);
                        try
                        {
                            await bot.EditMessageText(chatId, messageId,
                                "⭐ Хорошо, без оценки.",
                                parseMode: ParseMode.Html,
                                cancellationToken: ct);
                        }
                        catch { /* сообщение могло устареть */ }
                        return;
                    }
                    if (int.TryParse(tail[1], out var rating) && rating is >= 1 and <= 5)
                    {
                        _db.SetRating(user.Id, rateBookId, rating);
                        var stars = new string('⭐', rating);
                        await bot.AnswerCallbackQuery(query.Id, $"Спасибо! {stars}", cancellationToken: ct);
                        try
                        {
                            await bot.EditMessageText(chatId, messageId,
                                $"⭐ Твоя оценка: {stars}\nСпасибо — теперь подборка станет точнее.",
                                parseMode: ParseMode.Html,
                                cancellationToken: ct);
                        }
                        catch { /* сообщение могло устареть */ }
                        return;
                    }
                }
                await bot.AnswerCallbackQuery(query.Id, cancellationToken: ct);
                return;
            }

            // ── Начал читать ──────────────────────────────────────────
            if (data.StartsWith("book:reading:") && long.TryParse(data[13..], out var readingId))
            {
                _db.MarkBookAsReading(user.Id, readingId);
                var rdBook = _bookService.GetBookById(readingId);
                var inProgressLabel = rdBook?.Type is "audio" or "radio" ? "Сейчас слушаю" : "Сейчас читаю";
                await bot.AnswerCallbackQuery(query.Id, $"📖 Добавлено в «{inProgressLabel}»", cancellationToken: ct);
                return;
            }

            // ── Отложить книгу ────────────────────────────────────────
            if (data.StartsWith("book:stopreading:") && long.TryParse(data[17..], out var stopId))
            {
                _db.StopReading(user.Id, stopId);
                await bot.AnswerCallbackQuery(query.Id, "⏸ Отложена", cancellationToken: ct);
                return;
            }

            // ── Скрыть книгу ───────────────────────────────────────────
            if (data.StartsWith("book:ignore:") && long.TryParse(data[12..], out var ignoreId))
            {
                _db.MarkBookAsIgnored(user.Id, ignoreId);
                _db.MarkBooksAsSeen(user.Id, [ignoreId]);
                await bot.AnswerCallbackQuery(query.Id, "❌ Скрыто из рекомендаций", cancellationToken: ct);
                return;
            }

            // ── Профиль: просмотр ─────────────────────────────────────
            if (data == "profile:view")
            {
                await bot.AnswerCallbackQuery(query.Id, cancellationToken: ct);
                await bot.EditMessageText(chatId, messageId,
                    BuildProfileMessage(user.Id),
                    parseMode: ParseMode.Html,
                    replyMarkup: ProfileKeyboard(),
                    cancellationToken: ct);
                return;
            }

            // ── Профиль: сменить этап ─────────────────────────────────
            if (data == "profile:editstage")
            {
                await bot.AnswerCallbackQuery(query.Id, cancellationToken: ct);
                await bot.EditMessageText(chatId, messageId,
                    "Выбери этап веры:",
                    parseMode: ParseMode.Html,
                    replyMarkup: ProfileStageKeyboard(),
                    cancellationToken: ct);
                return;
            }

            // ── Профиль: сохранить этап → обратно к профилю ───────────
            if (data.StartsWith("profile:setstage:"))
            {
                var stage = data[17..];
                if (FaithStages.Any(s => s.Id == stage))
                {
                    _db.SetFaithStage(user.Id, stage);
                    await bot.AnswerCallbackQuery(query.Id, "Сохранено", cancellationToken: ct);
                    await bot.EditMessageText(chatId, messageId,
                        BuildProfileMessage(user.Id),
                        parseMode: ParseMode.Html,
                        replyMarkup: ProfileKeyboard(),
                        cancellationToken: ct);
                    return;
                }
            }

            // ── Профиль: редактировать интересы ───────────────────────
            if (data == "profile:editinterests")
            {
                await bot.AnswerCallbackQuery(query.Id, cancellationToken: ct);
                await bot.EditMessageText(chatId, messageId,
                    "Отметь свои интересы (можно несколько), потом нажми «Готово»:",
                    parseMode: ParseMode.Html,
                    replyMarkup: ProfileInterestsKeyboard(user.Id),
                    cancellationToken: ct);
                return;
            }

            // ── Профиль: тоггл интереса ───────────────────────────────
            if (data.StartsWith("profile:toggle:"))
            {
                var tag = data[15..];
                _db.ToggleInterest(user.Id, tag);
                await bot.AnswerCallbackQuery(query.Id, cancellationToken: ct);
                try
                {
                    await bot.EditMessageReplyMarkup(chatId, messageId,
                        ProfileInterestsKeyboard(user.Id), cancellationToken: ct);
                }
                catch { /* без изменений */ }
                return;
            }

            // ── Профиль: сохранить интересы → обратно к профилю ───────
            if (data == "profile:save")
            {
                await bot.AnswerCallbackQuery(query.Id, "Сохранено", cancellationToken: ct);
                await bot.EditMessageText(chatId, messageId,
                    BuildProfileMessage(user.Id),
                    parseMode: ParseMode.Html,
                    replyMarkup: ProfileKeyboard(),
                    cancellationToken: ct);
                return;
            }

            // ── Онбординг: выбор этапа веры ───────────────────────────
            if (data.StartsWith("onb:stage:"))
            {
                var stage = data[10..];
                if (FaithStages.Any(s => s.Id == stage))
                {
                    _db.SetFaithStage(user.Id, stage);
                    await bot.AnswerCallbackQuery(query.Id, cancellationToken: ct);
                    await bot.EditMessageText(chatId, messageId,
                        BuildOnboardingInterestsMessage(),
                        parseMode: ParseMode.Html,
                        replyMarkup: OnboardingInterestsKeyboard(user.Id),
                        cancellationToken: ct);
                    return;
                }
            }

            // ── Онбординг: переключение интереса ──────────────────────
            if (data.StartsWith("onb:interest:"))
            {
                var tag = data[13..];
                _db.ToggleInterest(user.Id, tag);
                await bot.AnswerCallbackQuery(query.Id, cancellationToken: ct);
                try
                {
                    await bot.EditMessageReplyMarkup(chatId, messageId,
                        OnboardingInterestsKeyboard(user.Id), cancellationToken: ct);
                }
                catch { /* без изменений — игнорируем */ }
                return;
            }

            // ── Онбординг: завершение или пропуск ─────────────────────
            if (data == "onb:done" || data == "onb:skip")
            {
                _db.MarkOnboardingDone(user.Id);
                await bot.AnswerCallbackQuery(query.Id,
                    data == "onb:skip" ? "Пропущено" : "Готово!", cancellationToken: ct);
                await bot.EditMessageText(chatId, messageId,
                    BuildOnboardingDoneMessage(user.FirstName, user.Id),
                    parseMode: ParseMode.Html,
                    replyMarkup: MainMenuKeyboard(),
                    cancellationToken: ct);
                return;
            }

            // ── Полная аннотация книги (AI) ───────────────────────────
            if (data.StartsWith("book:annotate:") && long.TryParse(data[14..], out var annotateId))
            {
                var book = _bookService.GetBookById(annotateId);
                if (book == null)
                {
                    await bot.AnswerCallbackQuery(query.Id, "Книга не найдена", cancellationToken: ct);
                    return;
                }
                await bot.AnswerCallbackQuery(query.Id, cancellationToken: ct);
                await bot.SendChatAction(chatId, ChatAction.Typing, cancellationToken: ct);
                try
                {
                    var annotation = await _bookService.AnnotateBookAsync(book);
                    var icon = book.Type == "audio" ? "🎧" : "📖";
                    var text = $"{icon} <b>«{BookService.EscapeHtml(book.Title)}»</b> — {BookService.EscapeHtml(book.Author)}\n\n{BookService.EscapeHtml(annotation)}";
                    await ReplaceMessage(bot, chatId, messageId, text, BookCardKeyboard(book, user.Id), ct);
                }
                catch (Exception ex)
                {
                    _logger.LogWarning(ex, "[Bot] Не удалось получить аннотацию");
                    await bot.SendMessage(chatId,
                        "Не смог получить подробную аннотацию сейчас. Попробуй позже 🙏",
                        cancellationToken: ct);
                }
                return;
            }

            // ── Похожие книги ──────────────────────────────────────────
            if (data.StartsWith("book:similar:") && long.TryParse(data[13..], out var similarId))
            {
                await bot.AnswerCallbackQuery(query.Id, cancellationToken: ct);
                await bot.SendChatAction(chatId, ChatAction.Typing, cancellationToken: ct);
                var result = await _bookService.GetSimilarBooksAsync(similarId, user.Id);
                await ReplaceMessage(bot, chatId, messageId, result.Text, RecommendationKeyboard(result.Books), ct);
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
                await ReplaceMessage(bot, chatId, messageId, "Подбираю материалы... 📚", null, ct);
                var history = _db.GetHistory(user.Id, limit: 10);
                var result  = await _bookService.RecommendBooksAsync(tag, history, user.Id);
                _db.SaveMessage(user.Id, "user",      $"Тема: {tag}");
                _db.SaveMessage(user.Id, "assistant", result.Text);
                await ReplaceMessage(bot, chatId, messageId, result.Text, RecommendationKeyboard(result.Books), ct);
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
                    await ReplaceMessage(bot, chatId, messageId, "Подбираю материалы... 📚", null, ct);
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

                case "menu:profile":
                    reply    = BuildProfileMessage(user.Id);
                    keyboard = ProfileKeyboard();
                    break;

                case "book:random":
                    var dayBook = _bookService.GetBookOfDay();
                    if (dayBook == null) { reply = "Каталог пока пуст 📚"; break; }
                    reply    = $"🎲 <b>Книга дня:</b>\n\n{BookService.FormatBookCard(dayBook)}";
                    keyboard = BookCardKeyboard(dayBook, user.Id);
                    break;

                case "recommend:more":
                    await ReplaceMessage(bot, chatId, messageId, "Ищу другие варианты... 📚", null, ct);
                    var moreHistory = _db.GetHistory(user.Id, limit: 20);
                    var moreResult  = await _bookService.RecommendBooksAsync("посоветуй другие книги", moreHistory, user.Id);
                    _db.SaveMessage(user.Id, "user",      "Посоветуй другие книги");
                    _db.SaveMessage(user.Id, "assistant", moreResult.Text);
                    reply    = moreResult.Text;
                    keyboard = RecommendationKeyboard(moreResult.Books);
                    break;

                case "menu:back":
                    await bot.EditMessageText(chatId, messageId,
                        "👋 Выбери что тебя интересует:",
                        replyMarkup: MainMenuKeyboard(),
                        cancellationToken: ct);
                    return;

                default:
                    return;
            }

            await ReplaceMessage(bot, chatId, messageId, reply, keyboard, ct);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "[Bot] Ошибка при обработке callback от {User}", user.FirstName);
        }
    }

    // ════════════════════════════════════════════════════════════
    // Inline Query
    // ════════════════════════════════════════════════════════════

    // Префиксы фильтра по типу: "статьи:семья", "радио:", "книги:молитва"
    private static readonly Dictionary<string, string> InlineTypePrefixes = new(StringComparer.OrdinalIgnoreCase)
    {
        ["книги"]    = "book",
        ["аудио"]    = "audio",
        ["статьи"]   = "article",
        ["журналы"]  = "magazine",
        ["радио"]    = "radio"
    };

    private async Task HandleInlineQueryAsync(ITelegramBotClient bot, InlineQuery query, CancellationToken ct)
    {
        var searchText = query.Query.Trim();

        // Парсим необязательный префикс типа
        string? typeFilter = null;
        var colon = searchText.IndexOf(':');
        if (colon > 0)
        {
            var prefix = searchText[..colon].Trim();
            if (InlineTypePrefixes.TryGetValue(prefix, out var t))
            {
                typeFilter = t;
                searchText = searchText[(colon + 1)..].Trim();
            }
        }

        List<Book> items;
        if (string.IsNullOrEmpty(searchText))
        {
            items = (typeFilter == null ? _bookService.GetAllBooks() : _bookService.GetByType(typeFilter))
                .OrderBy(_ => Guid.NewGuid()).Take(8).ToList();
        }
        else
        {
            items = _bookService.SearchBooks(searchText);
            if (typeFilter != null) items = items.Where(b => b.Type == typeFilter).ToList();
            items = items.Take(8).ToList();
        }

        var results = items.Select(book =>
        {
            var icon     = BookService.IconFor(book.Type);
            var authorPart = string.IsNullOrWhiteSpace(book.Author)
                ? ""
                : $" — {BookService.EscapeHtml(book.Author)}";
            var linkPart = string.IsNullOrEmpty(book.Url)
                ? ""
                : $"\n\n<a href=\"{book.Url}\">→ {BookService.LinkLabelFor(book.Type)}</a>";
            var msgText  =
                $"{icon} <b>«{BookService.EscapeHtml(book.Title)}»</b>{authorPart}\n\n" +
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

        // При пустом запросе показываем подсказку про префиксы типа в шапке
        var pmButton = string.IsNullOrEmpty(query.Query)
            ? new InlineQueryResultsButton { Text = "💡 Совет: «статьи:тема», «радио:», «книги:молитва»", StartParameter = "help" }
            : null;

        await bot.AnswerInlineQuery(query.Id, results, cacheTime: 60,
            button: pmButton, cancellationToken: ct);
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
        var filtered = typeFilter == "all" ? all : all.Where(b => b.Type == typeFilter).ToList();

        var emptyLabels = new Dictionary<string, string>
        {
            ["book"]     = "книг",
            ["audio"]    = "аудиокниг",
            ["article"]  = "статей",
            ["magazine"] = "журналов",
            ["radio"]    = "радио-станций"
        };
        if (filtered.Count == 0)
        {
            var empty = emptyLabels.TryGetValue(typeFilter, out var e) ? e : "материалов";
            return ($"В каталоге пока нет {empty}. 📚", MainMenuKeyboard());
        }

        var totalPages = (int)Math.Ceiling(filtered.Count / (double)CatalogPageSize);
        page = Math.Clamp(page, 0, totalPages - 1);
        var pageBooks = filtered.Skip(page * CatalogPageSize).Take(CatalogPageSize).ToList();

        var filterLabel = typeFilter switch
        {
            "book"     => "📖 Книги",
            "audio"    => "🎧 Аудиокниги",
            "article"  => "📰 Статьи",
            "magazine" => "📖 Журналы",
            "radio"    => "🎙 Радио",
            _          => "📚 Все материалы"
        };
        var text = $"{filterLabel} (стр. {page + 1}/{totalPages}) — нажми для подробностей:";

        var buttons = new List<InlineKeyboardButton[]>();

        foreach (var b in pageBooks)
        {
            var label = $"{BookService.IconFor(b.Type)} {b.Title}";
            if (label.Length > 48) label = label[..48] + "…";
            buttons.Add([InlineKeyboardButton.WithCallbackData(label, $"book:card:{b.Id}")]);
        }

        // Навигация по страницам
        var navRow = new List<InlineKeyboardButton>();
        if (page > 0)
            navRow.Add(InlineKeyboardButton.WithCallbackData("←", $"catalog:{typeFilter}:{page - 1}"));
        navRow.Add(InlineKeyboardButton.WithCallbackData(
            $"стр. {page + 1}/{totalPages}", $"catalog:{typeFilter}:{page}"));
        if (page < totalPages - 1)
            navRow.Add(InlineKeyboardButton.WithCallbackData("→", $"catalog:{typeFilter}:{page + 1}"));
        buttons.Add(navRow.ToArray());

        // Переключатели типа — два ряда по 3
        string Mark(string t, string label) => typeFilter == t ? label + " ✓" : label;
        buttons.Add(new[]
        {
            InlineKeyboardButton.WithCallbackData(Mark("all",   "📚 Все"),     "catalog:all:0"),
            InlineKeyboardButton.WithCallbackData(Mark("book",  "📖 Книги"),   "catalog:book:0"),
            InlineKeyboardButton.WithCallbackData(Mark("audio", "🎧 Аудио"),   "catalog:audio:0")
        });
        buttons.Add(new[]
        {
            InlineKeyboardButton.WithCallbackData(Mark("article",  "📰 Статьи"),  "catalog:article:0"),
            InlineKeyboardButton.WithCallbackData(Mark("magazine", "📖 Журналы"), "magazines:list"),
            InlineKeyboardButton.WithCallbackData(Mark("radio",    "🎙 Радио"),   "catalog:radio:0")
        });

        buttons.Add([InlineKeyboardButton.WithCallbackData("🤖 Подбери мне", "menu:recommend")]);
        buttons.Add([HomeButton()]);

        return (text, new InlineKeyboardMarkup(buttons));
    }

    // ════════════════════════════════════════════════════════════
    // Авторы — список и карточка
    // ════════════════════════════════════════════════════════════

    private const int AuthorsPageSize = 12;

    // token → author name. Заполняется лениво на построении каждой страницы;
    // нужен, потому что callback_data ограничено 64 байтами и UTF-8 имена
    // могут не влезть. Выживает все время работы процесса.
    private static readonly Dictionary<string, string> _authorTokens = new();

    private static string AuthorToken(string author)
    {
        var bytes = System.Text.Encoding.UTF8.GetBytes(author);
        var hash = System.Security.Cryptography.SHA1.HashData(bytes);
        var token = Convert.ToHexString(hash, 0, 8).ToLowerInvariant();
        _authorTokens[token] = author;
        return token;
    }

    private (string Text, InlineKeyboardMarkup Keyboard) BuildAuthorsPage(int page)
    {
        var total = _db.CountAuthors();
        if (total == 0)
            return ("Авторы пока не определены — у материалов не указано имя автора.", MainMenuKeyboard());

        var totalPages = (int)Math.Ceiling(total / (double)AuthorsPageSize);
        page = Math.Clamp(page, 0, totalPages - 1);
        var rows = _db.GetTopAuthors(AuthorsPageSize, page * AuthorsPageSize);

        var text = $"👤 <b>Авторы</b> (стр. {page + 1}/{totalPages}) — нажми, чтобы открыть материалы:";
        var buttons = new List<InlineKeyboardButton[]>();
        foreach (var (author, count) in rows)
        {
            var label = author.Length > 40 ? author[..40] + "…" : author;
            buttons.Add([InlineKeyboardButton.WithCallbackData(
                $"{label} · {count}", $"author:{AuthorToken(author)}")]);
        }

        var nav = new List<InlineKeyboardButton>();
        if (page > 0) nav.Add(InlineKeyboardButton.WithCallbackData("←", $"authors:{page - 1}"));
        nav.Add(InlineKeyboardButton.WithCallbackData($"стр. {page + 1}/{totalPages}", $"authors:{page}"));
        if (page < totalPages - 1) nav.Add(InlineKeyboardButton.WithCallbackData("→", $"authors:{page + 1}"));
        buttons.Add(nav.ToArray());
        buttons.Add([HomeButton()]);

        return (text, new InlineKeyboardMarkup(buttons));
    }

    private (string Text, InlineKeyboardMarkup Keyboard) BuildAuthorCard(string token)
    {
        if (!_authorTokens.TryGetValue(token, out var author))
        {
            // Промахнулись по кэшу (рестарт процесса) — ленивый разогрев:
            // прогоняем всех авторов через AuthorToken, заодно ища нужного.
            foreach (var (name, _) in _db.GetTopAuthors())
                if (AuthorToken(name) == token) { author = name; break; }
        }
        if (author is null)
            return ("Автор не найден — открой список заново.", new InlineKeyboardMarkup(new[]
            {
                new[] { InlineKeyboardButton.WithCallbackData("👤 К списку авторов", "authors:0") },
                new[] { HomeButton() }
            }));

        var items = _db.GetByAuthor(author, limit: 50);
        var sb = new System.Text.StringBuilder();
        sb.Append("👤 <b>").Append(BookService.EscapeHtml(author)).Append("</b>\n");
        sb.Append("Материалов: <b>").Append(items.Count).Append("</b>\n\n");
        sb.Append("Нажми на материал, чтобы открыть карточку:");

        var buttons = new List<InlineKeyboardButton[]>();
        foreach (var b in items.Take(20))
        {
            var label = $"{BookService.IconFor(b.Type)} {b.Title}";
            if (label.Length > 48) label = label[..48] + "…";
            buttons.Add([InlineKeyboardButton.WithCallbackData(label, $"book:card:{b.Id}")]);
        }
        buttons.Add(new[]
        {
            InlineKeyboardButton.WithCallbackData("👤 К списку авторов", "authors:0"),
            HomeButton()
        });
        return (sb.ToString(), new InlineKeyboardMarkup(buttons));
    }

    // ════════════════════════════════════════════════════════════
    // Журналы — список изданий и выпуски
    // ════════════════════════════════════════════════════════════

    private (string Text, InlineKeyboardMarkup Keyboard) BuildMagazinesList()
    {
        var mags = _db.GetAllMagazines();
        // Показываем только журналы, у которых есть выпуски
        var withIssues = mags
            .Select(m => (m.Id, m.Slug, m.Title, m.Url, Count: _db.GetMagazineIssues(m.Id).Count))
            .Where(m => m.Count > 0)
            .ToList();

        if (withIssues.Count == 0)
            return ("Журналы ещё не загружены. Запусти /import.", MainMenuKeyboard());

        var buttons = new List<InlineKeyboardButton[]>();
        foreach (var (id, slug, title, url, count) in withIssues)
            buttons.Add([InlineKeyboardButton.WithCallbackData(
                $"📖 {title} · {count} выпусков", $"mag:issues:{id}")]);
        buttons.Add([HomeButton()]);
        return ("📖 <b>Журналы</b>\n\nВыбери издание:", new InlineKeyboardMarkup(buttons));
    }

    private (string Text, InlineKeyboardMarkup Keyboard) BuildMagazineIssues(long magazineId)
    {
        var issues = _db.GetMagazineIssues(magazineId);
        if (issues.Count == 0)
            return ("Выпуски ещё не загружены.", new InlineKeyboardMarkup(new[]
            {
                new[] { InlineKeyboardButton.WithCallbackData("📖 К журналам", "magazines:list") },
                new[] { HomeButton() }
            }));

        var sb = new System.Text.StringBuilder();
        sb.Append("📖 <b>Выпуски</b> — нажми, чтобы читать:");

        var buttons = new List<InlineKeyboardButton[]>();
        foreach (var (id, title, url, coverUrl, releasedAt) in issues.Take(30))
        {
            if (!string.IsNullOrEmpty(url) && url.StartsWith("http"))
                buttons.Add([InlineKeyboardButton.WithUrl($"📖 {title} — Читать", url)]);
            else
                buttons.Add([InlineKeyboardButton.WithCallbackData($"📖 {title}", $"mag:issues:{magazineId}")]);
        }
        buttons.Add(new[]
        {
            InlineKeyboardButton.WithCallbackData("📖 К журналам", "magazines:list"),
            HomeButton()
        });
        return (sb.ToString(), new InlineKeyboardMarkup(buttons));
    }

    // ════════════════════════════════════════════════════════════
    // Клавиатуры
    // ════════════════════════════════════════════════════════════

    private static InlineKeyboardButton HomeButton() =>
        InlineKeyboardButton.WithCallbackData("🏠 На главную", "menu:back");

    private static InlineKeyboardMarkup MainMenuKeyboard() => new(new[]
    {
        new[] { InlineKeyboardButton.WithCallbackData("🤖 Подбери материал", "menu:recommend") },
        new[]
        {
            InlineKeyboardButton.WithCallbackData("📖 Книги",  "catalog:book:0"),
            InlineKeyboardButton.WithCallbackData("🎧 Аудио",  "catalog:audio:0"),
            InlineKeyboardButton.WithCallbackData("📰 Статьи", "catalog:article:0")
        },
        new[]
        {
            InlineKeyboardButton.WithCallbackData("🎙 Радио",   "catalog:radio:0"),
            InlineKeyboardButton.WithCallbackData("📖 Журналы", "magazines:list"),
            InlineKeyboardButton.WithCallbackData("🎲 Случайное","book:random")
        },
        new[] { InlineKeyboardButton.WithCallbackData("🏷️ По теме",     "menu:topics"),
                InlineKeyboardButton.WithCallbackData("👤 Авторы",       "authors:0") },
        new[] { InlineKeyboardButton.WithCallbackData("📌 Мои материалы","menu:mybooks"),
                InlineKeyboardButton.WithCallbackData("⚙️ Профиль",      "menu:profile") },
        new[] { InlineKeyboardButton.WithCallbackData("🔔 Рассылка",     "menu:notifications") }
    });

    private static InlineKeyboardMarkup RecommendationKeyboard(List<Book> books)
    {
        var rows = new List<InlineKeyboardButton[]>();
        for (var i = 0; i < books.Count; i++)
        {
            var book = books[i];
            var n    = i + 1;
            var doneLabel = book.Type is "audio" or "radio" ? "Прослушал" : "Прочитал";
            rows.Add([
                InlineKeyboardButton.WithCallbackData($"📝 Подробнее #{n}", $"book:annotate:{book.Id}")
            ]);
            rows.Add([
                InlineKeyboardButton.WithCallbackData($"✅ {doneLabel} #{n}", $"book:read:{book.Id}"),
                InlineKeyboardButton.WithCallbackData($"❌ Скрыть #{n}",      $"book:ignore:{book.Id}")
            ]);
        }
        rows.Add([InlineKeyboardButton.WithCallbackData("🔄 Другие варианты", "recommend:more")]);
        rows.Add([
            InlineKeyboardButton.WithCallbackData("📋 Каталог",    "catalog:all:0"),
            HomeButton()
        ]);
        return new InlineKeyboardMarkup(rows);
    }

    private static (string Done, string InProgress, string Pause, string Link) ActionLabels(string? type) => type switch
    {
        "audio" => ("✅ Прослушал", "🎧 Слушаю", "⏸ Отложить", "🎧 Слушать"),
        "article" => ("✅ Прочитал", "📰 Читаю", "⏸ Отложить", "📰 Читать"),
        "radio" => ("✅ Прослушал", "🎙 Слушаю", "⏸ Отложить", "🎙 Слушать"),
        _ => ("✅ Прочитал", "📖 Читаю", "⏸ Отложить", "📖 Читать"),
    };

    private InlineKeyboardMarkup BookCardKeyboard(Book book, long telegramId = 0)
    {
        var labels = ActionLabels(book.Type);
        var reading = telegramId > 0 && _db.GetReadingNow(telegramId).Any(b => b.Id == book.Id);
        var readingBtn = reading
            ? InlineKeyboardButton.WithCallbackData(labels.Pause,      $"book:stopreading:{book.Id}")
            : InlineKeyboardButton.WithCallbackData(labels.InProgress, $"book:reading:{book.Id}");

        var rows = new List<InlineKeyboardButton[]>
        {
            new[]
            {
                InlineKeyboardButton.WithCallbackData(labels.Done, $"book:read:{book.Id}"),
                readingBtn
            },
            new[]
            {
                InlineKeyboardButton.WithCallbackData("🔍 Похожие",    $"book:similar:{book.Id}"),
                InlineKeyboardButton.WithCallbackData("❌ Скрыть",     $"book:ignore:{book.Id}")
            }
        };
        if (!string.IsNullOrEmpty(book.Url))
            rows.Add([InlineKeyboardButton.WithUrl(labels.Link, book.Url)]);
        rows.Add([
            InlineKeyboardButton.WithCallbackData("← В каталог", "catalog:all:0"),
            HomeButton()
        ]);
        return new InlineKeyboardMarkup(rows);
    }

    private static InlineKeyboardMarkup RatingKeyboard(long bookId)
    {
        var stars = new InlineKeyboardButton[5];
        for (var i = 1; i <= 5; i++)
            stars[i - 1] = InlineKeyboardButton.WithCallbackData(i.ToString(), $"book:rate:{bookId}:{i}");
        return new InlineKeyboardMarkup(new[]
        {
            stars,
            new[] { InlineKeyboardButton.WithCallbackData("Пропустить", $"book:rate:{bookId}:skip") }
        });
    }

    private static InlineKeyboardMarkup ProfileKeyboard() => new(new[]
    {
        new[] { InlineKeyboardButton.WithCallbackData("🔄 Сменить этап",      "profile:editstage") },
        new[] { InlineKeyboardButton.WithCallbackData("🔄 Сменить интересы",  "profile:editinterests") },
        new[] { HomeButton() }
    });

    private static InlineKeyboardMarkup ProfileStageKeyboard()
    {
        var rows = FaithStages
            .Select(s => new[] { InlineKeyboardButton.WithCallbackData(s.Label, $"profile:setstage:{s.Id}") })
            .ToList();
        rows.Add([InlineKeyboardButton.WithCallbackData("← Назад к профилю", "profile:view")]);
        return new InlineKeyboardMarkup(rows);
    }

    private InlineKeyboardMarkup ProfileInterestsKeyboard(long telegramId)
    {
        var prefs    = _db.GetPreferences(telegramId);
        var selected = (prefs?.Interests ?? "")
            .Split(',', StringSplitOptions.RemoveEmptyEntries)
            .Select(s => s.Trim())
            .ToHashSet(StringComparer.OrdinalIgnoreCase);

        var rows = OnboardingInterests
            .Chunk(2)
            .Select(pair => pair
                .Select(t =>
                {
                    var on    = selected.Contains(t.Tag);
                    var label = on ? $"{t.Label} ✓" : t.Label;
                    return InlineKeyboardButton.WithCallbackData(label, $"profile:toggle:{t.Tag}");
                })
                .ToArray())
            .ToList();
        rows.Add([InlineKeyboardButton.WithCallbackData("✅ Готово", "profile:save")]);
        return new InlineKeyboardMarkup(rows);
    }

    private static InlineKeyboardMarkup OnboardingStageKeyboard()
    {
        var rows = FaithStages
            .Select(s => new[] { InlineKeyboardButton.WithCallbackData(s.Label, $"onb:stage:{s.Id}") })
            .ToList();
        rows.Add([InlineKeyboardButton.WithCallbackData("⏭ Пропустить", "onb:skip")]);
        return new InlineKeyboardMarkup(rows);
    }

    private InlineKeyboardMarkup OnboardingInterestsKeyboard(long telegramId)
    {
        var prefs    = _db.GetPreferences(telegramId);
        var selected = (prefs?.Interests ?? "")
            .Split(',', StringSplitOptions.RemoveEmptyEntries)
            .Select(s => s.Trim())
            .ToHashSet(StringComparer.OrdinalIgnoreCase);

        var rows = OnboardingInterests
            .Chunk(2)
            .Select(pair => pair
                .Select(t =>
                {
                    var on    = selected.Contains(t.Tag);
                    var label = on ? $"{t.Label} ✓" : t.Label;
                    return InlineKeyboardButton.WithCallbackData(label, $"onb:interest:{t.Tag}");
                })
                .ToArray())
            .ToList();
        rows.Add([InlineKeyboardButton.WithCallbackData("✅ Готово", "onb:done")]);
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
        topicRows.Add([HomeButton()]);
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
            new[] { HomeButton() }
        });
    }

    private static InlineKeyboardMarkup QuickActionsKeyboard() => new(new[]
    {
        new[] { InlineKeyboardButton.WithCallbackData("📚 Посоветуй книгу", "menu:recommend") },
        new[] { InlineKeyboardButton.WithCallbackData("🏷️ По теме",        "menu:topics"),
                InlineKeyboardButton.WithCallbackData("📋 Каталог",         "menu:catalog") },
        new[] { HomeButton() }
    });

    // ════════════════════════════════════════════════════════════
    // Утилиты отправки (защита от 4096 символов)
    // ════════════════════════════════════════════════════════════

    // Удаляет сообщение, глотая ошибки (старое / уже удалено / нет прав).
    private static async Task TryDelete(
        ITelegramBotClient bot, long chatId, int messageId, CancellationToken ct)
    {
        try { await bot.DeleteMessage(chatId, messageId, ct); }
        catch { /* not critical */ }
    }

    // Заменяет старое сообщение новым контентом: edit, если влезает,
    // иначе delete + send. Используется в callback-обработчиках, чтобы
    // нажатие кнопки не плодило новые сообщения в чате.
    private static async Task ReplaceMessage(
        ITelegramBotClient bot, long chatId, int oldMessageId,
        string text, InlineKeyboardMarkup? keyboard, CancellationToken ct)
    {
        const int MaxLen = 4000;
        if (text.Length <= MaxLen)
        {
            try
            {
                await bot.EditMessageText(chatId, oldMessageId, text,
                    parseMode: ParseMode.Html,
                    replyMarkup: keyboard,
                    linkPreviewOptions: new() { IsDisabled = true },
                    cancellationToken: ct);
                BotMessageTracker.SetCurrent(chatId, oldMessageId);
                return;
            }
            catch { /* старое сообщение / тот же контент / лимит — fall through */ }
        }
        await TryDelete(bot, chatId, oldMessageId, ct);
        var sentId = await SendMessage(bot, chatId, text, keyboard, ct);
        BotMessageTracker.SetCurrent(chatId, sentId);
    }

    private static async Task<int> SendMessage(
        ITelegramBotClient bot, long chatId, string text,
        InlineKeyboardMarkup? keyboard, CancellationToken ct)
    {
        const int MaxLen = 4000;
        if (text.Length <= MaxLen)
        {
            var msg = await bot.SendMessage(chatId, text,
                parseMode: ParseMode.Html,
                replyMarkup: keyboard,
                linkPreviewOptions: new() { IsDisabled = true },
                cancellationToken: ct);
            return msg.MessageId;
        }

        // Разбиваем на части по абзацам
        var chunks = SplitIntoChunks(text, MaxLen);
        int lastId = 0;
        for (var i = 0; i < chunks.Count; i++)
        {
            var isLast = i == chunks.Count - 1;
            var msg = await bot.SendMessage(chatId, chunks[i],
                parseMode: ParseMode.Html,
                replyMarkup: isLast ? keyboard : null,
                linkPreviewOptions: new() { IsDisabled = true },
                cancellationToken: ct);
            lastId = msg.MessageId;
            if (!isLast) await Task.Delay(200, ct);
        }
        return lastId;
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
            return await _claude.AskWithHistoryAsync(system, userText, history, maxTokens: 512);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "[Bot] Claude недоступен при свободном диалоге");
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

        • Напиши своим словами что тебя волнует — подберу материал
        • <b>По теме</b> — быстрый выбор: молитва, семья, трудности…
        • <b>Каталог</b> — книги, аудио, статьи, радио
        • <b>Мои материалы</b> — что сейчас читаю/слушаю и что завершил
        • <b>Рассылка</b> — утреннее вдохновение: ежедневно, раз в неделю или выключить
        • В карточке: «📝 Подробнее» / «📖 Читаю» или «🎧 Слушаю» / «✅ Прочитал/Прослушал»
        • <b>Профиль</b> — сменить этап веры или интересы в любой момент
        • <code>/search молитва</code> — прямой поиск по слову
        • @LioBot <i>запрос</i> — поиск прямо из любого чата (inline)
        """;

    private string BuildMyBooksMessage(long telegramId)
    {
        var reading = _db.GetReadingNow(telegramId);
        var read    = _db.GetReadBooks(telegramId);
        var ratings = _db.GetRatingsMap(telegramId);

        if (reading.Count == 0 && read.Count == 0)
            return "У тебя пока нет материалов в списке.\n\nВ карточке нажми «📖 Читаю» / «🎧 Слушаю», или «✅ Прочитал» / «✅ Прослушал» когда закончишь.";

        var sb = new System.Text.StringBuilder();

        // Ачивка по количеству завершённого
        var (badge, nextHint) = NextBadge(read.Count);
        if (!string.IsNullOrEmpty(badge))
        {
            sb.AppendLine(badge);
            if (!string.IsNullOrEmpty(nextHint)) sb.AppendLine($"<i>{nextHint}</i>");
            sb.AppendLine();
        }

        if (reading.Count > 0)
        {
            sb.AppendLine($"📖 <b>Сейчас в процессе ({reading.Count}):</b>");
            sb.AppendLine();
            foreach (var b in reading)
            {
                var line = $"{BookService.IconFor(b.Type)} «{BookService.EscapeHtml(b.Title)}»";
                if (!string.IsNullOrWhiteSpace(b.Author)) line += $" — {BookService.EscapeHtml(b.Author)}";
                sb.AppendLine(line);
            }
            if (read.Count > 0) sb.AppendLine();
        }

        if (read.Count > 0)
        {
            sb.AppendLine($"✅ <b>Завершено ({read.Count}):</b>");
            sb.AppendLine();
            for (var i = 0; i < read.Count; i++)
            {
                var b     = read[i];
                var stars = ratings.TryGetValue(b.Id, out var r) ? " " + new string('⭐', r) : "";
                var line  = $"{i + 1}. {BookService.IconFor(b.Type)} «{BookService.EscapeHtml(b.Title)}»";
                if (!string.IsNullOrWhiteSpace(b.Author)) line += $" — {BookService.EscapeHtml(b.Author)}";
                sb.AppendLine(line + stars);
            }
        }

        return sb.ToString().TrimEnd();
    }

    // Подбираем подходящий бейдж и подсказку про следующий рубеж.
    // Рубежи 5 / 10 / 25 / 50 / 100 — растущая шкала, чтобы было к чему стремиться.
    private static (string Badge, string NextHint) NextBadge(int finished)
    {
        var milestones = new[] { (5, "🌱", "Первые шаги"),
                                 (10, "📚", "Читатель"),
                                 (25, "🏅", "Опытный читатель"),
                                 (50, "🎓", "Знаток"),
                                 (100, "🏆", "Мастер слова") };

        (int Count, string Emoji, string Title)? earned = null;
        (int Count, string Emoji, string Title)? next = null;
        foreach (var m in milestones)
        {
            if (finished >= m.Item1) earned = (m.Item1, m.Item2, m.Item3);
            else { next = (m.Item1, m.Item2, m.Item3); break; }
        }

        var badge = earned is null ? "" : $"{earned.Value.Emoji} <b>{earned.Value.Title}</b> · {finished} завершено";
        var hint  = next is null
            ? (earned is null ? "" : "Все рубежи взяты — ты молодец!")
            : $"До «{next.Value.Title}» осталось {next.Value.Count - finished}.";
        return (badge, hint);
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

    // ════════════════════════════════════════════════════════════
    // Профиль
    // ════════════════════════════════════════════════════════════

    private string BuildProfileMessage(long telegramId)
    {
        var prefs = _db.GetPreferences(telegramId);
        var stage = FaithStages.FirstOrDefault(s => s.Id == (prefs?.FaithStage ?? "")).Label ?? "<i>не выбран</i>";

        var interests = string.IsNullOrWhiteSpace(prefs?.Interests)
            ? "<i>не выбраны</i>"
            : BookService.EscapeHtml(prefs!.Interests);

        return $"""
            ⚙️ <b>Твой профиль</b>

            <b>Этап веры:</b> {stage}
            <b>Интересы:</b> {interests}

            Эти данные помогают подбирать книги точнее. Можешь изменить в любой момент.
            """;
    }

    // ════════════════════════════════════════════════════════════
    // Онбординг
    // ════════════════════════════════════════════════════════════

    private static string BuildOnboardingStageMessage(string firstName) => $"""
        Привет, {firstName}! 👋

        Я Лио — бот книжного клуба «Лио». Прежде чем начать, расскажи немного о себе — подберу книги точнее.

        <b>На каком этапе твой путь веры?</b>
        """;

    private static string BuildOnboardingInterestsMessage() => """
        Отлично! 🙏

        <b>Что тебе сейчас интересно?</b>
        Отметь одну или несколько тем (можно выбрать несколько), потом нажми «Готово».
        """;

    private string BuildOnboardingDoneMessage(string firstName, long telegramId)
    {
        var prefs = _db.GetPreferences(telegramId);
        var stage = FaithStages.FirstOrDefault(s => s.Id == (prefs?.FaithStage ?? "")).Label ?? "—";
        var interests = string.IsNullOrWhiteSpace(prefs?.Interests) ? "не выбраны" : prefs!.Interests;

        return $"""
            Готово, {firstName}! ✨

            <b>Этап:</b> {stage}
            <b>Интересы:</b> {BookService.EscapeHtml(interests)}

            Теперь напиши, что тебя сейчас занимает, — подберу книгу. Или воспользуйся меню.
            """;
    }

    // ════════════════════════════════════════════════════════════
    // Статистика (админ)
    // ════════════════════════════════════════════════════════════

    private string BuildStatsMessage()
    {
        var users     = _db.GetUsersCount();
        var active7d  = _db.GetActiveUsersCount(TimeSpan.FromDays(7));
        var ratings   = _db.GetRatingsCount();
        var booksAll  = _db.GetBooksCount();
        var byType = new (string T, string Label)[]
        {
            ("book", "книг"), ("audio", "аудио"), ("article", "статей"),
            ("magazine", "журналов"), ("radio", "радио")
        };
        var counts = byType
            .Select(t => (t.Label, Count: _db.GetCountByType(t.T)))
            .Where(t => t.Count > 0)
            .Select(t => $"{t.Label}: {t.Count}");

        var topRated  = _db.GetTopRatedBooks(5, minVotes: 3);
        var topShown  = _db.GetTopRecommendedBooks(5);

        var sb = new System.Text.StringBuilder();
        sb.AppendLine("📊 <b>Статистика</b>");
        sb.AppendLine();
        sb.AppendLine($"👥 Пользователей: <b>{users}</b>");
        sb.AppendLine($"📈 Активных за 7 дней: <b>{active7d}</b>");
        sb.AppendLine($"⭐ Оценок: <b>{ratings}</b>");
        sb.AppendLine($"📚 Каталог: <b>{booksAll}</b> ({string.Join(", ", counts)})");

        if (topRated.Count > 0)
        {
            sb.AppendLine();
            sb.AppendLine("⭐ <b>Топ по оценкам</b> (мин. 3 голосов):");
            for (var i = 0; i < topRated.Count; i++)
            {
                var (t, avg, v) = topRated[i];
                sb.AppendLine($"{i + 1}. {avg:F1} — «{BookService.EscapeHtml(t)}» ({v})");
            }
        }

        if (topShown.Count > 0)
        {
            sb.AppendLine();
            sb.AppendLine("🔥 <b>Чаще всего рекомендовали:</b>");
            for (var i = 0; i < topShown.Count; i++)
            {
                var (t, c) = topShown[i];
                sb.AppendLine($"{i + 1}. «{BookService.EscapeHtml(t)}» ({c})");
            }
        }

        return sb.ToString().TrimEnd();
    }
}
