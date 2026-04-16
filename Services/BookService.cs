using System.Text;
using System.Text.Json;
using System.Text.RegularExpressions;
using LioBot.Data;
using LioBot.Models;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;

namespace LioBot.Services;

public class BookService
{
    private readonly DatabaseContext _db;
    private readonly ClaudeService _claude;
    private readonly HttpClient _http;
    private readonly ILogger<BookService> _logger;
    private readonly string _allowedBookDomain;

    public BookService(
        DatabaseContext db,
        ClaudeService claude,
        IHttpClientFactory httpClientFactory,
        IConfiguration configuration,
        ILogger<BookService> logger)
    {
        _db = db;
        _claude = claude;
        _http = httpClientFactory.CreateClient();
        _logger = logger;
        _allowedBookDomain = configuration["AllowedBookDomain"] ?? "lio-int.com";
    }

    // ────────────────────────────────────────────────────────────
    // Добавление книги по URL
    // ────────────────────────────────────────────────────────────

    public async Task<string> AddBookFromUrlAsync(string url)
    {
        if (!Uri.TryCreate(url, UriKind.Absolute, out var uri) ||
            (uri.Scheme != "https" && uri.Scheme != "http"))
            return "Некорректная ссылка. Укажи полный URL, начинающийся с https://";

        if (!uri.Host.EndsWith(_allowedBookDomain, StringComparison.OrdinalIgnoreCase))
            return $"Добавлять книги можно только с сайта {_allowedBookDomain}.";

        if (_db.BookExistsByUrl(url))
            return "Книга по этой ссылке уже есть в каталоге.";

        string html;
        try
        {
            _http.DefaultRequestHeaders.UserAgent.ParseAdd("Mozilla/5.0 (compatible; LioBot/1.0)");
            html = await _http.GetStringAsync(url);
        }
        catch (Exception ex)
        {
            return $"Не удалось загрузить страницу: {ex.Message}";
        }

        var text = Regex.Replace(html, "<[^>]+>", " ");
        text = Regex.Replace(text, @"\s{2,}", " ").Trim();
        if (text.Length > 3000) text = text[..3000];

        var systemPrompt = """
            Ты — помощник, который извлекает информацию о книге из текста веб-страницы.
            Верни ответ СТРОГО в формате (каждое поле на новой строке):
            НАЗВАНИЕ: <название книги>
            АВТОР: <автор книги>
            ОПИСАНИЕ: <2-3 предложения о содержании книги>
            ТЕГИ: <5-7 тегов через запятую, описывающих темы книги>
            Если какое-то поле не найдено — напиши "Не указано".
            Отвечай только на русском языке.
            """;

        string aiResponse;
        try
        {
            aiResponse = await _claude.AskAsync(systemPrompt, $"Текст страницы:\n{text}", maxTokens: 400);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "[BookService] AI недоступен при добавлении книги");
            return "Сервис временно недоступен. Попробуй чуть позже.";
        }

        var title       = ExtractField(aiResponse, "НАЗВАНИЕ");
        var author      = ExtractField(aiResponse, "АВТОР");
        var description = ExtractField(aiResponse, "ОПИСАНИЕ");
        var tags        = ExtractField(aiResponse, "ТЕГИ");

        if (string.IsNullOrWhiteSpace(title) || title == "Не указано")
            return "Не удалось определить название книги. Попробуй другую ссылку.";

        if (_db.BookExistsByTitle(title))
            return $"Книга «{title}» уже есть в каталоге.";

        var book = new Book
        {
            Title       = title,
            Author      = author == "Не указано" ? "" : author,
            Description = description,
            Tags        = tags,
            Url         = url
        };

        _db.AddBook(book);
        return $"✅ Книга добавлена!\n\n📖 «{book.Title}»\n👤 {book.Author}\n\n{book.Description}";
    }

    // ────────────────────────────────────────────────────────────
    // Рекомендации
    // ────────────────────────────────────────────────────────────

    public async Task<RecommendationResult> RecommendBooksAsync(
        string userRequest,
        IEnumerable<(string Role, string Content)>? history = null,
        long telegramId = 0)
    {
        var allBooks = _db.GetAllBooks();
        if (allBooks.Count == 0)
            return new RecommendationResult("К сожалению, в библиотеке пока нет книг. Мы уже работаем над этим! 📚", []);

        var excludeIds = new HashSet<long>();
        var lowRated   = new HashSet<long>();
        string prefsContext = "";

        if (telegramId > 0)
        {
            foreach (var id in _db.GetSeenBookIds(telegramId))    excludeIds.Add(id);
            foreach (var id in _db.GetIgnoredBookIds(telegramId)) excludeIds.Add(id);
            foreach (var (bookId, rating) in _db.GetRatingsMap(telegramId))
                if (rating <= 2) lowRated.Add(bookId);

            var prefs = _db.GetPreferences(telegramId);
            if (prefs != null)
            {
                var parts = new List<string>();
                if (!string.IsNullOrWhiteSpace(prefs.FaithStage))
                    parts.Add($"стадия в вере: {prefs.FaithStage}");
                if (!string.IsNullOrWhiteSpace(prefs.Interests))
                    parts.Add($"интересы: {prefs.Interests}");
                if (parts.Count > 0)
                    prefsContext = "\nО пользователе: " + string.Join("; ", parts);
            }
        }

        var historyContext = BuildHistoryContext(history);
        var stems = ExtractStems(userRequest + " " + historyContext);

        var candidates = allBooks
            .Where(b => !excludeIds.Contains(b.Id))
            .Select(b => new { Book = b, Score = ScoreBook(b, stems) })
            .Where(x => x.Score > 0)
            .OrderByDescending(x => x.Score)
            .Take(25)
            .Select(x => x.Book)
            .ToList();

        if (candidates.Count == 0)
            candidates = allBooks.Where(b => !excludeIds.Contains(b.Id))
                .OrderBy(_ => Guid.NewGuid()).Take(15).ToList();

        if (candidates.Count == 0)
            candidates = allBooks.OrderBy(_ => Guid.NewGuid()).Take(10).ToList();

        List<Book> selectedBooks;
        Dictionary<long, string> comments = new();
        try
        {
            var catalog = string.Join("\n", candidates.Select(b =>
                $"ID:{b.Id} | «{b.Title}» — {b.Author} | {b.Tags} | {TruncateDesc(b.Description, 120)}"));

            var avoidLine = lowRated.Count == 0 ? "" :
                $"\nИзбегай книг, похожих на те, что пользователь низко оценил (ID: {string.Join(",", lowRated)}).";

            var systemMsg = """
                Ты помощник христианского книжного клуба. Выбери 2 книги из списка, подходящие к запросу пользователя.
                Ответ — ТОЛЬКО JSON-массив объектов строго такого формата:
                [{"id": 12, "comment": "короткий комментарий почему эта книга подходит"},
                 {"id": 47, "comment": "..."}]
                Комментарий — 1-2 предложения, тёплый тон, без воды. Никакого текста вне JSON.
                """;

            var userMsg = $"Список книг:\n{catalog}\n\nЗапрос пользователя: {userRequest}{historyContext}{prefsContext}{avoidLine}";

            var raw = await _claude.AskAsync(systemMsg, userMsg, maxTokens: 500);
            var parsed = ParseRecommendations(raw);

            selectedBooks = parsed
                .Select(p => (Book: candidates.FirstOrDefault(b => b.Id == p.Id), p.Comment))
                .Where(x => x.Book != null)
                .Take(2)
                .Select(x => { comments[x.Book!.Id] = x.Comment; return x.Book!; })
                .ToList();
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "[BookService] AI недоступен, fallback на топ кандидатов");
            selectedBooks = [];
        }

        if (selectedBooks.Count == 0)
            selectedBooks = candidates.Take(2).ToList();

        // Подстрахуем пустые комментарии коротким описанием из каталога,
        // чтобы карточка книги никогда не была без пояснения.
        foreach (var b in selectedBooks)
        {
            if (!comments.TryGetValue(b.Id, out var c) || string.IsNullOrWhiteSpace(c))
                comments[b.Id] = TruncateDesc(b.Description ?? "", 180);
        }

        if (telegramId > 0)
            _db.MarkBooksAsSeen(telegramId, selectedBooks.Select(b => b.Id));

        return new RecommendationResult(
            FormatRecommendation("📚 Вот что нашёл:", selectedBooks, comments),
            selectedBooks);
    }

    // ────────────────────────────────────────────────────────────
    // Похожие книги
    // ────────────────────────────────────────────────────────────

    public async Task<RecommendationResult> GetSimilarBooksAsync(long bookId, long telegramId = 0)
    {
        var source = _db.GetBookById(bookId);
        if (source == null)
            return new RecommendationResult("Книга не найдена.", []);

        var excludeIds = new HashSet<long> { bookId };
        if (telegramId > 0)
            foreach (var id in _db.GetIgnoredBookIds(telegramId)) excludeIds.Add(id);

        var allBooks = _db.GetAllBooks().Where(b => !excludeIds.Contains(b.Id)).ToList();

        var sourceStems = ExtractStems(source.Tags + " " + source.Title);

        var candidates = allBooks
            .Select(b => new { Book = b, Score = ScoreBook(b, sourceStems) })
            .Where(x => x.Score > 0)
            .OrderByDescending(x => x.Score)
            .Take(20)
            .Select(x => x.Book)
            .ToList();

        if (candidates.Count == 0)
            candidates = allBooks.OrderBy(_ => Guid.NewGuid()).Take(5).ToList();

        if (telegramId > 0)
        {
            var seenIds = _db.GetSeenBookIds(telegramId);
            var fresh = candidates.Where(b => !seenIds.Contains(b.Id)).ToList();
            if (fresh.Count >= 2) candidates = fresh;
        }

        List<Book> selected;
        try
        {
            var catalog = string.Join("\n", candidates.Select(b =>
                $"ID:{b.Id} | «{b.Title}» — {b.Author} | {b.Tags}"));

            var idsRaw = await _claude.AskAsync(
                "Ты помощник христианского книжного клуба. Верни ТОЛЬКО JSON-массив из 2 целых чисел ID (например: [12, 47]). Без текста.",
                $"Список:\n{catalog}\n\nВыбери 2 книги, похожие на «{source.Title}» (теги: {source.Tags}).",
                maxTokens: 60);

            selected = ParseJsonIds(idsRaw)
                .Select(id => candidates.FirstOrDefault(b => b.Id == id))
                .Where(b => b != null)
                .Cast<Book>()
                .Take(2)
                .ToList();
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "[BookService] AI недоступен при поиске похожих");
            selected = [];
        }

        if (selected.Count == 0)
            selected = candidates.Take(2).ToList();

        if (telegramId > 0)
            _db.MarkBooksAsSeen(telegramId, selected.Select(b => b.Id));

        return new RecommendationResult(
            FormatBookList($"🔍 Похожие на «{EscapeHtml(source.Title)}»:", selected),
            selected);
    }

    // ────────────────────────────────────────────────────────────
    // Поиск
    // ────────────────────────────────────────────────────────────

    public List<Book> SearchBooks(string query)
    {
        var books = _db.GetAllBooks();
        if (string.IsNullOrWhiteSpace(query)) return books.Take(5).ToList();

        var stems = ExtractStems(query);
        if (stems.Count == 0) return new List<Book>();

        return books
            .Select(b => new { Book = b, Score = ScoreBook(b, stems) })
            .Where(x => x.Score > 0)
            .OrderByDescending(x => x.Score)
            .Select(x => x.Book)
            .ToList();
    }

    // ────────────────────────────────────────────────────────────
    // Книга дня (детерминированная по DayOfYear)
    // ────────────────────────────────────────────────────────────

    public Book? GetBookOfDay()
    {
        var books = _db.GetAllBooks();
        if (books.Count == 0) return null;
        return books[DateTime.UtcNow.DayOfYear % books.Count];
    }

    // ────────────────────────────────────────────────────────────
    // Вспомогательное
    // ────────────────────────────────────────────────────────────

    public Book? GetBookById(long id) => _db.GetBookById(id);
    public List<Book> GetAllBooks()   => _db.GetAllBooks();
    public List<Book> GetByType(string type, int? limit = null, int offset = 0) =>
        _db.GetByType(type, limit, offset);

    // ────────────────────────────────────────────────────────────
    // Иконки и подписи действий по типу контента
    // ────────────────────────────────────────────────────────────
    internal static string IconFor(string type) => type switch
    {
        "audio"    => "🎧",
        "article"  => "📰",
        "magazine" => "📖",
        "radio"    => "🎙",
        _          => "📚"
    };

    internal static string LinkLabelFor(string type) => type switch
    {
        "audio"    => "Слушать",
        "article"  => "Читать",
        "magazine" => "Открыть",
        "radio"    => "Слушать стрим",
        _          => "Читать"
    };

    private static string BuildHistoryContext(IEnumerable<(string Role, string Content)>? history)
    {
        if (history == null) return "";
        var msgs = history.Where(h => h.Role == "user").TakeLast(5).Select(h => h.Content);
        var joined = string.Join(" | ", msgs);
        return string.IsNullOrEmpty(joined) ? "" : $"\nКонтекст: {joined}";
    }

    // Форматирование с AI-комментариями к каждой книге (для рекомендаций)
    internal static string FormatRecommendation(
        string header, IEnumerable<Book> books, Dictionary<long, string> comments)
    {
        var sb = new StringBuilder();
        sb.AppendLine(header);
        sb.AppendLine();
        foreach (var book in books)
        {
            sb.Append(IconFor(book.Type));
            sb.Append($" <b>«{EscapeHtml(book.Title)}»</b>");
            if (!string.IsNullOrWhiteSpace(book.Author))
                sb.Append($" — {EscapeHtml(book.Author)}");
            sb.AppendLine();
            if (comments.TryGetValue(book.Id, out var comment) && !string.IsNullOrWhiteSpace(comment))
            {
                sb.Append($"<i>{EscapeHtml(comment)}</i>");
                sb.AppendLine();
            }
            if (!string.IsNullOrEmpty(book.Url))
                sb.Append($"<a href=\"{book.Url}\">→ {LinkLabelFor(book.Type)}</a>");
            sb.AppendLine("\n");
        }
        return sb.ToString().Trim();
    }

    // Полная аннотация — с кэшированием в БД. Каждая книга запрашивается
    // у AI максимум один раз; дальше — мгновенный возврат из базы.
    public async Task<string> AnnotateBookAsync(Book book)
    {
        var cached = _db.GetCachedAnnotation(book.Id);
        if (!string.IsNullOrWhiteSpace(cached))
            return cached;

        var system = """
            Ты помощник христианского книжного клуба. Составь развёрнутую аннотацию книги — такую, чтобы человек понял:
            1. О чём книга (ключевые темы и идеи, 2-3 абзаца).
            2. Кому она подойдёт (на каком этапе веры, в каких жизненных ситуациях).
            3. Чем она ценна (почему стоит прочесть).
            Тон — тёплый, как у друга в вере. Язык — русский. Без маркетингового пафоса.
            Максимум 7-8 предложений всего, без заголовков. Не повторяй название в начале.
            """;
        var user = $"""
            Название: «{book.Title}»
            Автор: {book.Author}
            Теги: {book.Tags}
            Короткое описание из каталога: {book.Description}
            """;
        try
        {
            var aiText = await _claude.AskAsync(system, user, maxTokens: 700);
            if (string.IsNullOrWhiteSpace(aiText))
            {
                _logger.LogWarning("[BookService] AI вернул пустой ответ для книги {Id}", book.Id);
                return BuildFallbackAnnotation(book);
            }
            // Сохраняем на будущее — одна успешная генерация на книгу, дальше из БД.
            try { _db.SaveCachedAnnotation(book.Id, aiText); }
            catch (Exception cacheEx) { _logger.LogWarning(cacheEx, "[BookService] Не удалось сохранить кэш аннотации"); }
            return aiText;
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "[BookService] AI недоступен для аннотации, fallback на описание из каталога");
            return BuildFallbackAnnotation(book);
        }
    }

    private static string BuildFallbackAnnotation(Book book)
    {
        var sb = new StringBuilder();
        if (!string.IsNullOrWhiteSpace(book.Description))
            sb.AppendLine(book.Description.Trim());
        if (!string.IsNullOrWhiteSpace(book.Tags))
        {
            if (sb.Length > 0) sb.AppendLine();
            sb.Append("Темы: ").Append(book.Tags.Trim()).Append('.');
        }
        if (sb.Length == 0)
            sb.Append("Развёрнутая аннотация сейчас недоступна, но книга точно достойна внимания.");
        return sb.ToString().Trim();
    }

    internal static string FormatBookList(string header, IEnumerable<Book> books)
    {
        var sb = new StringBuilder();
        sb.AppendLine(header);
        sb.AppendLine();
        foreach (var book in books)
        {
            sb.Append(IconFor(book.Type));
            sb.Append($" <b>«{EscapeHtml(book.Title)}»</b>");
            if (!string.IsNullOrWhiteSpace(book.Author))
                sb.Append($" — {EscapeHtml(book.Author)}");
            if (!string.IsNullOrEmpty(book.Description))
                sb.Append($"\n{EscapeHtml(TruncateDesc(book.Description, 100))}");
            if (!string.IsNullOrEmpty(book.Url))
                sb.Append($" <a href=\"{book.Url}\">→ {LinkLabelFor(book.Type)}</a>");
            sb.AppendLine("\n");
        }
        return sb.ToString().Trim();
    }

    internal static string FormatBookCard(Book book)
    {
        var sb = new StringBuilder();
        sb.AppendLine($"{IconFor(book.Type)} <b>«{EscapeHtml(book.Title)}»</b>");
        if (!string.IsNullOrEmpty(book.Author))
            sb.AppendLine($"👤 {EscapeHtml(book.Author)}");
        if (!string.IsNullOrEmpty(book.Description))
        {
            sb.AppendLine();
            sb.AppendLine(EscapeHtml(book.Description));
        }
        if (!string.IsNullOrEmpty(book.Tags))
        {
            sb.AppendLine();
            sb.AppendLine($"🏷️ {EscapeHtml(book.Tags)}");
        }
        return sb.ToString().Trim();
    }

    private static string ExtractField(string text, string field)
    {
        var match = Regex.Match(text, $@"{field}:\s*(.+?)(?:\n|$)", RegexOptions.IgnoreCase);
        return match.Success ? match.Groups[1].Value.Trim() : string.Empty;
    }

    private static string TruncateDesc(string text, int maxLen)
    {
        if (string.IsNullOrEmpty(text)) return "";
        text = text.Trim();
        if (text.Length <= maxLen) return text;
        var cut = text.LastIndexOf(' ', maxLen);
        return (cut > 0 ? text[..cut] : text[..maxLen]) + "...";
    }

    internal static string EscapeHtml(string text) =>
        text.Replace("&", "&amp;").Replace("<", "&lt;").Replace(">", "&gt;");

    // ────────────────────────────────────────────────────────────
    // Лёгкий русский стемминг: обрезаем окончания
    // ────────────────────────────────────────────────────────────

    private static readonly string[] StopWords =
    {
        "что", "как", "про", "для", "меня", "тебя", "себя", "когда", "почему",
        "если", "чтобы", "книг", "книга", "книги", "это", "эта", "этот",
        "хочу", "нужно", "надо", "очень", "просто", "помоги", "тему", "тема"
    };

    private static readonly string[] Endings =
    {
        "ями", "ами", "ого", "его", "ому", "ему", "ыми", "ими",
        "ая", "яя", "ое", "ее", "ую", "юю", "ой", "ый", "ий", "ей", "ый",
        "ах", "ях", "ом", "ем", "ов", "ев", "ам", "ям",
        "ть", "ся",
        "а", "я", "о", "е", "у", "ю", "ы", "и", "ь"
    };

    private static string Stem(string word)
    {
        word = word.ToLowerInvariant();
        foreach (var e in Endings)
            if (word.Length > e.Length + 2 && word.EndsWith(e))
                return word[..^e.Length];
        return word;
    }

    internal static HashSet<string> ExtractStems(string text)
    {
        var stems = new HashSet<string>();
        foreach (var raw in Regex.Split(text.ToLowerInvariant(), @"[^\p{L}]+"))
        {
            if (raw.Length < 3) continue;
            if (StopWords.Contains(raw)) continue;
            var s = Stem(raw);
            if (s.Length >= 3) stems.Add(s);
        }
        return stems;
    }

    private static int ScoreBook(Book b, HashSet<string> stems)
    {
        if (stems.Count == 0) return 0;
        var haystack = (b.Tags + " " + b.Title + " " + b.Description).ToLowerInvariant();
        var score = 0;
        foreach (var s in stems)
            if (haystack.Contains(s)) score++;
        return score;
    }

    // ────────────────────────────────────────────────────────────
    // Парсинг JSON-ответа Claude
    // ────────────────────────────────────────────────────────────

    internal static List<long> ParseJsonIds(string raw)
    {
        var ids = new List<long>();
        var match = Regex.Match(raw, @"\[([^\]]*)\]");
        if (match.Success)
        {
            try
            {
                var arr = JsonDocument.Parse("[" + match.Groups[1].Value + "]");
                foreach (var el in arr.RootElement.EnumerateArray())
                    if (el.TryGetInt64(out var v)) ids.Add(v);
                if (ids.Count > 0) return ids;
            }
            catch { /* fallback below */ }
        }

        foreach (Match m in Regex.Matches(raw, @"\d+"))
            if (long.TryParse(m.Value, out var v)) ids.Add(v);
        return ids;
    }

    // Парсит [{"id": N, "comment": "..."}]
    internal static List<(long Id, string Comment)> ParseRecommendations(string raw)
    {
        var list = new List<(long, string)>();
        var match = Regex.Match(raw ?? "", @"\[[\s\S]*\]");
        if (!match.Success) return list;

        try
        {
            using var doc = JsonDocument.Parse(match.Value);
            foreach (var el in doc.RootElement.EnumerateArray())
            {
                if (!el.TryGetProperty("id", out var idEl) || !idEl.TryGetInt64(out var id)) continue;
                var comment = el.TryGetProperty("comment", out var cEl) ? (cEl.GetString() ?? "") : "";
                list.Add((id, comment.Trim()));
            }
        }
        catch { /* невалидный JSON — вернём пустой список, вызывающий код возьмёт кандидатов */ }

        return list;
    }
}
