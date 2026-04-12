using System.Text;
using System.Text.RegularExpressions;
using LioBot.Data;
using LioBot.Models;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;

namespace LioBot.Services;

public class BookService
{
    private readonly DatabaseContext _db;
    private readonly GroqService _groq;
    private readonly HttpClient _http;
    private readonly ILogger<BookService> _logger;
    private readonly string _allowedBookDomain;

    public BookService(
        DatabaseContext db,
        GroqService groq,
        IHttpClientFactory httpClientFactory,
        IConfiguration configuration,
        ILogger<BookService> logger)
    {
        _db = db;
        _groq = groq;
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
            aiResponse = await _groq.AskAsync(systemPrompt, $"Текст страницы:\n{text}", maxTokens: 400);
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

        // Собираем ID книг, которые нужно исключить
        var excludeIds = new HashSet<long>();
        if (telegramId > 0)
        {
            foreach (var id in _db.GetSeenBookIds(telegramId))    excludeIds.Add(id);
            foreach (var id in _db.GetIgnoredBookIds(telegramId)) excludeIds.Add(id);
        }

        var historyContext = BuildHistoryContext(history);

        // Шаг 1: поиск кандидатов по ключевым словам
        var searchText = (userRequest + " " + historyContext).ToLowerInvariant();
        var keywords = searchText
            .Split(' ', ',', '.', '!', '?', '|')
            .Where(w => w.Length > 3)
            .ToList();

        var candidates = allBooks
            .Select(b => new
            {
                Book  = b,
                Score = keywords.Count(kw =>
                    b.Tags.ToLower().Contains(kw) ||
                    b.Title.ToLower().Contains(kw) ||
                    b.Description.ToLower().Contains(kw))
            })
            .Where(x => x.Score > 0)
            .OrderByDescending(x => x.Score)
            .Take(20)
            .Select(x => x.Book)
            .ToList();

        if (candidates.Count == 0)
            candidates = allBooks.OrderBy(_ => Guid.NewGuid()).Take(20).ToList();

        // Исключаем виденные / скрытые (если остаётся достаточно)
        var fresh = candidates.Where(b => !excludeIds.Contains(b.Id)).ToList();
        if (fresh.Count >= 2) candidates = fresh;

        // Шаг 2: AI выбирает лучшие 2-3
        List<Book> selectedBooks;
        try
        {
            var catalog = string.Join("\n", candidates.Select(b =>
                $"ID:{b.Id} | «{b.Title}» — {b.Author} | {b.Tags}"));

            var idsRaw = await _groq.AskAsync(
                "Ты помощник книжного клуба. Выбери 2-3 книги из списка, подходящие к запросу. Учитывай контекст. Ответь ТОЛЬКО числами ID через запятую. Ничего лишнего.",
                $"Список:\n{catalog}\n\nЗапрос: {userRequest}{historyContext}",
                maxTokens: 20);

            selectedBooks = ParseIds(idsRaw)
                .Select(id => candidates.FirstOrDefault(b => b.Id == id))
                .Where(b => b != null)
                .Cast<Book>()
                .ToList();
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "[BookService] AI недоступен, fallback на топ кандидатов");
            selectedBooks = [];
        }

        if (selectedBooks.Count == 0)
            selectedBooks = candidates.Take(2).ToList();

        if (telegramId > 0)
            _db.MarkBooksAsSeen(telegramId, selectedBooks.Select(b => b.Id));

        return new RecommendationResult(FormatBookList("📚 Вот что нашёл:", selectedBooks), selectedBooks);
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

        var sourceTags = source.Tags.ToLower()
            .Split(',', ' ').Where(t => t.Length > 2).ToHashSet();

        var candidates = allBooks
            .Select(b => new { Book = b, Score = sourceTags.Count(t => b.Tags.ToLower().Contains(t)) })
            .Where(x => x.Score > 0)
            .OrderByDescending(x => x.Score)
            .Take(20)
            .Select(x => x.Book)
            .ToList();

        if (candidates.Count == 0)
            candidates = allBooks.OrderBy(_ => Guid.NewGuid()).Take(5).ToList();

        // Исключаем уже виденные (если остаётся достаточно)
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

            var idsRaw = await _groq.AskAsync(
                "Ты помощник книжного клуба. Ответь ТОЛЬКО числами ID через запятую. Ничего лишнего.",
                $"Список:\n{catalog}\n\nВыбери 2-3 книги, похожие на «{source.Title}» (теги: {source.Tags}). Только ID.",
                maxTokens: 20);

            selected = ParseIds(idsRaw)
                .Select(id => candidates.FirstOrDefault(b => b.Id == id))
                .Where(b => b != null)
                .Cast<Book>()
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

        var keywords = query.ToLowerInvariant()
            .Split(' ', ',', '.').Where(w => w.Length > 2).ToList();

        return books
            .Select(b => new
            {
                Book  = b,
                Score = keywords.Count(kw =>
                    b.Title.ToLower().Contains(kw) ||
                    b.Author.ToLower().Contains(kw) ||
                    b.Tags.ToLower().Contains(kw) ||
                    b.Description.ToLower().Contains(kw))
            })
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

    private static string BuildHistoryContext(IEnumerable<(string Role, string Content)>? history)
    {
        if (history == null) return "";
        var msgs = history.Where(h => h.Role == "user").TakeLast(5).Select(h => h.Content);
        var joined = string.Join(" | ", msgs);
        return string.IsNullOrEmpty(joined) ? "" : $"\nКонтекст: {joined}";
    }

    private static string FormatBookList(string header, IEnumerable<Book> books)
    {
        var sb = new StringBuilder();
        sb.AppendLine(header);
        sb.AppendLine();
        foreach (var book in books)
        {
            var isAudio = book.Type == "audio";
            sb.Append(isAudio ? "🎧" : "📖");
            sb.Append($" <b>«{EscapeHtml(book.Title)}»</b> — {EscapeHtml(book.Author)}");
            if (!string.IsNullOrEmpty(book.Description))
                sb.Append($"\n{EscapeHtml(TruncateDesc(book.Description, 100))}");
            if (!string.IsNullOrEmpty(book.Url))
                sb.Append($" <a href=\"{book.Url}\">→ {(isAudio ? "Слушать" : "Читать")}</a>");
            sb.AppendLine("\n");
        }
        return sb.ToString().Trim();
    }

    internal static string FormatBookCard(Book book)
    {
        var isAudio = book.Type == "audio";
        var icon = isAudio ? "🎧" : "📖";
        var sb = new StringBuilder();
        sb.AppendLine($"{icon} <b>«{EscapeHtml(book.Title)}»</b>");
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

    private static List<long> ParseIds(string raw)
    {
        var ids = new List<long>();
        foreach (var part in raw.Split(',', ' ', '\n', ';'))
            if (long.TryParse(part.Trim(), out var id))
                ids.Add(id);
        return ids;
    }
}
