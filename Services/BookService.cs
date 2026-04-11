using System.Net.Http;
using System.Text.RegularExpressions;
using LioBot.Data;
using LioBot.Models;

namespace LioBot.Services;

public class BookService
{
    private readonly DatabaseContext _db;
    private readonly ClaudeService _claude;
    private readonly HttpClient _http;

    public BookService(DatabaseContext db, ClaudeService claude, IHttpClientFactory httpClientFactory)
    {
        _db = db;
        _claude = claude;
        _http = httpClientFactory.CreateClient();
    }

    /// <summary>
    /// Загружает страницу по URL, извлекает данные о книге через AI и сохраняет в БД.
    /// </summary>
    public async Task<string> AddBookFromUrlAsync(string url)
    {
        // Загружаем страницу
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

        // Убираем HTML-теги, оставляем текст
        var text = Regex.Replace(html, "<[^>]+>", " ");
        text = Regex.Replace(text, @"\s{2,}", " ").Trim();
        // Берём первые 3000 символов — этого достаточно для заголовка и описания
        if (text.Length > 3000) text = text[..3000];

        // Просим AI извлечь данные
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

        var aiResponse = await _claude.AskAsync(systemPrompt, $"Текст страницы:\n{text}", maxTokens: 400);

        // Парсим ответ AI
        var title       = ExtractField(aiResponse, "НАЗВАНИЕ");
        var author      = ExtractField(aiResponse, "АВТОР");
        var description = ExtractField(aiResponse, "ОПИСАНИЕ");
        var tags        = ExtractField(aiResponse, "ТЕГИ");

        if (string.IsNullOrWhiteSpace(title) || title == "Не указано")
            return "Не удалось определить название книги. Попробуй другую ссылку.";

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

    private static string ExtractField(string text, string field)
    {
        var match = Regex.Match(text, $@"{field}:\s*(.+?)(?:\n|$)", RegexOptions.IgnoreCase);
        return match.Success ? match.Groups[1].Value.Trim() : string.Empty;
    }

    /// <summary>
    /// Подбирает книги под запрос пользователя.
    /// Шаг 1: поиск по ключевым словам прямо в БД (без AI, без токенов).
    /// Шаг 2: из найденных AI выбирает лучшие 2-3.
    /// </summary>
    public async Task<string> RecommendBooksAsync(
        string userRequest,
        IEnumerable<(string Role, string Content)>? history = null)
    {
        var allBooks = _db.GetAllBooks();
        if (allBooks.Count == 0)
            return "К сожалению, в библиотеке пока нет книг. Мы уже работаем над этим! 📚";

        // Контекст из истории — добавляем к запросу чтобы учесть интересы пользователя
        var historyContext = "";
        if (history != null)
        {
            var userMessages = history
                .Where(h => h.Role == "user")
                .TakeLast(5)
                .Select(h => h.Content);
            var joined = string.Join(" | ", userMessages);
            if (!string.IsNullOrEmpty(joined))
                historyContext = $"\nКонтекст из прошлых сообщений: {joined}";
        }

        // Шаг 1: ищем кандидатов по словам из запроса + истории
        var searchText = (userRequest + " " + historyContext).ToLowerInvariant();
        var keywords = searchText
            .Split(' ', ',', '.', '!', '?', '|')
            .Where(w => w.Length > 3)
            .ToList();

        var candidates = allBooks
            .Select(b => new
            {
                Book = b,
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

        // Шаг 2: AI выбирает лучшие 2-3 с учётом контекста
        var catalog = string.Join("\n", candidates.Select(b =>
            $"ID:{b.Id} | «{b.Title}» — {b.Author} | {b.Tags}"));

        var systemPrompt = """
            Ты — помощник книжного клуба. Выбери 2-3 книги из списка, которые лучше всего подходят к запросу.
            Учитывай контекст из прошлых сообщений пользователя чтобы понять его ситуацию и интересы.
            Ответь ТОЛЬКО числами ID через запятую. Например: 3,7,12
            Только ID из списка, ничего лишнего.
            """;

        var idsRaw = await _claude.AskAsync(systemPrompt,
            $"Список:\n{catalog}\n\nЗапрос: {userRequest}{historyContext}", maxTokens: 20);

        var selectedBooks = ParseIds(idsRaw)
            .Select(id => candidates.FirstOrDefault(b => b.Id == id))
            .Where(b => b != null)
            .Cast<Book>()
            .ToList();

        if (selectedBooks.Count == 0)
            selectedBooks = candidates.Take(2).ToList();

        // Шаг 3: формируем ответ из данных базы — без дополнительных запросов к AI
        var sb = new System.Text.StringBuilder();
        sb.AppendLine("📚 Вот что нашёл:\n");

        foreach (var book in selectedBooks)
        {
            var isAudio = book.Type == "audio";
            var icon = isAudio ? "🎧" : "📖";
            var shortDesc = TruncateDesc(book.Description, 100);
            var linkLabel = isAudio ? "Слушать" : "Читать";
            sb.Append($"{icon} <b>«{EscapeHtml(book.Title)}»</b> — {EscapeHtml(book.Author)}");
            if (!string.IsNullOrEmpty(shortDesc))
                sb.Append($"\n{EscapeHtml(shortDesc)}");
            if (!string.IsNullOrEmpty(book.Url))
                sb.Append($" <a href=\"{book.Url}\">→ {linkLabel}</a>");
            sb.AppendLine("\n");
        }
        return sb.ToString().Trim();
    }

    private static string TruncateDesc(string text, int maxLen)
    {
        if (string.IsNullOrEmpty(text)) return "";
        text = text.Trim();
        if (text.Length <= maxLen) return text;
        var cut = text.LastIndexOf(' ', maxLen);
        return (cut > 0 ? text[..cut] : text[..maxLen]) + "...";
    }

    private static string EscapeHtml(string text) =>
        text.Replace("&", "&amp;").Replace("<", "&lt;").Replace(">", "&gt;");

    private static List<long> ParseIds(string raw)
    {
        var ids = new List<long>();
        foreach (var part in raw.Split(',', ' ', '\n', ';'))
        {
            if (long.TryParse(part.Trim(), out var id))
                ids.Add(id);
        }
        return ids;
    }

    public List<Book> GetAllBooks() => _db.GetAllBooks();
}
