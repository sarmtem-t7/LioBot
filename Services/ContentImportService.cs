using System.Net;
using System.Text.RegularExpressions;
using LioBot.Data;
using LioBot.Models;
using Microsoft.Extensions.Logging;

namespace LioBot.Services;

public record ImportSummary(int Books, int Audio, int Articles, int Radio, int Magazines, int Issues = 0)
{
    public int Total => Books + Audio + Articles + Radio + Magazines + Issues;
}

// Импорт контента с lio-int.com и lio-blog.com прямо из бота — повторяет
// логику import_content.py (Python), чтобы можно было лить в /data/liobot.db
// на проде без выхода в shell.
public class ContentImportService
{
    private readonly DatabaseContext _db;
    private readonly ClaudeService _ai;
    private readonly HttpClient _http;
    private readonly ILogger<ContentImportService> _logger;

    private const string SitemapMain = "https://www.lio-int.com/sitemap.xml";
    private const string SitemapBlog = "https://lio-blog.com/sitemap.xml";

    // Защита от параллельного запуска двух импортов одновременно.
    private static readonly SemaphoreSlim _gate = new(1, 1);

    public ContentImportService(
        DatabaseContext db,
        ClaudeService ai,
        IHttpClientFactory httpClientFactory,
        ILogger<ContentImportService> logger)
    {
        _db = db;
        _ai = ai;
        _http = httpClientFactory.CreateClient();
        _http.DefaultRequestHeaders.UserAgent.ParseAdd("Mozilla/5.0 (compatible; LioBot-Importer/1.0)");
        _logger = logger;
    }

    public bool IsRunning => _gate.CurrentCount == 0;

    public async Task<ImportSummary> ImportAllAsync(CancellationToken ct = default)
    {
        if (!await _gate.WaitAsync(0, ct))
            throw new InvalidOperationException("Импорт уже выполняется.");
        try
        {
            var audio    = await ImportAudiobooksAsync(ct);
            var articles = await ImportArticlesAsync(ct);
            var radio    = await ImportRadioAsync(ct);
            var mags     = await ImportMagazinesAsync(ct);
            var issues   = await ImportMagazineIssuesAsync(ct);
            return new ImportSummary(0, audio, articles, radio, mags, issues);
        }
        finally { _gate.Release(); }
    }

    public async Task<int> ImportAudiobooksAsync(CancellationToken ct = default)
    {
        var urls = (await SitemapUrlsAsync(SitemapMain, "/audioknigi/", ct))
            .Where(u => new Uri(u).AbsolutePath.Count(c => c == '/') >= 2 && !u.Contains("/audioknigi-"))
            .ToList();
        return await ImportPagesAsync(urls, type: "audio", isAudio: true, generateTags: true, ct);
    }

    public async Task<int> ImportArticlesAsync(CancellationToken ct = default)
    {
        var urls = (await SitemapUrlsAsync(SitemapBlog, prefix: null, ct))
            .Where(u => Regex.IsMatch(u, @"/\d{4}/\d{2}/\d{2}/"))
            .ToList();
        return await ImportPagesAsync(urls, type: "article", isAudio: false, generateTags: true, ct);
    }

    public async Task<int> ImportRadioAsync(CancellationToken ct = default)
    {
        var urls = (await SitemapUrlsAsync(SitemapMain, "/radio/", ct))
            .Where(u => new Uri(u).AbsolutePath.Count(c => c == '/') >= 2)
            .ToList();
        return await ImportPagesAsync(urls, type: "radio", isAudio: true, generateTags: false, ct,
            defaultTags: "радио");
    }

    public async Task<int> ImportMagazinesAsync(CancellationToken ct = default)
    {
        var keep = new Regex(@"/zurnaly/(vera|tropinka|menora)(-audio)?/?$");
        var urls = (await SitemapUrlsAsync(SitemapMain, "/zurnaly/", ct))
            .Where(u => keep.IsMatch(u))
            .ToList();

        var added = 0;
        foreach (var url in urls)
        {
            var slug = url.TrimEnd('/').Split('/').Last();
            if (_db.MagazineExistsBySlug(slug)) continue;
            var html = await FetchAsync(url, ct);
            var meta = ExtractMeta(html ?? "");
            var title = meta.GetValueOrDefault("og_title")
                     ?? meta.GetValueOrDefault("title")
                     ?? slug;
            _db.AddMagazine(slug, title, url);
            added++;
        }
        return added;
    }

    // Парсит обложки выпусков из CDN-ссылок на страницах журналов.
    // Формат файлов: "2025 Вера и Жизнь - 3 a.jpg", "2024 Тропинка - 1a.jpg"
    private static readonly Regex IssueFileRe = new(
        @"(\d{4})\+([^/]+?)\+?-\+?(\d+)\s*a\.(jpg|png)",
        RegexOptions.IgnoreCase);

    public async Task<int> ImportMagazineIssuesAsync(CancellationToken ct = default)
    {
        var slugs = new[] { "vera", "tropinka" };
        var added = 0;
        foreach (var slug in slugs)
        {
            var magId = _db.GetMagazineIdBySlug(slug);
            if (magId is null)
            {
                _logger.LogDebug("[Import] Журнал {Slug} не найден в Magazines — пропуск", slug);
                continue;
            }
            var url = $"https://www.lio-int.com/zurnaly/{slug}";
            var html = await FetchAsync(url, ct);
            if (string.IsNullOrEmpty(html)) continue;

            var seen = new HashSet<string>();
            foreach (Match m in Regex.Matches(html,
                @"https://irp\.cdn-website\.com/[^""]+\.(jpg|png)", RegexOptions.IgnoreCase))
            {
                var imgUrl = m.Value;
                var decoded = System.Net.WebUtility.UrlDecode(imgUrl.Replace('+', ' '));
                var fileMatch = IssueFileRe.Match(imgUrl);
                if (!fileMatch.Success) continue;

                var year = fileMatch.Groups[1].Value;
                var number = fileMatch.Groups[3].Value;
                var title = $"{year} №{number}";
                if (!seen.Add(title)) continue;
                if (_db.MagazineIssueExists(magId.Value, title)) continue;

                _db.AddMagazineIssue(magId.Value, title, url, imgUrl, $"{year}-01-01");
                added++;
                _logger.LogInformation("[Import] Выпуск: {Slug} {Title}", slug, title);
            }
        }
        return added;
    }

    // ─── Общая часть: импорт списка URL в Books ───────────────────────
    private async Task<int> ImportPagesAsync(
        List<string> urls, string type, bool isAudio, bool generateTags,
        CancellationToken ct, string defaultTags = "")
    {
        _logger.LogInformation("[Import {Type}] {Count} URL", type, urls.Count);
        var added = 0;
        foreach (var url in urls)
        {
            ct.ThrowIfCancellationRequested();
            if (_db.BookExistsByUrl(url)) continue;

            var html = await FetchAsync(url, ct);
            if (string.IsNullOrEmpty(html)) continue;

            var meta = ExtractMeta(html);
            var title       = meta.GetValueOrDefault("og_title")
                           ?? meta.GetValueOrDefault("title")
                           ?? url.Split('/').Last();
            // Для блога заголовок обычно "Название — Вера & Жизнь", чистим
            if (type == "article")
                title = Regex.Replace(title, @"\s*[—–-]\s*Вера\s*&\s*Жизнь.*$", "");

            var description = meta.GetValueOrDefault("og_description")
                           ?? meta.GetValueOrDefault("description")
                           ?? "";
            var author = type == "audio"
                ? (meta.GetValueOrDefault("og_description") ?? "")
                : "";

            var tags = defaultTags;
            if (generateTags)
            {
                try { tags = await GenerateTagsAsync(title, description, ct); }
                catch (Exception ex)
                {
                    _logger.LogDebug(ex, "[Import] AI-теги недоступны для {Url}", url);
                    tags = ""; // дозаполнится позже отдельным проходом
                }
            }

            _db.AddBook(new Book
            {
                Title = title,
                Author = author,
                Description = description,
                Tags = tags,
                Url = url,
                Type = type,
                AudioUrl = isAudio ? url : ""
            });
            added++;
        }
        return added;
    }

    // ─── HTTP / sitemap / meta ────────────────────────────────────────
    private async Task<List<string>> SitemapUrlsAsync(string sitemap, string? prefix, CancellationToken ct)
    {
        var xml = await FetchAsync(sitemap, ct);
        if (string.IsNullOrEmpty(xml)) return new();
        var urls = Regex.Matches(xml, @"<loc>([^<]+)</loc>")
            .Select(m => m.Groups[1].Value)
            .ToList();
        if (!string.IsNullOrEmpty(prefix))
            urls = urls.Where(u => u.Contains(prefix)).ToList();
        return urls;
    }

    private async Task<string?> FetchAsync(string url, CancellationToken ct)
    {
        try
        {
            using var cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
            cts.CancelAfter(TimeSpan.FromSeconds(30));
            return await _http.GetStringAsync(url, cts.Token);
        }
        catch (Exception ex)
        {
            _logger.LogDebug("[Import] fetch fail {Url}: {Msg}", url, ex.Message);
            return null;
        }
    }

    private static readonly Dictionary<string, Regex> MetaRe = new()
    {
        ["og_title"]       = new(@"<meta[^>]+property=[""']og:title[""'][^>]+content=[""']([^""']+)", RegexOptions.IgnoreCase),
        ["og_description"] = new(@"<meta[^>]+property=[""']og:description[""'][^>]+content=[""']([^""']+)", RegexOptions.IgnoreCase),
        ["og_image"]       = new(@"<meta[^>]+property=[""']og:image[""'][^>]+content=[""']([^""']+)", RegexOptions.IgnoreCase),
        ["description"]    = new(@"<meta[^>]+name=[""']description[""'][^>]+content=[""']([^""']+)", RegexOptions.IgnoreCase),
        ["title"]          = new(@"<title>([^<]+)</title>", RegexOptions.IgnoreCase),
    };

    private static Dictionary<string, string> ExtractMeta(string html)
    {
        var result = new Dictionary<string, string>();
        foreach (var (key, rx) in MetaRe)
        {
            var m = rx.Match(html);
            if (m.Success) result[key] = WebUtility.HtmlDecode(m.Groups[1].Value).Trim();
        }
        return result;
    }

    private async Task<string> GenerateTagsAsync(string title, string description, CancellationToken ct)
    {
        var prompt = "Название: " + title + "\nОписание: " + description +
            "\n\nВерни 3–6 тегов через запятую на русском. Только теги, без пояснений. " +
            "Используй темы: молитва, семья, отношения, вера, Библия, воспитание, " +
            "апологетика, миссия, свидетельство, проповедь, поэзия, история, дети, подростки.";
        var raw = await _ai.AskAsync(systemPrompt: "Ты помощник, выдающий теги через запятую.", userMessage: prompt, maxTokens: 80);
        return Regex.Replace(raw, @"[^а-яА-Яa-zA-Z0-9,\s\-]", "").Trim();
    }
}
