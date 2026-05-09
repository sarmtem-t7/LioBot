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

    // Парсит выпуски через flipbook-ссылки. Раньше брали только обложки
    // с CDN, но обложки на странице есть только у свежих выпусков (~16),
    // тогда как flipbook-ссылок — десятки и сотни (один link = один
    // выпуск). У каждой flipbook-страницы в og-тегах есть title и image,
    // которые и используем как канонический источник.
    private static readonly Regex FlipbookUrlRe = new(
        @"https://online\.fliphtml5\.com/[a-z0-9]+/[a-z0-9]+/?",
        RegexOptions.IgnoreCase);

    // Извлекает год из заголовка выпуска ("Тропинка 1998.4", "Вера и Жизнь 2026.1").
    private static readonly Regex YearInTitleRe = new(@"\b(19|20)\d{2}\b");

    // Достаёт пару (год, номер) из заголовка любого формата:
    //   «Тропинка 2023.1»          → (2023, 1)
    //   «2023 №1»                  → (2023, 1)
    //   «Вера и Жизнь 2026.1»      → (2026, 1)
    //   «1995 Вера и Жизнь № 4»    → (1995, 4)
    // Возвращает null, если оба числа извлечь не удалось.
    private static readonly Regex IssueKeyDotRe = new(@"(?<y>(?:19|20)\d{2})\s*\.\s*(?<n>\d+)");
    private static readonly Regex IssueKeyHashRe = new(@"(?<y>(?:19|20)\d{2}).{0,30}?[№#]\s*(?<n>\d+)");
    private static readonly Regex IssueKeyDashRe = new(@"(?<y>(?:19|20)\d{2})\s*[-–—]\s*(?<n>\d+)");

    private static (int Year, int Num)? ParseIssueKey(string title)
    {
        if (string.IsNullOrWhiteSpace(title)) return null;
        foreach (var rx in new[] { IssueKeyDotRe, IssueKeyHashRe, IssueKeyDashRe })
        {
            var m = rx.Match(title);
            if (m.Success
                && int.TryParse(m.Groups["y"].Value, out var y)
                && int.TryParse(m.Groups["n"].Value, out var n)
                && n is > 0 and < 100)
                return (y, n);
        }
        return null;
    }

    // Каноническое имя журнала для построения заголовка выпуска.
    private static string CanonicalMagazinePrefix(string slug, string fallback)
    {
        return slug switch
        {
            "vera"     => "Вера и Жизнь",
            "tropinka" => "Тропинка",
            _          => string.IsNullOrWhiteSpace(fallback) ? slug : fallback
        };
    }

    // Является ли заголовок «красивым» — то есть содержит имя журнала.
    // Используется при дедупе: записи с «уродскими» заголовками («2023 №1»)
    // проигрывают записям с «красивыми» (`Тропинка 2023.1`), даже если у
    // последних нет обложки — обложку можно подтянуть позже.
    private static bool IsCanonicalTitle(string title, string slug, string magTitle)
    {
        var prefix = CanonicalMagazinePrefix(slug, magTitle);
        return title.Contains(prefix, StringComparison.OrdinalIgnoreCase);
    }

    // Удаляет дубликаты выпусков, у которых совпадает (год, номер) по title.
    // Ранжирование: сначала записи с «красивым» заголовком (с именем журнала),
    // потом по наличию обложки, потом по длине заголовка, потом по id.
    public int DeduplicateMagazineIssues()
    {
        var deleted = 0;
        foreach (var (magId, slug, magTitle, _) in _db.GetAllMagazines())
        {
            var issues = _db.GetMagazineIssues(magId);
            var keyed = issues
                .Select(i => new { Issue = i, Key = ParseIssueKey(i.Title) })
                .Where(x => x.Key.HasValue)
                .ToList();

            foreach (var group in keyed.GroupBy(x => x.Key!.Value))
            {
                if (group.Count() < 2) continue;
                var ranked = group
                    .OrderByDescending(x => IsCanonicalTitle(x.Issue.Title, slug, magTitle) ? 1 : 0)
                    .ThenByDescending(x => string.IsNullOrEmpty(x.Issue.CoverUrl) ? 0 : 1)
                    .ThenByDescending(x => x.Issue.Title.Length)
                    .ThenByDescending(x => x.Issue.Id)
                    .ToList();
                foreach (var dup in ranked.Skip(1))
                {
                    _db.DeleteMagazineIssue(dup.Issue.Id);
                    deleted++;
                    _logger.LogInformation("[Dedup] Удалён дубль mag={M} ({Y}/{N}) id={Id} «{T}»",
                        magId, group.Key.Year, group.Key.Num, dup.Issue.Id, dup.Issue.Title);
                }
            }
        }
        return deleted;
    }

    // Приводит заголовки оставшихся выпусков к каноническому формату
    // «<Журнал> YYYY.N» — на случай, если после дедупа в живых остался
    // «уродецкий» вариант. Идемпотентно: уже канонические записи не трогает.
    public int NormalizeMagazineIssueTitles()
    {
        var renamed = 0;
        foreach (var (magId, slug, magTitle, _) in _db.GetAllMagazines())
        {
            var prefix = CanonicalMagazinePrefix(slug, magTitle);
            var issues = _db.GetMagazineIssues(magId);
            foreach (var issue in issues)
            {
                var key = ParseIssueKey(issue.Title);
                if (!key.HasValue) continue;
                var canonical = $"{prefix} {key.Value.Year}.{key.Value.Num}";
                if (issue.Title == canonical) continue;
                _db.UpdateMagazineIssueTitle(issue.Id, canonical);
                renamed++;
                _logger.LogInformation("[Rename] mag={M} «{Old}» -> «{New}»", magId, issue.Title, canonical);
            }
        }
        return renamed;
    }

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
            var pageUrl = $"https://www.lio-int.com/zurnaly/{slug}";
            var html = await FetchAsync(pageUrl, ct);
            if (string.IsNullOrEmpty(html)) continue;

            // Собираем уникальные flipbook-ссылки. На одной странице журнала
            // их обычно столько же, сколько и выпусков.
            var flipUrls = FlipbookUrlRe.Matches(html)
                .Select(m => m.Value.TrimEnd('/') + "/")  // нормализуем со слешем
                .Distinct(StringComparer.OrdinalIgnoreCase)
                .ToList();

            _logger.LogInformation("[Import {Slug}] flipbook ссылок: {Count}", slug, flipUrls.Count);

            foreach (var flipUrl in flipUrls)
            {
                ct.ThrowIfCancellationRequested();

                var flipHtml = await FetchAsync(flipUrl, ct);
                if (string.IsNullOrEmpty(flipHtml)) continue;

                var meta = ExtractMeta(flipHtml);
                var title = meta.GetValueOrDefault("og_title")
                          ?? meta.GetValueOrDefault("title")
                          ?? "";
                title = title.Trim();
                if (string.IsNullOrEmpty(title)) continue;

                var coverUrl = meta.GetValueOrDefault("og_image") ?? "";

                // Год для сортировки. Если в заголовке нет — оставляем null.
                string? releasedAt = null;
                var ym = YearInTitleRe.Match(title);
                if (ym.Success) releasedAt = $"{ym.Value}-01-01";

                if (_db.MagazineIssueExists(magId.Value, title))
                {
                    // Идемпотентный апдейт: освежаем url/обложку, если изменились.
                    _db.UpdateMagazineIssueUrl(magId.Value, title, flipUrl);
                    continue;
                }

                _db.AddMagazineIssue(magId.Value, title, flipUrl, coverUrl, releasedAt);
                added++;
                _logger.LogInformation("[Import] Выпуск: {Slug} «{Title}» -> {Url}", slug, title, flipUrl);

                // Не давим на flipbook
                await Task.Delay(120, ct);
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
                AudioUrl = isAudio ? url : "",
                CoverUrl = meta.GetValueOrDefault("og_image") ?? ""
            });
            added++;
        }
        return added;
    }

    // ─── Backfill обложек для уже залитых записей ─────────────────────
    // Парсит og:image со страницы каждого Book без CoverUrl и проставляет.
    // Без вызовов AI — только HTTP, поэтому быстрее и не упирается в Groq лимит.
    public async Task<(int Updated, int Scanned)> BackfillCoversAsync(string? typeFilter = null, CancellationToken ct = default)
    {
        if (!await _gate.WaitAsync(0, ct))
            throw new InvalidOperationException("Импорт уже выполняется.");
        try
        {
            var rows = _db.GetBooksWithoutCover(typeFilter);
            _logger.LogInformation("[Import covers] {Count} материалов без обложки", rows.Count);
            var updated = 0;
            foreach (var (id, url) in rows)
            {
                ct.ThrowIfCancellationRequested();
                var html = await FetchAsync(url, ct);
                if (string.IsNullOrEmpty(html)) continue;
                var meta = ExtractMeta(html);
                var cover = meta.GetValueOrDefault("og_image");
                if (string.IsNullOrWhiteSpace(cover)) continue;
                _db.SetBookCover(id, cover);
                updated++;
            }
            return (updated, rows.Count);
        }
        finally { _gate.Release(); }
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
