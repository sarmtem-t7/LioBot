using Microsoft.Data.Sqlite;
using Microsoft.Extensions.Configuration;
using LioBot.Models;

namespace LioBot.Data;

public partial class DatabaseContext
{
    private readonly string _connectionString;

    public DatabaseContext(IConfiguration configuration)
    {
        var dataDir = configuration["DATA_PATH"]
            ?? Environment.GetEnvironmentVariable("DATA_PATH")
            ?? AppContext.BaseDirectory;
        Directory.CreateDirectory(dataDir);
        var dbPath = Path.Combine(dataDir, "liobot.db");
        _connectionString = $"Data Source={dbPath}";
    }

    public SqliteConnection CreateConnection() => new SqliteConnection(_connectionString);

    public void Initialize()
    {
        using var conn = CreateConnection();
        conn.Open();

        var cmd = conn.CreateCommand();

        // Enable WAL mode for better concurrent read performance
        cmd.CommandText = "PRAGMA journal_mode=WAL;";
        cmd.ExecuteNonQuery();

        // Core tables
        cmd.CommandText = """
            CREATE TABLE IF NOT EXISTS Users (
                Id           INTEGER PRIMARY KEY AUTOINCREMENT,
                TelegramId   INTEGER NOT NULL UNIQUE,
                Username     TEXT,
                FirstName    TEXT NOT NULL,
                RegisteredAt TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS Books (
                Id          INTEGER PRIMARY KEY AUTOINCREMENT,
                Title       TEXT NOT NULL,
                Author      TEXT NOT NULL,
                Description TEXT NOT NULL,
                Tags        TEXT NOT NULL,
                Url         TEXT NOT NULL DEFAULT '',
                Type        TEXT NOT NULL DEFAULT 'book'
            );
            """;
        cmd.ExecuteNonQuery();

        // Migrations — catch if column already exists
        foreach (var migration in new[]
        {
            "ALTER TABLE Books ADD COLUMN Type TEXT NOT NULL DEFAULT 'book'",
            "ALTER TABLE Users ADD COLUMN NotifyMode TEXT NOT NULL DEFAULT 'daily'",
            "ALTER TABLE Users ADD COLUMN OnboardingDone INTEGER NOT NULL DEFAULT 0",
            "ALTER TABLE Books ADD COLUMN AiAnnotation TEXT NOT NULL DEFAULT ''",
            // Унифицированный контент: audio, журналы, дата публикации
            "ALTER TABLE Books ADD COLUMN AudioUrl TEXT NOT NULL DEFAULT ''",
            "ALTER TABLE Books ADD COLUMN IssueId INTEGER",
            "ALTER TABLE Books ADD COLUMN ReleasedAt TEXT",
            "ALTER TABLE Users ADD COLUMN LastBotMessageId INTEGER",
            "ALTER TABLE Books ADD COLUMN CoverUrl TEXT NOT NULL DEFAULT ''"
        })
        {
            cmd.CommandText = migration;
            try { cmd.ExecuteNonQuery(); } catch { /* already exists */ }
        }

        // Authors + связь M:N с материалами
        cmd.CommandText = """
            CREATE TABLE IF NOT EXISTS Authors (
                Id   INTEGER PRIMARY KEY AUTOINCREMENT,
                Name TEXT NOT NULL UNIQUE
            );
            CREATE TABLE IF NOT EXISTS ContentAuthors (
                ContentId INTEGER NOT NULL,
                AuthorId  INTEGER NOT NULL,
                PRIMARY KEY (ContentId, AuthorId)
            );
            CREATE INDEX IF NOT EXISTS idx_content_authors_author ON ContentAuthors(AuthorId);
            """;
        cmd.ExecuteNonQuery();

        // Журналы и их выпуски
        cmd.CommandText = """
            CREATE TABLE IF NOT EXISTS Magazines (
                Id    INTEGER PRIMARY KEY AUTOINCREMENT,
                Slug  TEXT NOT NULL UNIQUE,
                Title TEXT NOT NULL,
                Url   TEXT NOT NULL DEFAULT ''
            );
            CREATE TABLE IF NOT EXISTS MagazineIssues (
                Id         INTEGER PRIMARY KEY AUTOINCREMENT,
                MagazineId INTEGER NOT NULL,
                Title      TEXT NOT NULL,
                Url        TEXT NOT NULL DEFAULT '',
                AudioUrl   TEXT NOT NULL DEFAULT '',
                CoverUrl   TEXT NOT NULL DEFAULT '',
                ReleasedAt TEXT,
                FOREIGN KEY (MagazineId) REFERENCES Magazines(Id)
            );
            CREATE INDEX IF NOT EXISTS idx_issues_magazine ON MagazineIssues(MagazineId);
            """;
        cmd.ExecuteNonQuery();

        // Избранное (отдельно от статусов «читаю/прочитано/отложено»)
        cmd.CommandText = """
            CREATE TABLE IF NOT EXISTS Favorites (
                TelegramId INTEGER NOT NULL,
                ContentId  INTEGER NOT NULL,
                AddedAt    TEXT NOT NULL,
                PRIMARY KEY (TelegramId, ContentId)
            );
            """;
        cmd.ExecuteNonQuery();

        // History
        cmd.CommandText = """
            CREATE TABLE IF NOT EXISTS Messages (
                Id         INTEGER PRIMARY KEY AUTOINCREMENT,
                TelegramId INTEGER NOT NULL,
                Role       TEXT NOT NULL,
                Content    TEXT NOT NULL,
                CreatedAt  TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_messages_telegram ON Messages(TelegramId, Id);
            """;
        cmd.ExecuteNonQuery();

        // Seen recommendations (14-day window)
        cmd.CommandText = """
            CREATE TABLE IF NOT EXISTS SeenBooks (
                TelegramId INTEGER NOT NULL,
                BookId     INTEGER NOT NULL,
                SeenAt     TEXT NOT NULL,
                PRIMARY KEY (TelegramId, BookId)
            );
            """;
        cmd.ExecuteNonQuery();

        // Read books (permanent)
        cmd.CommandText = """
            CREATE TABLE IF NOT EXISTS ReadBooks (
                TelegramId INTEGER NOT NULL,
                BookId     INTEGER NOT NULL,
                MarkedAt   TEXT NOT NULL,
                PRIMARY KEY (TelegramId, BookId)
            );
            """;
        cmd.ExecuteNonQuery();

        // Ignored books (permanent)
        cmd.CommandText = """
            CREATE TABLE IF NOT EXISTS IgnoredBooks (
                TelegramId INTEGER NOT NULL,
                BookId     INTEGER NOT NULL,
                MarkedAt   TEXT NOT NULL,
                PRIMARY KEY (TelegramId, BookId)
            );
            """;
        cmd.ExecuteNonQuery();

        // User onboarding preferences
        cmd.CommandText = """
            CREATE TABLE IF NOT EXISTS UserPreferences (
                TelegramId INTEGER PRIMARY KEY,
                FaithStage TEXT NOT NULL DEFAULT '',
                Interests  TEXT NOT NULL DEFAULT '',
                UpdatedAt  TEXT NOT NULL
            );
            """;
        cmd.ExecuteNonQuery();

        // Book ratings (1..5)
        cmd.CommandText = """
            CREATE TABLE IF NOT EXISTS BookRatings (
                TelegramId INTEGER NOT NULL,
                BookId     INTEGER NOT NULL,
                Rating     INTEGER NOT NULL,
                CreatedAt  TEXT NOT NULL,
                PRIMARY KEY (TelegramId, BookId)
            );
            """;
        cmd.ExecuteNonQuery();

        // Currently reading
        cmd.CommandText = """
            CREATE TABLE IF NOT EXISTS ReadingNow (
                TelegramId      INTEGER NOT NULL,
                BookId          INTEGER NOT NULL,
                StartedAt       TEXT NOT NULL,
                LastRemindedAt  TEXT,
                PRIMARY KEY (TelegramId, BookId)
            );
            """;
        cmd.ExecuteNonQuery();

        // FSM state for text input after button press
        cmd.CommandText = """
            CREATE TABLE IF NOT EXISTS UserState (
                TelegramId     INTEGER PRIMARY KEY,
                PendingAction  TEXT NOT NULL,
                PendingContext TEXT NOT NULL DEFAULT '',
                UpdatedAt      TEXT NOT NULL
            );
            """;
        cmd.ExecuteNonQuery();

        // Канальные авто-меню: для каждого зарегистрированного канала
        // храним id последнего сообщения-меню, чтобы знать, что удалять
        // при ре-постинге «снизу» после нового поста админа.
        cmd.CommandText = """
            CREATE TABLE IF NOT EXISTS ChannelMenus (
                ChatId    INTEGER PRIMARY KEY,
                MessageId INTEGER NOT NULL,
                UpdatedAt TEXT NOT NULL
            );
            """;
        cmd.ExecuteNonQuery();

        // Verses already sent to a user (avoid repeats)
        cmd.CommandText = """
            CREATE TABLE IF NOT EXISTS SentVerses (
                TelegramId INTEGER NOT NULL,
                VerseRef   TEXT NOT NULL,
                SentAt     TEXT NOT NULL,
                PRIMARY KEY (TelegramId, VerseRef)
            );
            """;
        cmd.ExecuteNonQuery();
    }

    // --- Messages ---

    public void SaveMessage(long telegramId, string role, string content)
    {
        using var conn = CreateConnection();
        conn.Open();
        var cmd = conn.CreateCommand();
        cmd.CommandText = """
            INSERT INTO Messages (TelegramId, Role, Content, CreatedAt)
            VALUES ($tid, $role, $content, $ts)
            """;
        cmd.Parameters.AddWithValue("$tid",     telegramId);
        cmd.Parameters.AddWithValue("$role",    role);
        cmd.Parameters.AddWithValue("$content", content);
        cmd.Parameters.AddWithValue("$ts",      DateTime.UtcNow.ToString("O"));
        cmd.ExecuteNonQuery();

        cmd.Parameters.Clear();
        cmd.CommandText = """
            DELETE FROM Messages WHERE TelegramId = $tid
            AND Id NOT IN (
                SELECT Id FROM Messages WHERE TelegramId = $tid
                ORDER BY Id DESC LIMIT 20
            )
            """;
        cmd.Parameters.AddWithValue("$tid", telegramId);
        cmd.ExecuteNonQuery();
    }

    public List<(string Role, string Content)> GetHistory(long telegramId, int limit = 10)
    {
        using var conn = CreateConnection();
        conn.Open();
        var cmd = conn.CreateCommand();
        cmd.CommandText = """
            SELECT Role, Content FROM (
                SELECT Id, Role, Content FROM Messages
                WHERE TelegramId = $tid
                ORDER BY Id DESC LIMIT $limit
            ) ORDER BY Id ASC
            """;
        cmd.Parameters.AddWithValue("$tid",   telegramId);
        cmd.Parameters.AddWithValue("$limit", limit);
        using var reader = cmd.ExecuteReader();
        var list = new List<(string, string)>();
        while (reader.Read())
            list.Add((reader.GetString(0), reader.GetString(1)));
        return list;
    }

    // --- Users ---

    public User? GetUserByTelegramId(long telegramId)
    {
        using var conn = CreateConnection();
        conn.Open();
        var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT Id, TelegramId, Username, FirstName, RegisteredAt, NotifyMode, OnboardingDone FROM Users WHERE TelegramId = $tid";
        cmd.Parameters.AddWithValue("$tid", telegramId);
        using var reader = cmd.ExecuteReader();
        if (!reader.Read()) return null;
        return MapUser(reader);
    }

    public void UpsertUser(User user)
    {
        using var conn = CreateConnection();
        conn.Open();
        var cmd = conn.CreateCommand();
        cmd.CommandText = """
            INSERT INTO Users (TelegramId, Username, FirstName, RegisteredAt, NotifyMode)
            VALUES ($tid, $uname, $fname, $reg, 'daily')
            ON CONFLICT(TelegramId) DO UPDATE SET
                Username  = excluded.Username,
                FirstName = excluded.FirstName;
            """;
        cmd.Parameters.AddWithValue("$tid",   user.TelegramId);
        cmd.Parameters.AddWithValue("$uname", user.Username ?? (object)DBNull.Value);
        cmd.Parameters.AddWithValue("$fname", user.FirstName);
        cmd.Parameters.AddWithValue("$reg",   user.RegisteredAt.ToString("O"));
        cmd.ExecuteNonQuery();
    }

    public void SetNotifyMode(long telegramId, string mode)
    {
        using var conn = CreateConnection();
        conn.Open();
        var cmd = conn.CreateCommand();
        cmd.CommandText = "UPDATE Users SET NotifyMode = $mode WHERE TelegramId = $tid";
        cmd.Parameters.AddWithValue("$mode", mode);
        cmd.Parameters.AddWithValue("$tid",  telegramId);
        cmd.ExecuteNonQuery();
    }

    public List<User> GetAllUsers()
    {
        using var conn = CreateConnection();
        conn.Open();
        var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT Id, TelegramId, Username, FirstName, RegisteredAt, NotifyMode, OnboardingDone FROM Users";
        using var reader = cmd.ExecuteReader();
        var list = new List<User>();
        while (reader.Read()) list.Add(MapUser(reader));
        return list;
    }

    public void SetLastBotMessageId(long telegramId, int messageId)
    {
        using var conn = CreateConnection();
        conn.Open();
        var cmd = conn.CreateCommand();
        cmd.CommandText = "UPDATE Users SET LastBotMessageId = $mid WHERE TelegramId = $tid";
        cmd.Parameters.AddWithValue("$mid", messageId);
        cmd.Parameters.AddWithValue("$tid", telegramId);
        cmd.ExecuteNonQuery();
    }

    public int? GetLastBotMessageId(long telegramId)
    {
        using var conn = CreateConnection();
        conn.Open();
        var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT LastBotMessageId FROM Users WHERE TelegramId = $tid";
        cmd.Parameters.AddWithValue("$tid", telegramId);
        var result = cmd.ExecuteScalar();
        if (result is null || result is DBNull) return null;
        return Convert.ToInt32(result);
    }

    public void MarkOnboardingDone(long telegramId)
    {
        using var conn = CreateConnection();
        conn.Open();
        var cmd = conn.CreateCommand();
        cmd.CommandText = "UPDATE Users SET OnboardingDone = 1 WHERE TelegramId = $tid";
        cmd.Parameters.AddWithValue("$tid", telegramId);
        cmd.ExecuteNonQuery();
    }

    // --- Books ---

    public Book? GetBookById(long id)
    {
        using var conn = CreateConnection();
        conn.Open();
        var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT Id, Title, Author, Description, Tags, Url, Type, AudioUrl, IssueId, ReleasedAt, CoverUrl FROM Books WHERE Id = $id";
        cmd.Parameters.AddWithValue("$id", id);
        using var reader = cmd.ExecuteReader();
        if (!reader.Read()) return null;
        return MapBook(reader);
    }

    public List<Book> GetAllBooks()
    {
        using var conn = CreateConnection();
        conn.Open();
        var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT Id, Title, Author, Description, Tags, Url, Type, AudioUrl, IssueId, ReleasedAt, CoverUrl FROM Books";
        using var reader = cmd.ExecuteReader();
        var list = new List<Book>();
        while (reader.Read()) list.Add(MapBook(reader));
        return list;
    }

    public void AddBook(Book book)
    {
        using var conn = CreateConnection();
        conn.Open();
        var cmd = conn.CreateCommand();
        cmd.CommandText = """
            INSERT INTO Books (Title, Author, Description, Tags, Url, Type, CoverUrl)
            VALUES ($title, $author, $desc, $tags, $url, $type, $cover)
            """;
        cmd.Parameters.AddWithValue("$title",  book.Title);
        cmd.Parameters.AddWithValue("$author", book.Author);
        cmd.Parameters.AddWithValue("$desc",   book.Description);
        cmd.Parameters.AddWithValue("$tags",   book.Tags);
        cmd.Parameters.AddWithValue("$url",    book.Url);
        cmd.Parameters.AddWithValue("$type",   book.Type);
        cmd.Parameters.AddWithValue("$cover",  book.CoverUrl ?? "");
        cmd.ExecuteNonQuery();
    }

    public bool BookExistsByUrl(string url)
    {
        if (string.IsNullOrWhiteSpace(url)) return false;
        using var conn = CreateConnection();
        conn.Open();
        var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT COUNT(*) FROM Books WHERE Url = $url";
        cmd.Parameters.AddWithValue("$url", url);
        return Convert.ToInt32(cmd.ExecuteScalar()) > 0;
    }

    public bool BookExistsByTitle(string title)
    {
        using var conn = CreateConnection();
        conn.Open();
        var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT COUNT(*) FROM Books WHERE LOWER(Title) = LOWER($title)";
        cmd.Parameters.AddWithValue("$title", title);
        return Convert.ToInt32(cmd.ExecuteScalar()) > 0;
    }

    public int GetBooksCount()
    {
        using var conn = CreateConnection();
        conn.Open();
        var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT COUNT(*) FROM Books";
        return Convert.ToInt32(cmd.ExecuteScalar());
    }

    public string? GetCachedAnnotation(long bookId)
    {
        using var conn = CreateConnection();
        conn.Open();
        var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT AiAnnotation FROM Books WHERE Id = $id";
        cmd.Parameters.AddWithValue("$id", bookId);
        var result = cmd.ExecuteScalar() as string;
        return string.IsNullOrWhiteSpace(result) ? null : result;
    }

    public void SaveCachedAnnotation(long bookId, string annotation)
    {
        using var conn = CreateConnection();
        conn.Open();
        var cmd = conn.CreateCommand();
        cmd.CommandText = "UPDATE Books SET AiAnnotation = $a WHERE Id = $id";
        cmd.Parameters.AddWithValue("$a",  annotation);
        cmd.Parameters.AddWithValue("$id", bookId);
        cmd.ExecuteNonQuery();
    }

    public int GetCountByType(string type)
    {
        using var conn = CreateConnection();
        conn.Open();
        var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT COUNT(*) FROM Books WHERE Type = $type";
        cmd.Parameters.AddWithValue("$type", type);
        return Convert.ToInt32(cmd.ExecuteScalar());
    }

    public bool MagazineExistsBySlug(string slug)
    {
        using var conn = CreateConnection();
        conn.Open();
        var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT 1 FROM Magazines WHERE Slug = $s LIMIT 1";
        cmd.Parameters.AddWithValue("$s", slug);
        return cmd.ExecuteScalar() != null;
    }

    public void AddMagazine(string slug, string title, string url)
    {
        using var conn = CreateConnection();
        conn.Open();
        var cmd = conn.CreateCommand();
        cmd.CommandText = "INSERT INTO Magazines (Slug, Title, Url) VALUES ($s, $t, $u)";
        cmd.Parameters.AddWithValue("$s", slug);
        cmd.Parameters.AddWithValue("$t", title);
        cmd.Parameters.AddWithValue("$u", url);
        cmd.ExecuteNonQuery();
    }

    public long? GetMagazineIdBySlug(string slug)
    {
        using var conn = CreateConnection();
        conn.Open();
        var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT Id FROM Magazines WHERE Slug = $s LIMIT 1";
        cmd.Parameters.AddWithValue("$s", slug);
        var result = cmd.ExecuteScalar();
        return result != null ? (long)result : null;
    }

    public bool MagazineIssueExists(long magazineId, string title)
    {
        using var conn = CreateConnection();
        conn.Open();
        var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT 1 FROM MagazineIssues WHERE MagazineId = $m AND Title = $t LIMIT 1";
        cmd.Parameters.AddWithValue("$m", magazineId);
        cmd.Parameters.AddWithValue("$t", title);
        return cmd.ExecuteScalar() != null;
    }

    public void AddMagazineIssue(long magazineId, string title, string url, string coverUrl, string? releasedAt)
    {
        using var conn = CreateConnection();
        conn.Open();
        var cmd = conn.CreateCommand();
        cmd.CommandText = """
            INSERT INTO MagazineIssues (MagazineId, Title, Url, CoverUrl, ReleasedAt)
            VALUES ($m, $t, $u, $c, $r)
            """;
        cmd.Parameters.AddWithValue("$m", magazineId);
        cmd.Parameters.AddWithValue("$t", title);
        cmd.Parameters.AddWithValue("$u", url);
        cmd.Parameters.AddWithValue("$c", coverUrl);
        cmd.Parameters.AddWithValue("$r", (object?)releasedAt ?? DBNull.Value);
        cmd.ExecuteNonQuery();
    }

    public void UpdateMagazineIssueUrl(long magazineId, string title, string url)
    {
        using var conn = CreateConnection();
        conn.Open();
        var cmd = conn.CreateCommand();
        cmd.CommandText = "UPDATE MagazineIssues SET Url = $u WHERE MagazineId = $m AND Title = $t";
        cmd.Parameters.AddWithValue("$u", url);
        cmd.Parameters.AddWithValue("$m", magazineId);
        cmd.Parameters.AddWithValue("$t", title);
        cmd.ExecuteNonQuery();
    }

    public List<(long Id, string Title, string Url, string CoverUrl, string? ReleasedAt)> GetMagazineIssues(long magazineId)
    {
        using var conn = CreateConnection();
        conn.Open();
        var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT Id, Title, Url, CoverUrl, ReleasedAt FROM MagazineIssues WHERE MagazineId = $m ORDER BY ReleasedAt DESC, Id DESC";
        cmd.Parameters.AddWithValue("$m", magazineId);
        using var reader = cmd.ExecuteReader();
        var list = new List<(long, string, string, string, string?)>();
        while (reader.Read())
            list.Add((reader.GetInt64(0), reader.GetString(1), reader.GetString(2),
                reader.GetString(3), reader.IsDBNull(4) ? null : reader.GetString(4)));
        return list;
    }

    public List<(long Id, string Slug, string Title, string Url)> GetAllMagazines()
    {
        using var conn = CreateConnection();
        conn.Open();
        var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT Id, Slug, Title, Url FROM Magazines ORDER BY Title";
        using var reader = cmd.ExecuteReader();
        var list = new List<(long, string, string, string)>();
        while (reader.Read())
            list.Add((reader.GetInt64(0), reader.GetString(1), reader.GetString(2), reader.GetString(3)));
        return list;
    }

    public List<Book> GetByType(string type, int? limit = null, int offset = 0)
    {
        using var conn = CreateConnection();
        conn.Open();
        var cmd = conn.CreateCommand();
        var sql = "SELECT Id, Title, Author, Description, Tags, Url, Type, AudioUrl, IssueId, ReleasedAt, CoverUrl FROM Books WHERE Type = $type ORDER BY Id DESC";
        if (limit.HasValue) sql += $" LIMIT {limit.Value} OFFSET {offset}";
        cmd.CommandText = sql;
        cmd.Parameters.AddWithValue("$type", type);
        using var reader = cmd.ExecuteReader();
        var list = new List<Book>();
        while (reader.Read()) list.Add(MapBook(reader));
        return list;
    }

    // --- Авторы (на базе поля Books.Author) ---

    // Возвращает (Author, Count) по убыванию числа материалов.
    // Авторы с пустым именем отсекаются.
    public List<(string Author, int Count)> GetTopAuthors(int? limit = null, int offset = 0)
    {
        using var conn = CreateConnection();
        conn.Open();
        var cmd = conn.CreateCommand();
        var sql = "SELECT TRIM(Author) AS A, COUNT(*) AS C FROM Books " +
                  "WHERE Author IS NOT NULL AND TRIM(Author) <> '' " +
                  "GROUP BY A ORDER BY C DESC, A ASC";
        if (limit.HasValue) sql += $" LIMIT {limit.Value} OFFSET {offset}";
        cmd.CommandText = sql;
        using var reader = cmd.ExecuteReader();
        var list = new List<(string, int)>();
        while (reader.Read())
            list.Add((reader.GetString(0), reader.GetInt32(1)));
        return list;
    }

    public int CountAuthors()
    {
        using var conn = CreateConnection();
        conn.Open();
        var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT COUNT(DISTINCT TRIM(Author)) FROM Books " +
                          "WHERE Author IS NOT NULL AND TRIM(Author) <> ''";
        return Convert.ToInt32(cmd.ExecuteScalar());
    }

    public List<Book> GetByAuthor(string author, int? limit = null, int offset = 0)
    {
        using var conn = CreateConnection();
        conn.Open();
        var cmd = conn.CreateCommand();
        var sql = "SELECT Id, Title, Author, Description, Tags, Url, Type, AudioUrl, IssueId, ReleasedAt, CoverUrl FROM Books " +
                  "WHERE TRIM(Author) = $author ORDER BY Id DESC";
        if (limit.HasValue) sql += $" LIMIT {limit.Value} OFFSET {offset}";
        cmd.CommandText = sql;
        cmd.Parameters.AddWithValue("$author", author.Trim());
        using var reader = cmd.ExecuteReader();
        var list = new List<Book>();
        while (reader.Read()) list.Add(MapBook(reader));
        return list;
    }

    // --- SeenBooks ---

    public HashSet<long> GetSeenBookIds(long telegramId)
    {
        using var conn = CreateConnection();
        conn.Open();
        var cmd = conn.CreateCommand();
        cmd.CommandText = """
            SELECT BookId FROM SeenBooks
            WHERE TelegramId = $tid AND SeenAt > $cutoff
            """;
        cmd.Parameters.AddWithValue("$tid",    telegramId);
        cmd.Parameters.AddWithValue("$cutoff", DateTime.UtcNow.AddDays(-14).ToString("O"));
        using var reader = cmd.ExecuteReader();
        var ids = new HashSet<long>();
        while (reader.Read()) ids.Add(reader.GetInt64(0));
        return ids;
    }

    public void MarkBooksAsSeen(long telegramId, IEnumerable<long> bookIds)
    {
        var ids = bookIds.ToList();
        if (ids.Count == 0) return;

        using var conn = CreateConnection();
        conn.Open();
        using var tx = conn.BeginTransaction();
        var cmd = conn.CreateCommand();
        cmd.Transaction = tx;
        cmd.CommandText = "INSERT OR REPLACE INTO SeenBooks (TelegramId, BookId, SeenAt) VALUES ($tid, $bid, $ts)";
        cmd.Parameters.AddWithValue("$tid", telegramId);
        cmd.Parameters.AddWithValue("$bid", 0L);
        cmd.Parameters.AddWithValue("$ts",  DateTime.UtcNow.ToString("O"));
        foreach (var id in ids)
        {
            cmd.Parameters["$bid"].Value = id;
            cmd.ExecuteNonQuery();
        }
        tx.Commit();
    }

    // --- ReadBooks ---

    public void MarkBookAsRead(long telegramId, long bookId)
    {
        using var conn = CreateConnection();
        conn.Open();
        var cmd = conn.CreateCommand();
        cmd.CommandText = "INSERT OR REPLACE INTO ReadBooks (TelegramId, BookId, MarkedAt) VALUES ($tid, $bid, $ts)";
        cmd.Parameters.AddWithValue("$tid", telegramId);
        cmd.Parameters.AddWithValue("$bid", bookId);
        cmd.Parameters.AddWithValue("$ts",  DateTime.UtcNow.ToString("O"));
        cmd.ExecuteNonQuery();
    }

    public HashSet<long> GetReadBookIds(long telegramId)
    {
        using var conn = CreateConnection();
        conn.Open();
        var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT BookId FROM ReadBooks WHERE TelegramId = $tid";
        cmd.Parameters.AddWithValue("$tid", telegramId);
        using var reader = cmd.ExecuteReader();
        var ids = new HashSet<long>();
        while (reader.Read()) ids.Add(reader.GetInt64(0));
        return ids;
    }

    public List<Book> GetReadBooks(long telegramId)
    {
        using var conn = CreateConnection();
        conn.Open();
        var cmd = conn.CreateCommand();
        cmd.CommandText = """
            SELECT b.Id, b.Title, b.Author, b.Description, b.Tags, b.Url, b.Type, b.AudioUrl, b.IssueId, b.ReleasedAt, b.CoverUrl
            FROM Books b
            INNER JOIN ReadBooks r ON r.BookId = b.Id
            WHERE r.TelegramId = $tid
            ORDER BY r.MarkedAt DESC
            """;
        cmd.Parameters.AddWithValue("$tid", telegramId);
        using var reader = cmd.ExecuteReader();
        var list = new List<Book>();
        while (reader.Read()) list.Add(MapBook(reader));
        return list;
    }

    // --- IgnoredBooks ---

    public void MarkBookAsIgnored(long telegramId, long bookId)
    {
        using var conn = CreateConnection();
        conn.Open();
        var cmd = conn.CreateCommand();
        cmd.CommandText = "INSERT OR REPLACE INTO IgnoredBooks (TelegramId, BookId, MarkedAt) VALUES ($tid, $bid, $ts)";
        cmd.Parameters.AddWithValue("$tid", telegramId);
        cmd.Parameters.AddWithValue("$bid", bookId);
        cmd.Parameters.AddWithValue("$ts",  DateTime.UtcNow.ToString("O"));
        cmd.ExecuteNonQuery();
    }

    public HashSet<long> GetIgnoredBookIds(long telegramId)
    {
        using var conn = CreateConnection();
        conn.Open();
        var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT BookId FROM IgnoredBooks WHERE TelegramId = $tid";
        cmd.Parameters.AddWithValue("$tid", telegramId);
        using var reader = cmd.ExecuteReader();
        var ids = new HashSet<long>();
        while (reader.Read()) ids.Add(reader.GetInt64(0));
        return ids;
    }

    // --- Favorites ---

    public void AddFavorite(long telegramId, long contentId)
    {
        using var conn = CreateConnection();
        conn.Open();
        var cmd = conn.CreateCommand();
        cmd.CommandText = "INSERT OR REPLACE INTO Favorites (TelegramId, ContentId, AddedAt) VALUES ($tid, $cid, $ts)";
        cmd.Parameters.AddWithValue("$tid", telegramId);
        cmd.Parameters.AddWithValue("$cid", contentId);
        cmd.Parameters.AddWithValue("$ts",  DateTime.UtcNow.ToString("O"));
        cmd.ExecuteNonQuery();
    }

    public void RemoveFavorite(long telegramId, long contentId)
    {
        using var conn = CreateConnection();
        conn.Open();
        var cmd = conn.CreateCommand();
        cmd.CommandText = "DELETE FROM Favorites WHERE TelegramId = $tid AND ContentId = $cid";
        cmd.Parameters.AddWithValue("$tid", telegramId);
        cmd.Parameters.AddWithValue("$cid", contentId);
        cmd.ExecuteNonQuery();
    }

    public bool IsFavorite(long telegramId, long contentId)
    {
        using var conn = CreateConnection();
        conn.Open();
        var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT COUNT(*) FROM Favorites WHERE TelegramId = $tid AND ContentId = $cid";
        cmd.Parameters.AddWithValue("$tid", telegramId);
        cmd.Parameters.AddWithValue("$cid", contentId);
        return Convert.ToInt32(cmd.ExecuteScalar()) > 0;
    }

    public List<Book> GetFavorites(long telegramId)
    {
        using var conn = CreateConnection();
        conn.Open();
        var cmd = conn.CreateCommand();
        cmd.CommandText = """
            SELECT b.Id, b.Title, b.Author, b.Description, b.Tags, b.Url, b.Type, b.AudioUrl, b.IssueId, b.ReleasedAt, b.CoverUrl
            FROM Books b
            INNER JOIN Favorites f ON f.ContentId = b.Id
            WHERE f.TelegramId = $tid
            ORDER BY f.AddedAt DESC
            """;
        cmd.Parameters.AddWithValue("$tid", telegramId);
        using var reader = cmd.ExecuteReader();
        var list = new List<Book>();
        while (reader.Read()) list.Add(MapBook(reader));
        return list;
    }

    // --- Mappers ---

    private static User MapUser(SqliteDataReader r) => new()
    {
        Id             = r.GetInt64(0),
        TelegramId     = r.GetInt64(1),
        Username       = r.IsDBNull(2) ? null : r.GetString(2),
        FirstName      = r.GetString(3),
        RegisteredAt   = DateTime.Parse(r.GetString(4)),
        NotifyMode     = r.IsDBNull(5) ? "daily" : r.GetString(5),
        OnboardingDone = r.FieldCount > 6 && !r.IsDBNull(6) && r.GetInt64(6) == 1
    };

    private static Book MapBook(SqliteDataReader r) => new()
    {
        Id          = r.GetInt64(0),
        Title       = r.GetString(1),
        Author      = r.GetString(2),
        Description = r.GetString(3),
        Tags        = r.GetString(4),
        Url         = r.IsDBNull(5) ? string.Empty : r.GetString(5),
        Type        = r.IsDBNull(6) ? "book" : r.GetString(6),
        AudioUrl    = r.FieldCount > 7 && !r.IsDBNull(7) ? r.GetString(7) : string.Empty,
        IssueId     = r.FieldCount > 8 && !r.IsDBNull(8) ? r.GetInt64(8) : null,
        ReleasedAt  = r.FieldCount > 9 && !r.IsDBNull(9) ? DateTime.Parse(r.GetString(9)) : null,
        CoverUrl    = r.FieldCount > 10 && !r.IsDBNull(10) ? r.GetString(10) : string.Empty
    };

    public List<(long Id, string Url)> GetBooksWithoutCover(string? typeFilter = null, int? limit = null)
    {
        using var conn = CreateConnection();
        conn.Open();
        var cmd = conn.CreateCommand();
        var sql = "SELECT Id, Url FROM Books WHERE (CoverUrl IS NULL OR CoverUrl = '') AND Url <> ''";
        if (typeFilter != null)
        {
            sql += " AND Type = $t";
            cmd.Parameters.AddWithValue("$t", typeFilter);
        }
        if (limit.HasValue) sql += $" LIMIT {limit.Value}";
        cmd.CommandText = sql;
        using var reader = cmd.ExecuteReader();
        var list = new List<(long, string)>();
        while (reader.Read()) list.Add((reader.GetInt64(0), reader.GetString(1)));
        return list;
    }

    // ── Channel auto-bottom menu ──────────────────────────────────

    public int? GetChannelMenuMessageId(long chatId)
    {
        using var conn = CreateConnection();
        conn.Open();
        var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT MessageId FROM ChannelMenus WHERE ChatId = $c";
        cmd.Parameters.AddWithValue("$c", chatId);
        var r = cmd.ExecuteScalar();
        return r is null or DBNull ? null : Convert.ToInt32(r);
    }

    public void SetChannelMenuMessageId(long chatId, int messageId)
    {
        using var conn = CreateConnection();
        conn.Open();
        var cmd = conn.CreateCommand();
        cmd.CommandText = """
            INSERT INTO ChannelMenus (ChatId, MessageId, UpdatedAt) VALUES ($c, $m, $t)
            ON CONFLICT(ChatId) DO UPDATE SET MessageId = excluded.MessageId, UpdatedAt = excluded.UpdatedAt;
            """;
        cmd.Parameters.AddWithValue("$c", chatId);
        cmd.Parameters.AddWithValue("$m", messageId);
        cmd.Parameters.AddWithValue("$t", DateTime.UtcNow.ToString("O"));
        cmd.ExecuteNonQuery();
    }

    public bool IsChannelMenuRegistered(long chatId)
    {
        using var conn = CreateConnection();
        conn.Open();
        var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT 1 FROM ChannelMenus WHERE ChatId = $c LIMIT 1";
        cmd.Parameters.AddWithValue("$c", chatId);
        return cmd.ExecuteScalar() != null;
    }

    public void SetBookCover(long bookId, string coverUrl)
    {
        using var conn = CreateConnection();
        conn.Open();
        var cmd = conn.CreateCommand();
        cmd.CommandText = "UPDATE Books SET CoverUrl = $c WHERE Id = $id";
        cmd.Parameters.AddWithValue("$c",  coverUrl ?? "");
        cmd.Parameters.AddWithValue("$id", bookId);
        cmd.ExecuteNonQuery();
    }
}
