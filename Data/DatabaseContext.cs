using Microsoft.Data.Sqlite;
using Microsoft.Extensions.Configuration;
using LioBot.Models;

namespace LioBot.Data;

public class DatabaseContext
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
            "ALTER TABLE Users ADD COLUMN NotifyMode TEXT NOT NULL DEFAULT 'daily'"
        })
        {
            cmd.CommandText = migration;
            try { cmd.ExecuteNonQuery(); } catch { /* already exists */ }
        }

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
        cmd.CommandText = "SELECT Id, TelegramId, Username, FirstName, RegisteredAt, NotifyMode FROM Users WHERE TelegramId = $tid";
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
        cmd.CommandText = "SELECT Id, TelegramId, Username, FirstName, RegisteredAt, NotifyMode FROM Users";
        using var reader = cmd.ExecuteReader();
        var list = new List<User>();
        while (reader.Read()) list.Add(MapUser(reader));
        return list;
    }

    // --- Books ---

    public Book? GetBookById(long id)
    {
        using var conn = CreateConnection();
        conn.Open();
        var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT Id, Title, Author, Description, Tags, Url, Type FROM Books WHERE Id = $id";
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
        cmd.CommandText = "SELECT Id, Title, Author, Description, Tags, Url, Type FROM Books";
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
            INSERT INTO Books (Title, Author, Description, Tags, Url, Type)
            VALUES ($title, $author, $desc, $tags, $url, $type)
            """;
        cmd.Parameters.AddWithValue("$title",  book.Title);
        cmd.Parameters.AddWithValue("$author", book.Author);
        cmd.Parameters.AddWithValue("$desc",   book.Description);
        cmd.Parameters.AddWithValue("$tags",   book.Tags);
        cmd.Parameters.AddWithValue("$url",    book.Url);
        cmd.Parameters.AddWithValue("$type",   book.Type);
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

    public int GetCountByType(string type)
    {
        using var conn = CreateConnection();
        conn.Open();
        var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT COUNT(*) FROM Books WHERE Type = $type";
        cmd.Parameters.AddWithValue("$type", type);
        return Convert.ToInt32(cmd.ExecuteScalar());
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
            SELECT b.Id, b.Title, b.Author, b.Description, b.Tags, b.Url, b.Type
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

    // --- Mappers ---

    private static User MapUser(SqliteDataReader r) => new()
    {
        Id           = r.GetInt64(0),
        TelegramId   = r.GetInt64(1),
        Username     = r.IsDBNull(2) ? null : r.GetString(2),
        FirstName    = r.GetString(3),
        RegisteredAt = DateTime.Parse(r.GetString(4)),
        NotifyMode   = r.IsDBNull(5) ? "daily" : r.GetString(5)
    };

    private static Book MapBook(SqliteDataReader r) => new()
    {
        Id          = r.GetInt64(0),
        Title       = r.GetString(1),
        Author      = r.GetString(2),
        Description = r.GetString(3),
        Tags        = r.GetString(4),
        Url         = r.IsDBNull(5) ? string.Empty : r.GetString(5),
        Type        = r.IsDBNull(6) ? "book" : r.GetString(6)
    };
}
