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

        // Create tables
        cmd.CommandText = """
            CREATE TABLE IF NOT EXISTS Users (
                Id          INTEGER PRIMARY KEY AUTOINCREMENT,
                TelegramId  INTEGER NOT NULL UNIQUE,
                Username    TEXT,
                FirstName   TEXT NOT NULL,
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

        // Migration: add Type column if DB existed before this feature
        cmd.CommandText = "ALTER TABLE Books ADD COLUMN Type TEXT NOT NULL DEFAULT 'book'";
        try { cmd.ExecuteNonQuery(); } catch { /* column already exists */ }

        // История сообщений
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

        // Оставляем только последние 20 сообщений на пользователя
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
        cmd.CommandText = "SELECT Id, TelegramId, Username, FirstName, RegisteredAt FROM Users WHERE TelegramId = $tid";
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
            INSERT INTO Users (TelegramId, Username, FirstName, RegisteredAt)
            VALUES ($tid, $uname, $fname, $reg)
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

    public List<User> GetAllUsers()
    {
        using var conn = CreateConnection();
        conn.Open();
        var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT Id, TelegramId, Username, FirstName, RegisteredAt FROM Users";
        using var reader = cmd.ExecuteReader();
        var list = new List<User>();
        while (reader.Read()) list.Add(MapUser(reader));
        return list;
    }

    // --- Books ---

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

    // --- Mappers ---

    private static User MapUser(SqliteDataReader r) => new()
    {
        Id           = r.GetInt64(0),
        TelegramId   = r.GetInt64(1),
        Username     = r.IsDBNull(2) ? null : r.GetString(2),
        FirstName    = r.GetString(3),
        RegisteredAt = DateTime.Parse(r.GetString(4))
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
