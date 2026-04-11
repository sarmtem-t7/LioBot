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
                Url         TEXT NOT NULL DEFAULT ''
            );
            """;
        cmd.ExecuteNonQuery();
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
        cmd.CommandText = "SELECT Id, Title, Author, Description, Tags, Url FROM Books";
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
            INSERT INTO Books (Title, Author, Description, Tags, Url)
            VALUES ($title, $author, $desc, $tags, $url)
            """;
        cmd.Parameters.AddWithValue("$title",  book.Title);
        cmd.Parameters.AddWithValue("$author", book.Author);
        cmd.Parameters.AddWithValue("$desc",   book.Description);
        cmd.Parameters.AddWithValue("$tags",   book.Tags);
        cmd.Parameters.AddWithValue("$url",    book.Url);
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
        Url         = r.IsDBNull(5) ? string.Empty : r.GetString(5)
    };
}
