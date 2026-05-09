using LioBot.Models;
using Microsoft.Data.Sqlite;

namespace LioBot.Data;

public partial class DatabaseContext
{
    // ────────────────────────────────────────────────────────────
    // UserPreferences (онбординг)
    // ────────────────────────────────────────────────────────────

    public UserPreferences? GetPreferences(long telegramId)
    {
        using var conn = CreateConnection();
        conn.Open();
        var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT TelegramId, FaithStage, Interests, COALESCE(Languages, ''), UpdatedAt FROM UserPreferences WHERE TelegramId = $tid";
        cmd.Parameters.AddWithValue("$tid", telegramId);
        using var r = cmd.ExecuteReader();
        if (!r.Read()) return null;
        return new UserPreferences
        {
            TelegramId = r.GetInt64(0),
            FaithStage = r.GetString(1),
            Interests  = r.GetString(2),
            Languages  = r.GetString(3),
            UpdatedAt  = DateTime.Parse(r.GetString(4))
        };
    }

    public void ToggleLanguage(long telegramId, string lang)
    {
        var prefs = GetPreferences(telegramId);
        var current = (prefs?.Languages ?? "")
            .Split(',', StringSplitOptions.RemoveEmptyEntries)
            .Select(s => s.Trim())
            .Where(s => s.Length > 0)
            .ToList();

        if (current.Contains(lang, StringComparer.OrdinalIgnoreCase))
            current.RemoveAll(s => string.Equals(s, lang, StringComparison.OrdinalIgnoreCase));
        else
            current.Add(lang);

        using var conn = CreateConnection();
        conn.Open();
        var cmd = conn.CreateCommand();
        cmd.CommandText = """
            INSERT INTO UserPreferences (TelegramId, FaithStage, Interests, Languages, UpdatedAt)
            VALUES ($tid, '', '', $langs, $ts)
            ON CONFLICT(TelegramId) DO UPDATE SET Languages = excluded.Languages, UpdatedAt = excluded.UpdatedAt
            """;
        cmd.Parameters.AddWithValue("$tid",   telegramId);
        cmd.Parameters.AddWithValue("$langs", string.Join(",", current));
        cmd.Parameters.AddWithValue("$ts",    DateTime.UtcNow.ToString("O"));
        cmd.ExecuteNonQuery();
    }

    public HashSet<string> GetUserLanguages(long telegramId)
    {
        var prefs = GetPreferences(telegramId);
        return (prefs?.Languages ?? "")
            .Split(',', StringSplitOptions.RemoveEmptyEntries)
            .Select(s => s.Trim().ToLowerInvariant())
            .Where(s => s.Length > 0)
            .ToHashSet();
    }

    public void SetFaithStage(long telegramId, string stage)
    {
        using var conn = CreateConnection();
        conn.Open();
        var cmd = conn.CreateCommand();
        cmd.CommandText = """
            INSERT INTO UserPreferences (TelegramId, FaithStage, Interests, UpdatedAt)
            VALUES ($tid, $stage, '', $ts)
            ON CONFLICT(TelegramId) DO UPDATE SET FaithStage = excluded.FaithStage, UpdatedAt = excluded.UpdatedAt
            """;
        cmd.Parameters.AddWithValue("$tid",   telegramId);
        cmd.Parameters.AddWithValue("$stage", stage);
        cmd.Parameters.AddWithValue("$ts",    DateTime.UtcNow.ToString("O"));
        cmd.ExecuteNonQuery();
    }

    public void ToggleInterest(long telegramId, string tag)
    {
        var prefs = GetPreferences(telegramId);
        var current = (prefs?.Interests ?? "")
            .Split(',', StringSplitOptions.RemoveEmptyEntries)
            .Select(s => s.Trim())
            .Where(s => s.Length > 0)
            .ToList();

        if (current.Contains(tag, StringComparer.OrdinalIgnoreCase))
            current.RemoveAll(s => string.Equals(s, tag, StringComparison.OrdinalIgnoreCase));
        else
            current.Add(tag);

        using var conn = CreateConnection();
        conn.Open();
        var cmd = conn.CreateCommand();
        cmd.CommandText = """
            INSERT INTO UserPreferences (TelegramId, FaithStage, Interests, UpdatedAt)
            VALUES ($tid, '', $interests, $ts)
            ON CONFLICT(TelegramId) DO UPDATE SET Interests = excluded.Interests, UpdatedAt = excluded.UpdatedAt
            """;
        cmd.Parameters.AddWithValue("$tid",       telegramId);
        cmd.Parameters.AddWithValue("$interests", string.Join(",", current));
        cmd.Parameters.AddWithValue("$ts",        DateTime.UtcNow.ToString("O"));
        cmd.ExecuteNonQuery();
    }

    // ────────────────────────────────────────────────────────────
    // BookRatings
    // ────────────────────────────────────────────────────────────

    public void SetRating(long telegramId, long bookId, int rating)
    {
        rating = Math.Clamp(rating, 1, 5);
        using var conn = CreateConnection();
        conn.Open();
        var cmd = conn.CreateCommand();
        cmd.CommandText = """
            INSERT INTO BookRatings (TelegramId, BookId, Rating, CreatedAt)
            VALUES ($tid, $bid, $rating, $ts)
            ON CONFLICT(TelegramId, BookId) DO UPDATE SET Rating = excluded.Rating, CreatedAt = excluded.CreatedAt
            """;
        cmd.Parameters.AddWithValue("$tid",    telegramId);
        cmd.Parameters.AddWithValue("$bid",    bookId);
        cmd.Parameters.AddWithValue("$rating", rating);
        cmd.Parameters.AddWithValue("$ts",     DateTime.UtcNow.ToString("O"));
        cmd.ExecuteNonQuery();
    }

    public int? GetRating(long telegramId, long bookId)
    {
        using var conn = CreateConnection();
        conn.Open();
        var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT Rating FROM BookRatings WHERE TelegramId = $tid AND BookId = $bid";
        cmd.Parameters.AddWithValue("$tid", telegramId);
        cmd.Parameters.AddWithValue("$bid", bookId);
        var obj = cmd.ExecuteScalar();
        return obj is null || obj is DBNull ? null : Convert.ToInt32(obj);
    }

    public Dictionary<long, int> GetRatingsMap(long telegramId)
    {
        using var conn = CreateConnection();
        conn.Open();
        var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT BookId, Rating FROM BookRatings WHERE TelegramId = $tid";
        cmd.Parameters.AddWithValue("$tid", telegramId);
        using var r = cmd.ExecuteReader();
        var map = new Dictionary<long, int>();
        while (r.Read()) map[r.GetInt64(0)] = r.GetInt32(1);
        return map;
    }

    // ────────────────────────────────────────────────────────────
    // ReadingNow
    // ────────────────────────────────────────────────────────────

    public void MarkBookAsReading(long telegramId, long bookId)
    {
        using var conn = CreateConnection();
        conn.Open();
        var cmd = conn.CreateCommand();
        cmd.CommandText = """
            INSERT INTO ReadingNow (TelegramId, BookId, StartedAt, LastRemindedAt)
            VALUES ($tid, $bid, $ts, NULL)
            ON CONFLICT(TelegramId, BookId) DO NOTHING
            """;
        cmd.Parameters.AddWithValue("$tid", telegramId);
        cmd.Parameters.AddWithValue("$bid", bookId);
        cmd.Parameters.AddWithValue("$ts",  DateTime.UtcNow.ToString("O"));
        cmd.ExecuteNonQuery();
    }

    public void StopReading(long telegramId, long bookId)
    {
        using var conn = CreateConnection();
        conn.Open();
        var cmd = conn.CreateCommand();
        cmd.CommandText = "DELETE FROM ReadingNow WHERE TelegramId = $tid AND BookId = $bid";
        cmd.Parameters.AddWithValue("$tid", telegramId);
        cmd.Parameters.AddWithValue("$bid", bookId);
        cmd.ExecuteNonQuery();
    }

    public List<Book> GetReadingNow(long telegramId)
    {
        using var conn = CreateConnection();
        conn.Open();
        var cmd = conn.CreateCommand();
        cmd.CommandText = """
            SELECT b.Id, b.Title, b.Author, b.Description, b.Tags, b.Url, b.Type
            FROM Books b
            INNER JOIN ReadingNow r ON r.BookId = b.Id
            WHERE r.TelegramId = $tid
            ORDER BY r.StartedAt DESC
            """;
        cmd.Parameters.AddWithValue("$tid", telegramId);
        using var reader = cmd.ExecuteReader();
        var list = new List<Book>();
        while (reader.Read())
            list.Add(new Book
            {
                Id          = reader.GetInt64(0),
                Title       = reader.GetString(1),
                Author      = reader.GetString(2),
                Description = reader.GetString(3),
                Tags        = reader.GetString(4),
                Url         = reader.IsDBNull(5) ? "" : reader.GetString(5),
                Type        = reader.IsDBNull(6) ? "book" : reader.GetString(6)
            });
        return list;
    }

    public List<(long TelegramId, long BookId, string Title, DateTime StartedAt)> GetStaleReading(TimeSpan olderThan)
    {
        var cutoff = DateTime.UtcNow - olderThan;
        using var conn = CreateConnection();
        conn.Open();
        var cmd = conn.CreateCommand();
        cmd.CommandText = """
            SELECT r.TelegramId, r.BookId, b.Title, r.StartedAt
            FROM ReadingNow r
            INNER JOIN Books b ON b.Id = r.BookId
            WHERE r.StartedAt < $cutoff
              AND (r.LastRemindedAt IS NULL OR r.LastRemindedAt < $cutoff)
            """;
        cmd.Parameters.AddWithValue("$cutoff", cutoff.ToString("O"));
        using var r = cmd.ExecuteReader();
        var list = new List<(long, long, string, DateTime)>();
        while (r.Read())
            list.Add((r.GetInt64(0), r.GetInt64(1), r.GetString(2), DateTime.Parse(r.GetString(3))));
        return list;
    }

    public void TouchReadingReminded(long telegramId, long bookId)
    {
        using var conn = CreateConnection();
        conn.Open();
        var cmd = conn.CreateCommand();
        cmd.CommandText = "UPDATE ReadingNow SET LastRemindedAt = $ts WHERE TelegramId = $tid AND BookId = $bid";
        cmd.Parameters.AddWithValue("$tid", telegramId);
        cmd.Parameters.AddWithValue("$bid", bookId);
        cmd.Parameters.AddWithValue("$ts",  DateTime.UtcNow.ToString("O"));
        cmd.ExecuteNonQuery();
    }

    // ────────────────────────────────────────────────────────────
    // UserState (FSM — ожидание ввода текста)
    // ────────────────────────────────────────────────────────────

    public void SetPending(long telegramId, string action, string context = "")
    {
        using var conn = CreateConnection();
        conn.Open();
        var cmd = conn.CreateCommand();
        cmd.CommandText = """
            INSERT INTO UserState (TelegramId, PendingAction, PendingContext, UpdatedAt)
            VALUES ($tid, $action, $ctx, $ts)
            ON CONFLICT(TelegramId) DO UPDATE SET
                PendingAction = excluded.PendingAction,
                PendingContext = excluded.PendingContext,
                UpdatedAt = excluded.UpdatedAt
            """;
        cmd.Parameters.AddWithValue("$tid",    telegramId);
        cmd.Parameters.AddWithValue("$action", action);
        cmd.Parameters.AddWithValue("$ctx",    context);
        cmd.Parameters.AddWithValue("$ts",     DateTime.UtcNow.ToString("O"));
        cmd.ExecuteNonQuery();
    }

    public (string Action, string Context)? GetPending(long telegramId)
    {
        using var conn = CreateConnection();
        conn.Open();
        var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT PendingAction, PendingContext FROM UserState WHERE TelegramId = $tid";
        cmd.Parameters.AddWithValue("$tid", telegramId);
        using var r = cmd.ExecuteReader();
        if (!r.Read()) return null;
        return (r.GetString(0), r.GetString(1));
    }

    public void ClearPending(long telegramId)
    {
        using var conn = CreateConnection();
        conn.Open();
        var cmd = conn.CreateCommand();
        cmd.CommandText = "DELETE FROM UserState WHERE TelegramId = $tid";
        cmd.Parameters.AddWithValue("$tid", telegramId);
        cmd.ExecuteNonQuery();
    }

    // ────────────────────────────────────────────────────────────
    // SentVerses (избегаем повтора стихов)
    // ────────────────────────────────────────────────────────────

    public bool WasVerseSent(long telegramId, string verseRef)
    {
        using var conn = CreateConnection();
        conn.Open();
        var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT COUNT(*) FROM SentVerses WHERE TelegramId = $tid AND VerseRef = $ref";
        cmd.Parameters.AddWithValue("$tid", telegramId);
        cmd.Parameters.AddWithValue("$ref", verseRef);
        return Convert.ToInt32(cmd.ExecuteScalar()) > 0;
    }

    public List<string> GetRecentSentVerses(long telegramId, int limit = 30)
    {
        using var conn = CreateConnection();
        conn.Open();
        var cmd = conn.CreateCommand();
        cmd.CommandText = """
            SELECT VerseRef FROM SentVerses
            WHERE TelegramId = $tid
            ORDER BY SentAt DESC
            LIMIT $limit
            """;
        cmd.Parameters.AddWithValue("$tid",   telegramId);
        cmd.Parameters.AddWithValue("$limit", limit);
        using var r = cmd.ExecuteReader();
        var list = new List<string>();
        while (r.Read()) list.Add(r.GetString(0));
        return list;
    }

    public void RecordVerseSent(long telegramId, string verseRef)
    {
        using var conn = CreateConnection();
        conn.Open();
        var cmd = conn.CreateCommand();
        cmd.CommandText = """
            INSERT INTO SentVerses (TelegramId, VerseRef, SentAt)
            VALUES ($tid, $ref, $ts)
            ON CONFLICT(TelegramId, VerseRef) DO UPDATE SET SentAt = excluded.SentAt
            """;
        cmd.Parameters.AddWithValue("$tid", telegramId);
        cmd.Parameters.AddWithValue("$ref", verseRef);
        cmd.Parameters.AddWithValue("$ts",  DateTime.UtcNow.ToString("O"));
        cmd.ExecuteNonQuery();
    }

    // ────────────────────────────────────────────────────────────
    // Cleanup — старые SeenBooks
    // ────────────────────────────────────────────────────────────

    public int CleanOldSeenBooks(TimeSpan olderThan)
    {
        using var conn = CreateConnection();
        conn.Open();
        var cmd = conn.CreateCommand();
        cmd.CommandText = "DELETE FROM SeenBooks WHERE SeenAt < $cutoff";
        cmd.Parameters.AddWithValue("$cutoff", (DateTime.UtcNow - olderThan).ToString("O"));
        return cmd.ExecuteNonQuery();
    }

    // ────────────────────────────────────────────────────────────
    // Статистика
    // ────────────────────────────────────────────────────────────

    public int GetUsersCount()
    {
        using var conn = CreateConnection();
        conn.Open();
        var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT COUNT(*) FROM Users";
        return Convert.ToInt32(cmd.ExecuteScalar());
    }

    public int GetActiveUsersCount(TimeSpan window)
    {
        using var conn = CreateConnection();
        conn.Open();
        var cmd = conn.CreateCommand();
        cmd.CommandText = """
            SELECT COUNT(DISTINCT TelegramId) FROM Messages
            WHERE CreatedAt > $cutoff AND Role = 'user'
            """;
        cmd.Parameters.AddWithValue("$cutoff", (DateTime.UtcNow - window).ToString("O"));
        return Convert.ToInt32(cmd.ExecuteScalar());
    }

    public List<(string Title, int Count)> GetTopRecommendedBooks(int limit = 5)
    {
        using var conn = CreateConnection();
        conn.Open();
        var cmd = conn.CreateCommand();
        cmd.CommandText = """
            SELECT b.Title, COUNT(*) AS c
            FROM SeenBooks s
            INNER JOIN Books b ON b.Id = s.BookId
            GROUP BY b.Id, b.Title
            ORDER BY c DESC
            LIMIT $limit
            """;
        cmd.Parameters.AddWithValue("$limit", limit);
        using var r = cmd.ExecuteReader();
        var list = new List<(string, int)>();
        while (r.Read()) list.Add((r.GetString(0), r.GetInt32(1)));
        return list;
    }

    public List<(string Title, double AvgRating, int Votes)> GetTopRatedBooks(int limit = 5, int minVotes = 1)
    {
        using var conn = CreateConnection();
        conn.Open();
        var cmd = conn.CreateCommand();
        cmd.CommandText = """
            SELECT b.Title, AVG(r.Rating) AS avg_r, COUNT(*) AS votes
            FROM BookRatings r
            INNER JOIN Books b ON b.Id = r.BookId
            GROUP BY b.Id, b.Title
            HAVING votes >= $min
            ORDER BY avg_r DESC, votes DESC
            LIMIT $limit
            """;
        cmd.Parameters.AddWithValue("$min",   minVotes);
        cmd.Parameters.AddWithValue("$limit", limit);
        using var r = cmd.ExecuteReader();
        var list = new List<(string, double, int)>();
        while (r.Read()) list.Add((r.GetString(0), r.GetDouble(1), r.GetInt32(2)));
        return list;
    }

    public int GetRatingsCount()
    {
        using var conn = CreateConnection();
        conn.Open();
        var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT COUNT(*) FROM BookRatings";
        return Convert.ToInt32(cmd.ExecuteScalar());
    }
}
