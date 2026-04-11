using System.Reflection;
using System.Text.Json;

namespace LioBot.Data;

public static class BookSeeder
{
    public static void SeedIfEmpty(DatabaseContext db)
    {
        if (db.GetBooksCount() > 0)
            return;

        var assembly = Assembly.GetExecutingAssembly();
        const string resourceName = "LioBot.Data.books_seed.json";

        using var stream = assembly.GetManifestResourceStream(resourceName)
            ?? throw new InvalidOperationException($"Embedded resource '{resourceName}' not found.");

        var books = JsonSerializer.Deserialize<List<SeedBook>>(stream)
            ?? throw new InvalidOperationException("Failed to deserialize books_seed.json");

        using var conn = db.CreateConnection();
        conn.Open();
        using var transaction = conn.BeginTransaction();

        foreach (var b in books)
        {
            var cmd = conn.CreateCommand();
            cmd.Transaction = transaction;
            cmd.CommandText = """
                INSERT INTO Books (Title, Author, Description, Tags, Url)
                VALUES ($title, $author, $desc, $tags, $url)
                """;
            cmd.Parameters.AddWithValue("$title",  b.title ?? "");
            cmd.Parameters.AddWithValue("$author", b.author ?? "");
            cmd.Parameters.AddWithValue("$desc",   b.description ?? "");
            cmd.Parameters.AddWithValue("$tags",   b.tags ?? "");
            cmd.Parameters.AddWithValue("$url",    b.url ?? "");
            cmd.ExecuteNonQuery();
        }

        transaction.Commit();
        Console.WriteLine($"[BookSeeder] Loaded {books.Count} books into database.");
    }

    private record SeedBook(string? title, string? author, string? description, string? tags, string? url);
}
