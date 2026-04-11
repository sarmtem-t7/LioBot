using System.Reflection;
using System.Text.Json;

namespace LioBot.Data;

public static class BookSeeder
{
    public static void SeedIfEmpty(DatabaseContext db)
    {
        var assembly = Assembly.GetExecutingAssembly();

        // Seed regular books if none exist
        if (db.GetCountByType("book") == 0)
            SeedFromResource(db, assembly, "LioBot.Data.books_seed.json", "book");

        // Seed audio books if none exist
        if (db.GetCountByType("audio") == 0)
            SeedFromResource(db, assembly, "LioBot.Data.audiobooks_seed.json", "audio");
    }

    private static void SeedFromResource(DatabaseContext db, Assembly assembly, string resourceName, string type)
    {
        using var stream = assembly.GetManifestResourceStream(resourceName);
        if (stream == null)
        {
            Console.WriteLine($"[BookSeeder] Resource not found: {resourceName}");
            return;
        }

        var books = JsonSerializer.Deserialize<List<SeedBook>>(stream) ?? [];

        using var conn = db.CreateConnection();
        conn.Open();
        using var transaction = conn.BeginTransaction();

        foreach (var b in books)
        {
            var cmd = conn.CreateCommand();
            cmd.Transaction = transaction;
            cmd.CommandText = """
                INSERT INTO Books (Title, Author, Description, Tags, Url, Type)
                VALUES ($title, $author, $desc, $tags, $url, $type)
                """;
            cmd.Parameters.AddWithValue("$title",  b.title ?? "");
            cmd.Parameters.AddWithValue("$author", b.author ?? "");
            cmd.Parameters.AddWithValue("$desc",   b.description ?? "");
            cmd.Parameters.AddWithValue("$tags",   b.tags ?? "");
            cmd.Parameters.AddWithValue("$url",    b.url ?? "");
            cmd.Parameters.AddWithValue("$type",   b.type ?? type);
            cmd.ExecuteNonQuery();
        }

        transaction.Commit();
        Console.WriteLine($"[BookSeeder] Loaded {books.Count} {type}s.");
    }

    private record SeedBook(string? title, string? author, string? description, string? tags, string? url, string? type);
}
