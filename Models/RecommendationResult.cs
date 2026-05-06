namespace LioBot.Models;

public record RecommendationResult(string Text, List<Book> Books, string? Comment = null);
