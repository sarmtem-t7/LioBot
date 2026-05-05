namespace LioBot.Models;

public class Book
{
    public long Id { get; set; }
    public string Title { get; set; } = string.Empty;
    public string Author { get; set; } = string.Empty;
    public string Description { get; set; } = string.Empty;
    public string Tags { get; set; } = string.Empty;
    public string Url { get; set; } = string.Empty;
    // book | audio | article | magazine | radio
    public string Type { get; set; } = "book";
    public string AudioUrl { get; set; } = string.Empty;
    public long? IssueId { get; set; }
    public DateTime? ReleasedAt { get; set; }
    public string CoverUrl { get; set; } = string.Empty;
}
