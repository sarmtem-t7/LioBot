namespace LioBot.Models;

public enum BookProgress
{
    Reading,
    Read,
    Dropped
}

public class UserBookStatus
{
    public long TelegramId { get; set; }
    public long BookId { get; set; }
    public BookProgress Progress { get; set; }
    public int? Rating { get; set; } // 1..5 when Progress == Read
    public DateTime UpdatedAt { get; set; } = DateTime.UtcNow;
}
