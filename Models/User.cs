namespace LioBot.Models;

public class User
{
    public long Id { get; set; }
    public long TelegramId { get; set; }
    public string? Username { get; set; }
    public string FirstName { get; set; } = string.Empty;
    public DateTime RegisteredAt { get; set; } = DateTime.UtcNow;
    public string NotifyMode { get; set; } = "daily"; // daily | weekly | off
}
