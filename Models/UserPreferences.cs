namespace LioBot.Models;

public class UserPreferences
{
    public long TelegramId { get; set; }
    public string FaithStage { get; set; } = string.Empty; // seeker | new | growing | mature
    public string Interests { get; set; } = string.Empty;  // comma-separated tags
    public DateTime UpdatedAt { get; set; } = DateTime.UtcNow;
}
