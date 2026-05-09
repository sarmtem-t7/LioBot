namespace LioBot.Models;

public class UserPreferences
{
    public long TelegramId { get; set; }
    public string FaithStage { get; set; } = string.Empty; // seeker | new | growing | mature
    public string Interests { get; set; } = string.Empty;  // comma-separated tags
    public string Languages { get; set; } = string.Empty;  // comma-separated language codes; empty = no filter
    public DateTime UpdatedAt { get; set; } = DateTime.UtcNow;
}
