using System.Text;
using System.Text.Json;
using System.Text.RegularExpressions;
using Microsoft.Extensions.Configuration;

namespace LioBot.Services;

public class ClaudeService
{
    private readonly HttpClient _http;
    private readonly string _apiKey;
    private const string Model = "llama-3.3-70b-versatile";
    private const string ApiUrl = "https://api.groq.com/openai/v1/chat/completions";

    public ClaudeService(IConfiguration configuration, IHttpClientFactory httpClientFactory)
    {
        _apiKey = configuration["GroqApiKey"]
            ?? throw new InvalidOperationException("GroqApiKey не задан в конфигурации.");
        _http = httpClientFactory.CreateClient("anthropic");
    }

    public async Task<string> AskAsync(string systemPrompt, string userMessage, int maxTokens = 1024)
    {
        // Явно требуем русский язык на уровне системного промпта
        var fullSystem = "ВАЖНО: Отвечай ТОЛЬКО на русском языке. Не используй никакие другие языки, иероглифы или символы кроме русских, латинских букв, цифр и знаков препинания.\n\n" + systemPrompt;

        var body = new
        {
            model = Model,
            max_tokens = maxTokens,
            temperature = 0.7,
            messages = new[]
            {
                new { role = "system", content = fullSystem },
                new { role = "user",   content = userMessage }
            }
        };

        var json = JsonSerializer.Serialize(body);
        var request = new HttpRequestMessage(HttpMethod.Post, ApiUrl)
        {
            Content = new StringContent(json, Encoding.UTF8, "application/json")
        };
        request.Headers.Add("Authorization", $"Bearer {_apiKey}");

        var response = await _http.SendAsync(request);
        var responseBody = await response.Content.ReadAsStringAsync();

        if (!response.IsSuccessStatusCode)
        {
            Console.WriteLine($"[Groq] Ошибка {response.StatusCode}: {responseBody}");
            return "Прости, что-то пошло не так. Попробуй снова чуть позже 🙏";
        }

        using var doc = JsonDocument.Parse(responseBody);
        var text = doc.RootElement
            .GetProperty("choices")[0]
            .GetProperty("message")
            .GetProperty("content")
            .GetString() ?? string.Empty;

        return SanitizeText(text);
    }

    // Убираем иероглифы и прочие нежелательные символы
    private static string SanitizeText(string text)
    {
        // Оставляем: кириллицу, латиницу, цифры, пунктуацию, эмодзи, пробелы
        var result = Regex.Replace(text, @"[\p{IsCJKUnifiedIdeographs}\p{IsCJKCompatibilityIdeographs}\p{IsHangulSyllables}\p{IsArabic}\p{IsThai}]+", "");
        // Убираем повторяющиеся пробелы/переносы
        result = Regex.Replace(result, @"\n{3,}", "\n\n");
        return result.Trim();
    }
}
