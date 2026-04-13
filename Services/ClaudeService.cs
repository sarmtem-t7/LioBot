using System.Text;
using System.Text.Json;
using System.Text.RegularExpressions;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;

namespace LioBot.Services;

public class ClaudeService
{
    private readonly HttpClient _http;
    private readonly string _apiKey;
    private readonly string _model;
    private readonly ILogger<ClaudeService> _logger;

    private const string ApiUrl = "https://api.anthropic.com/v1/messages";
    private const string DefaultModel = "claude-haiku-4-5";
    private const string AnthropicVersion = "2023-06-01";

    public ClaudeService(
        IConfiguration configuration,
        IHttpClientFactory httpClientFactory,
        ILogger<ClaudeService> logger)
    {
        _apiKey = configuration["AnthropicApiKey"]
            ?? throw new InvalidOperationException("AnthropicApiKey не задан в конфигурации.");
        _model = configuration["AnthropicModel"] ?? DefaultModel;
        _http = httpClientFactory.CreateClient("anthropic");
        _logger = logger;
    }

    public Task<string> AskAsync(string systemPrompt, string userMessage, int maxTokens = 1024)
        => AskWithHistoryAsync(systemPrompt, userMessage, history: null, maxTokens);

    public async Task<string> AskWithHistoryAsync(
        string systemPrompt,
        string userMessage,
        IEnumerable<(string Role, string Content)>? history,
        int maxTokens = 1024)
    {
        var fullSystem = "ВАЖНО: Отвечай ТОЛЬКО на русском языке. Не используй никакие другие языки, иероглифы или символы кроме русских, латинских букв, цифр и знаков препинания.\n\n" + systemPrompt;

        var messages = new List<object>();

        if (history != null)
        {
            foreach (var (role, content) in history)
            {
                if (string.IsNullOrWhiteSpace(content)) continue;
                var normalizedRole = role == "assistant" ? "assistant" : "user";
                messages.Add(new { role = normalizedRole, content });
            }
        }

        messages.Add(new { role = "user", content = userMessage });

        var body = new
        {
            model = _model,
            max_tokens = maxTokens,
            system = fullSystem,
            temperature = 0.85,
            messages
        };

        var json = JsonSerializer.Serialize(body);
        using var request = new HttpRequestMessage(HttpMethod.Post, ApiUrl)
        {
            Content = new StringContent(json, Encoding.UTF8, "application/json")
        };
        request.Headers.Add("x-api-key", _apiKey);
        request.Headers.Add("anthropic-version", AnthropicVersion);

        using var response = await _http.SendAsync(request);
        var responseBody = await response.Content.ReadAsStringAsync();

        if (!response.IsSuccessStatusCode)
        {
            _logger.LogError("[Claude] Ошибка {Status}: {Body}", response.StatusCode, responseBody);
            throw new HttpRequestException($"Claude API error {(int)response.StatusCode}: {responseBody}");
        }

        using var doc = JsonDocument.Parse(responseBody);
        var text = doc.RootElement
            .GetProperty("content")[0]
            .GetProperty("text")
            .GetString() ?? string.Empty;

        return SanitizeText(text);
    }

    private static string SanitizeText(string text)
    {
        var result = Regex.Replace(text, @"[\p{IsCJKUnifiedIdeographs}\p{IsCJKCompatibilityIdeographs}\p{IsHangulSyllables}\p{IsArabic}\p{IsThai}]+", "");
        result = Regex.Replace(result, @"\n{3,}", "\n\n");
        return result.Trim();
    }
}
