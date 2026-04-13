using System.Net.Http.Headers;
using System.Text;
using System.Text.Json;
using System.Text.RegularExpressions;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;

namespace LioBot.Services;

// Groq (OpenAI-compatible) LLM client. Keeps the ClaudeService class name
// to avoid churn in callers; provider is controlled by config.
public class ClaudeService
{
    private readonly HttpClient _http;
    private readonly string _apiKey;
    private readonly string _model;
    private readonly ILogger<ClaudeService> _logger;

    private const string ApiUrl = "https://api.groq.com/openai/v1/chat/completions";
    private const string DefaultModel = "llama-3.3-70b-versatile";

    public ClaudeService(
        IConfiguration configuration,
        IHttpClientFactory httpClientFactory,
        ILogger<ClaudeService> logger)
    {
        _apiKey = configuration["GroqApiKey"]
            ?? configuration["AnthropicApiKey"]
            ?? throw new InvalidOperationException("GroqApiKey не задан в конфигурации.");
        _model = configuration["GroqModel"] ?? DefaultModel;
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

        var messages = new List<object> { new { role = "system", content = fullSystem } };

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
            temperature = 0.85,
            messages
        };

        var json = JsonSerializer.Serialize(body);

        // Ретраим транзиентные сбои: сетевые ошибки, таймауты, 429, 5xx.
        const int maxAttempts = 4;
        Exception? lastError = null;

        for (var attempt = 1; attempt <= maxAttempts; attempt++)
        {
            using var request = new HttpRequestMessage(HttpMethod.Post, ApiUrl)
            {
                Content = new StringContent(json, Encoding.UTF8, "application/json")
            };
            request.Headers.Authorization = new AuthenticationHeaderValue("Bearer", _apiKey);

            try
            {
                using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));
                using var response = await _http.SendAsync(request, cts.Token);
                var responseBody = await response.Content.ReadAsStringAsync(cts.Token);

                if (response.IsSuccessStatusCode)
                {
                    using var doc = JsonDocument.Parse(responseBody);
                    var text = doc.RootElement
                        .GetProperty("choices")[0]
                        .GetProperty("message")
                        .GetProperty("content")
                        .GetString() ?? string.Empty;
                    return SanitizeText(text);
                }

                var status = (int)response.StatusCode;
                var transient = status == 429 || status >= 500;
                _logger.LogWarning("[Groq] {Status} (попытка {Attempt}/{Max}): {Body}",
                    status, attempt, maxAttempts, Truncate(responseBody, 300));

                if (!transient || attempt == maxAttempts)
                    throw new HttpRequestException($"Groq API error {status}: {responseBody}");

                lastError = new HttpRequestException($"Groq API error {status}");
            }
            catch (Exception ex) when (ex is HttpRequestException or TaskCanceledException or IOException)
            {
                lastError = ex;
                _logger.LogWarning(ex, "[Groq] Сетевой сбой (попытка {Attempt}/{Max})", attempt, maxAttempts);
                if (attempt == maxAttempts) throw;
            }

            // Экспоненциальный бэкофф с джиттером: ~0.7s, 1.5s, 3s
            var delayMs = (int)(700 * Math.Pow(2, attempt - 1) + Random.Shared.Next(0, 300));
            await Task.Delay(delayMs);
        }

        throw lastError ?? new HttpRequestException("Groq API: unknown failure");
    }

    private static string Truncate(string s, int n) =>
        string.IsNullOrEmpty(s) || s.Length <= n ? s : s[..n] + "…";

    private static string SanitizeText(string text)
    {
        var result = Regex.Replace(text, @"[\p{IsCJKUnifiedIdeographs}\p{IsCJKCompatibilityIdeographs}\p{IsHangulSyllables}\p{IsArabic}\p{IsThai}]+", "");
        result = Regex.Replace(result, @"\n{3,}", "\n\n");
        return result.Trim();
    }
}
