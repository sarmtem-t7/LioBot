namespace LioBot.Services;

// Единый каталог языковых кодов и их Russian-display labels.
// Используется в импортёре (для разметки контента) и в UI (для меню выбора).
public static class LanguageRegistry
{
    // code → отображаемый ярлык (с эмодзи-флагом, если есть).
    public static readonly Dictionary<string, string> Labels = new()
    {
        ["ru"]               = "🇷🇺 Русский",
        ["ukr"]              = "🇺🇦 Украинский",
        ["bg"]               = "🇧🇬 Болгарский",
        ["kz"]               = "🇰🇿 Казахский",
        ["arm"]              = "🇦🇲 Армянский",
        ["eng"]              = "🇬🇧 Английский",
        ["ger"]              = "🇩🇪 Немецкий",
        ["grz"]              = "🇬🇪 Грузинский",
        ["kg"]               = "🇰🇬 Киргизский",
        ["lv"]               = "🇱🇻 Латышский",
        ["rum"]              = "🇷🇴 Румынский",
        ["uzb"]              = "🇺🇿 Узбекский",
        ["azer"]             = "🇦🇿 Азербайджанский",
        ["tajikski"]         = "🇹🇯 Таджикский",
        ["turkmenski"]       = "🇹🇲 Туркменский",
        ["abasinski"]        = "Абазинский",
        ["adygeiski"]        = "Адыгейский",
        ["altaiski"]         = "Алтайский",
        ["avarski"]          = "Аварский",
        ["balkarski"]        = "Балкарский",
        ["bashkirski"]       = "Башкирский",
        ["buryatski"]        = "Бурятский",
        ["chechenski"]       = "Чеченский",
        ["gagauzski"]        = "Гагаузский",
        ["ingushski"]        = "Ингушский",
        ["kabardinski"]      = "Кабардинский",
        ["kalmycki"]         = "Калмыцкий",
        ["krymskotatarski"]  = "Крымскотатарский",
        ["osetinski"]        = "Осетинский",
        ["tatarski"]         = "Татарский",
        ["tuvinski"]         = "Тувинский",
        ["yakutski"]         = "Якутский",
        ["zyganski"]         = "Цыганский"
    };

    // Радио-стримы: slug в URL → код языка.
    private static readonly Dictionary<string, string> RadioSlugLanguages = new()
    {
        ["abasinski"]                              = "abasinski",
        ["adygeiski"]                              = "adygeiski",
        ["altaiski"]                               = "altaiski",
        ["armyanski"]                              = "arm",
        ["avarski"]                                = "avarski",
        ["azerbaidzanski"]                         = "azer",
        ["balkarski"]                              = "balkarski",
        ["bashkirski"]                             = "bashkirski",
        ["bibliya-novy-zavet"]                     = "ru",
        ["bibliya-vetxi-zavet"]                    = "ru",
        ["buryatski"]                              = "buryatski",
        ["chechenski"]                             = "chechenski",
        ["copy-of-світло-на-сході--украинскии"]    = "ukr",
        ["eli"]                                    = "ru",
        ["gagauzski"]                              = "gagauzski",
        ["grusinski"]                              = "grz",
        ["impuls"]                                 = "ru",
        ["ingushski"]                              = "ingushski",
        ["kabardinski"]                            = "kabardinski",
        ["kalmycki"]                               = "kalmycki",
        ["kazahski"]                               = "kz",
        ["krymskotatarski"]                        = "krymskotatarski",
        ["kyrgyzski"]                              = "kg",
        ["osetinski"]                              = "osetinski",
        ["svetnavostoke"]                          = "ru",
        ["tajikski"]                               = "tajikski",
        ["tatarski"]                               = "tatarski",
        ["tropinka"]                               = "ru",
        ["turkmenski"]                             = "turkmenski",
        ["tuvinski"]                               = "tuvinski",
        ["ukrainski"]                              = "ukr",
        ["uzbekski"]                               = "uzb",
        ["yakutski"]                               = "yakutski",
        ["zyganski"]                               = "zyganski"
    };

    public static string LanguageForRadioUrl(string url)
    {
        if (string.IsNullOrWhiteSpace(url)) return "ru";
        var slug = url.TrimEnd('/').Split('/').Last();
        slug = System.Net.WebUtility.UrlDecode(slug);
        return RadioSlugLanguages.TryGetValue(slug, out var code) ? code : "ru";
    }

    public static string Label(string code) =>
        Labels.TryGetValue(code, out var l) ? l : code;
}
