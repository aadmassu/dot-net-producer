using System.Net.Http.Headers;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using System.Text.Json;
using Confluent.Kafka;
using Microsoft.Extensions.Configuration;

namespace DotNetProducer;

internal static class Program
{
    private static async Task<int> Main()
    {
        Console.WriteLine("Starting Kafka producer test...");

        var config = new ConfigurationBuilder()
            .SetBasePath(AppContext.BaseDirectory)
            .AddJsonFile("appsettings.json", optional: false, reloadOnChange: false)
            .AddEnvironmentVariables()
            .Build();

        var settings = AppSettings.Load(config);

        try
        {
            await ProduceTestMessageAsync(settings);
            return 0;
        }
        catch (Exception ex)
        {
            Console.Error.WriteLine($"Producer failed: {ex.Message}");
            return 1;
        }
    }

    private static async Task ProduceTestMessageAsync(AppSettings settings)
    {
        var producerConfig = new ProducerConfig
        {
            BootstrapServers = settings.BootstrapServers,
            SecurityProtocol = SecurityProtocol.SaslSsl,
            SaslMechanism = SaslMechanism.OAuthBearer,
            ClientId = settings.ClientId,
        };

        if (!string.IsNullOrWhiteSpace(settings.CaCertPath))
        {
            producerConfig.SslCaLocation = settings.CaCertPath;
        }

        using var producer = new ProducerBuilder<string, string>(producerConfig)
            .SetOAuthBearerTokenRefreshHandler((client, _) => RefreshOAuthToken(client, settings))
            .SetErrorHandler((_, e) => Console.Error.WriteLine($"Kafka error: {e.Reason}"))
            .Build();

        var payload = new
        {
            message = "Hello ayu from Okta OAuth",
            timestamp = DateTimeOffset.UtcNow.ToUnixTimeSeconds(),
        };

        var json = JsonSerializer.Serialize(payload);

        var deliveryResult = await producer.ProduceAsync(
            settings.TopicName,
            new Message<string, string>
            {
                Key = "test-key",
                Value = json,
            });

        producer.Flush(TimeSpan.FromSeconds(10));

        Console.WriteLine(
            $" Message successfully produced to {deliveryResult.TopicPartitionOffset}");
    }

    private static void RefreshOAuthToken(IClient client, AppSettings settings)
    {
        try
        {
            var (token, expiryMs, principal) = FetchTokenAsync(settings).GetAwaiter().GetResult();
            client.OAuthBearerSetToken(token, expiryMs, principal);
        }
        catch (Exception ex)
        {
            client.OAuthBearerSetTokenFailure(ex.ToString());
        }
    }

    private static async Task<(string Token, long ExpiryMs, string Principal)> FetchTokenAsync(
        AppSettings settings)
    {
        using var handler = new HttpClientHandler();

        if (!string.IsNullOrWhiteSpace(settings.CaCertPath) && File.Exists(settings.CaCertPath))
        {
            var caCert = X509CertificateLoader.LoadCertificateFromFile(settings.CaCertPath);
            handler.ServerCertificateCustomValidationCallback = (_, serverCert, chain, errors) =>
                ValidateWithCustomCa(serverCert, chain, errors, caCert);
        }

        using var http = new HttpClient(handler) { Timeout = TimeSpan.FromSeconds(10) };

        var basic = Convert.ToBase64String(
            Encoding.ASCII.GetBytes($"{settings.ClientId}:{settings.ClientSecret}"));
        http.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Basic", basic);

        var form = new FormUrlEncodedContent(new[]
        {
            new KeyValuePair<string, string>("grant_type", "client_credentials"),
            new KeyValuePair<string, string>("scope", settings.Scope),
        });

        using var response = await http.PostAsync(settings.TokenUrl, form);
        response.EnsureSuccessStatusCode();

        await using var stream = await response.Content.ReadAsStreamAsync();
        using var doc = await JsonDocument.ParseAsync(stream);

        var accessToken = doc.RootElement.GetProperty("access_token").GetString()
            ?? throw new InvalidOperationException("access_token missing from token response");

        var expiresIn = doc.RootElement.TryGetProperty("expires_in", out var expEl)
            ? expEl.GetInt64()
            : 3600L;

        var expiryMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() + (expiresIn * 1000);

        return (accessToken, expiryMs, settings.ClientId);
    }

    private static bool ValidateWithCustomCa(
        X509Certificate2? serverCert,
        X509Chain? chain,
        System.Net.Security.SslPolicyErrors errors,
        X509Certificate2 caCert)
    {
        if (serverCert is null || chain is null)
        {
            return false;
        }

        chain.ChainPolicy.RevocationMode = X509RevocationMode.NoCheck;
        chain.ChainPolicy.TrustMode = X509ChainTrustMode.CustomRootTrust;
        chain.ChainPolicy.CustomTrustStore.Clear();
        chain.ChainPolicy.CustomTrustStore.Add(caCert);
        chain.ChainPolicy.ExtraStore.Add(caCert);

        return chain.Build(serverCert);
    }
}

internal sealed record AppSettings(
    string BootstrapServers,
    string TopicName,
    string CaCertPath,
    string TokenUrl,
    string ClientId,
    string ClientSecret,
    string Scope)
{
    public static AppSettings Load(IConfiguration config)
    {
        var kafka = config.GetSection("Kafka");
        var okta = config.GetSection("Okta");

        return new AppSettings(
            BootstrapServers: kafka["BootstrapServers"] ?? string.Empty,
            TopicName: kafka["TopicName"] ?? "walter.topic",
            CaCertPath: kafka["CaCertPath"] ?? string.Empty,
            TokenUrl: okta["TokenUrl"] ?? throw new InvalidOperationException("Okta:TokenUrl is required"),
            ClientId: okta["ClientId"] ?? throw new InvalidOperationException("Okta:ClientId is required"),
            ClientSecret: okta["ClientSecret"] ?? throw new InvalidOperationException("Okta:ClientSecret is required"),
            Scope: okta["Scope"] ?? string.Empty);
    }
}
