# dot-net-producer

A .NET 9 console app that produces a JSON message to a Kafka topic using
**SASL_SSL + OAUTHBEARER**, with the token fetched from **Okta** via
`client_credentials`. Port of the equivalent Python `confluent-kafka` script.

## Requirements

- .NET 9 SDK (`dotnet --list-sdks` should show `9.0.x`)
- Network access to your Kafka brokers and Okta token endpoint
- Your private CA bundle as `ca.pem` (only needed if your Kafka cluster or
  Okta endpoint uses a private/internal CA)

## Configuration

Edit `appsettings.json`:

```json
{
  "Kafka": {
    "BootstrapServers": "broker1:9092,broker2:9092",
    "TopicName": "walter.topic",
    "CaCertPath": "ca.pem"
  },
  "Okta": {
    "TokenUrl": "https://<your-okta-domain>/oauth2/<id>/v1/token",
    "ClientId": "<client-id>",
    "ClientSecret": "<client-secret>",
    "Scope": "11547.DAH(Dev)"
  }
}
```

Any value can also be overridden via environment variable using the
double-underscore convention:

```bash
export Okta__ClientSecret='…'
export Kafka__BootstrapServers='broker1:9092,broker2:9092'
```

Set `Kafka:CaCertPath` to an empty string if you don't need a custom CA — the
app will then use the OS trust store.

## Run

```bash
dotnet restore
dotnet run
```

Expected output:

```
Starting Kafka producer test...
 Message successfully produced to walter.topic [0] @ <offset>
```

## How the OAuth flow works

1. `ProducerBuilder.SetOAuthBearerTokenRefreshHandler` is invoked by
   librdkafka whenever a token is needed (initial connect and on refresh).
2. The handler `POST`s to `Okta:TokenUrl` with HTTP Basic auth
   (`ClientId`:`ClientSecret`) and `grant_type=client_credentials`.
3. The returned `access_token` plus `expires_in` is handed back via
   `client.OAuthBearerSetToken(token, expiryMs, principal)`. Note that
   librdkafka expects **epoch milliseconds** for the expiry (the Python
   client uses seconds — that's the one easy gotcha when porting).
4. If the token fetch fails, `OAuthBearerSetTokenFailure` is called so
   librdkafka surfaces the error instead of hanging.

## Custom CA handling

`HttpClientHandler.ServerCertificateCustomValidationCallback` is wired to
trust **only** the cert in `ca.pem` for the Okta call
(`X509ChainTrustMode.CustomRootTrust`). For Kafka itself, librdkafka uses
`SslCaLocation` (the same `ssl.ca.location` setting as the Python client).

## Project layout

```
DotNetProducer.csproj   # net9.0, references Confluent.Kafka
Program.cs              # entry point + OAuth + producer logic
appsettings.json        # Kafka + Okta config (gitignored secrets via env)
global.json             # pins SDK to 9.0.x
ca.pem                  # your private CA bundle (gitignored)
```

## Troubleshooting

- **`SDK 9.0.x was not found`** — install the .NET 9 SDK or remove
  `global.json` to use whatever SDK is on your machine.
- **`SASL OAUTHBEARER: client authentication failed`** — check the scope,
  client id/secret, and that the principal name passed to
  `OAuthBearerSetToken` matches what the broker expects (defaults to the
  client id here).
- **TLS errors on the token endpoint** — confirm `ca.pem` actually contains
  the issuing CA for your Okta hostname; the validator is strict and will
  not fall back to the system store.
