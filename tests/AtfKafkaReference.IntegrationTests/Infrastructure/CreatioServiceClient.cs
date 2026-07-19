using System;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;

namespace AtfKafkaReference.IntegrationTests.Infrastructure;

public sealed class CreatioServiceClient : IDisposable {
	private readonly CookieContainer _cookies = new CookieContainer();
	private readonly HttpClient _client;
	private readonly CreatioTestSettings _settings;

	public CreatioServiceClient(CreatioTestSettings settings) {
		_settings = settings;
		_client = new HttpClient(new HttpClientHandler {
			CookieContainer = _cookies,
			AllowAutoRedirect = false
		}) { BaseAddress = settings.Url };
	}

	public async Task AuthenticateAsync(CancellationToken cancellationToken) {
		if (_settings.UsesAccessToken) {
			_client.DefaultRequestHeaders.Authorization =
				new AuthenticationHeaderValue("Bearer", _settings.AccessToken);
			return;
		}

		string payload = JsonSerializer.Serialize(new {
			UserName = _settings.Username,
			UserPassword = _settings.Password
		});
		using HttpResponseMessage response = await _client.PostAsync(
			"ServiceModel/AuthService.svc/Login",
			new StringContent(payload, Encoding.UTF8, "application/json"),
			cancellationToken).ConfigureAwait(false);
		response.EnsureSuccessStatusCode();
		string responseBody = await response.Content.ReadAsStringAsync(cancellationToken).ConfigureAwait(false);
		using JsonDocument document = JsonDocument.Parse(responseBody);
		if (!document.RootElement.TryGetProperty("Code", out JsonElement code) || code.GetInt32() != 0) {
			throw new InvalidOperationException("Creatio authentication was rejected.");
		}
	}

	public async Task<HttpResponseMessage> PostJsonAsync(string serviceName, string methodName, object body,
			CancellationToken cancellationToken) {
		string prefix = _settings.IsNetCore ? "rest" : "0/rest";
		using var request = new HttpRequestMessage(HttpMethod.Post, $"{prefix}/{serviceName}/{methodName}") {
			Content = new StringContent(JsonSerializer.Serialize(body), Encoding.UTF8, "application/json")
		};
		CookieCollection cookies = _cookies.GetCookies(_settings.Url);
		AddCsrfHeader(request, cookies, "BPMCSRF");
		AddCsrfHeader(request, cookies, "CRT_CSRF");
		return await _client.SendAsync(request, cancellationToken).ConfigureAwait(false);
	}

	public async Task<HttpResponseMessage> PostDataServiceAsync(string json, CancellationToken cancellationToken) {
		string prefix = _settings.IsNetCore ? string.Empty : "0/";
		using var request = new HttpRequestMessage(HttpMethod.Post,
			$"{prefix}DataService/json/SyncReply/SelectQuery") {
			Content = new StringContent(json, Encoding.UTF8, "application/json")
		};
		CookieCollection cookies = _cookies.GetCookies(_settings.Url);
		AddCsrfHeader(request, cookies, "BPMCSRF");
		AddCsrfHeader(request, cookies, "CRT_CSRF");
		return await _client.SendAsync(request, cancellationToken).ConfigureAwait(false);
	}

	private static void AddCsrfHeader(HttpRequestMessage request, CookieCollection cookies, string name) {
		string value = cookies[name]?.Value;
		if (!string.IsNullOrWhiteSpace(value)) {
			request.Headers.TryAddWithoutValidation(name, value);
		}
	}

	public void Dispose() => _client.Dispose();
}
