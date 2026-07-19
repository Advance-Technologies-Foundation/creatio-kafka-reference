using System;
using System.Net;
using System.Net.Http;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Allure.NUnit;
using Allure.NUnit.Attributes;
using Confluent.Kafka;
using AtfKafkaReference.Kafka;
using FluentAssertions;
using NUnit.Framework;
using AtfKafkaReference.IntegrationTests.Infrastructure;

namespace AtfKafkaReference.IntegrationTests;

[TestFixture]
[Category("E2E")]
[AllureNUnit]
[AllureEpic("Creatio Kafka reference")]
[AllureFeature("Creatio Kafka message service")]
public sealed class KafkaMessageServiceE2ETests : CreatioIntegrationFixture {
	private KafkaTestSettings _kafkaSettings;

	[OneTimeSetUp]
	public void LoadKafkaSettings() => _kafkaSettings = KafkaTestSettings.Load();

	[Test]
	[Description("Posts text to Creatio and verifies that the correlated notification arrives through Kafka.")]
	[AllureName("Authenticated POST publishes the message to Kafka")]
	[AllureDescription("Exercises Creatio authentication, the configuration web service, real Kafka publishing, serialization, and correlated consumption.")]
	public async Task Publish_WhenRequestIsValid_ShouldDeliverCorrelatedKafkaNotification() {
		// Arrange
		string expectedMessage = $"E2E notification {Guid.NewGuid():N}";
		using IConsumer<string, string> consumer = CreateAssignedConsumer(_kafkaSettings.NotificationTopic);

		// Act
		PublishResult result = await PostMessageAsync(expectedMessage);

		// Assert
		AssertHttpStatus(result.StatusCode, HttpStatusCode.Accepted);
		AssertSuccess(result.Response.Success, true);
		AssertMessageIdIsPresent(result.Response.MessageId);
		KafkaNotification notification = ConsumeNotification(consumer, result.Response.MessageId,
			_kafkaSettings.Timeout);
		AssertNotification(notification?.MessageId ?? Guid.Empty, result.Response.MessageId,
			notification?.Message, expectedMessage);
	}

	[Test]
	[Description("Posts a blank message and verifies that Creatio rejects it without accepting the request.")]
	[AllureName("Blank message is rejected with HTTP 400")]
	[AllureDescription("Exercises the deployed request deserializer and validation/status-code mapping without publishing a valid notification.")]
	public async Task Publish_WhenMessageIsBlank_ShouldReturnBadRequest() {
		// Arrange
		string invalidMessage = CreateBlankMessage();

		// Act
		PublishResult result = await PostMessageAsync(invalidMessage);

		// Assert
		AssertHttpStatus(result.StatusCode, HttpStatusCode.BadRequest);
		AssertSuccess(result.Response.Success, false);
		AssertError(result.Response.Error, "Message is required.");
	}

	[AllureStep("Arrange — create a blank message input")]
	private static string CreateBlankMessage() => " ";

	[AllureStep("Arrange — create and assign a Kafka consumer for topic '{topic}'")]
	private IConsumer<string, string> CreateAssignedConsumer(string topic) {
		IConsumer<string, string> consumer = new ConsumerBuilder<string, string>(new ConsumerConfig {
			BootstrapServers = _kafkaSettings.BootstrapServers,
			SecurityProtocol = SecurityProtocol.SaslSsl,
			SaslMechanism = SaslMechanism.ScramSha512,
			SaslUsername = _kafkaSettings.Username,
			SaslPassword = _kafkaSettings.Password,
			SslCaCertificateStores = "Root",
			GroupId = "atf-kafka-reference.e2e." + Guid.NewGuid().ToString("N"),
			AutoOffsetReset = AutoOffsetReset.Latest,
			EnableAutoCommit = false
		}).Build();
		consumer.Subscribe(topic);
		WaitForAssignment(consumer, _kafkaSettings.Timeout);
		return consumer;
	}

	[AllureStep("Act — authenticate and POST message '{message}' to Creatio")]
	private async Task<PublishResult> PostMessageAsync(string message) {
		using var serviceClient = new CreatioServiceClient(Settings);
		await serviceClient.AuthenticateAsync(CancellationToken.None);
		using HttpResponseMessage response = await serviceClient.PostJsonAsync(
			"KafkaMessageService", "Publish", new { message }, CancellationToken.None);
		string responseBody = await response.Content.ReadAsStringAsync();
		PublishResponse result = JsonSerializer.Deserialize<PublishResponse>(responseBody,
			new JsonSerializerOptions { PropertyNameCaseInsensitive = true });
		result.Should().NotBeNull(because: "the deployed endpoint must return its concrete response DTO");
		return new PublishResult(response.StatusCode, result);
	}

	private static void WaitForAssignment(IConsumer<string, string> consumer, TimeSpan timeout) {
		DateTime deadline = DateTime.UtcNow.Add(timeout);
		while (consumer.Assignment.Count == 0 && DateTime.UtcNow < deadline) {
			consumer.Consume(TimeSpan.FromMilliseconds(250));
		}
		consumer.Assignment.Should().NotBeEmpty(because: "the consumer must join before the HTTP publish");
	}

	private static KafkaNotification ConsumeNotification(IConsumer<string, string> consumer, Guid messageId,
			TimeSpan timeout) {
		DateTime deadline = DateTime.UtcNow.Add(timeout);
		while (DateTime.UtcNow < deadline) {
			ConsumeResult<string, string> consumed = consumer.Consume(TimeSpan.FromMilliseconds(250));
			if (consumed == null) {
				continue;
			}
			KafkaNotification notification = JsonSerializer.Deserialize<KafkaNotification>(consumed.Message.Value);
			if (notification?.MessageId == messageId) {
				return notification;
			}
		}
		return null;
	}

	[AllureStep("Assert — HTTP status '{actual}' equals '{expected}'")]
	private static void AssertHttpStatus(HttpStatusCode actual, HttpStatusCode expected) =>
		actual.Should().Be(expected, because: "the deployed endpoint must map the scenario to the documented status");

	[AllureStep("Assert — response success '{actual}' equals '{expected}'")]
	private static void AssertSuccess(bool actual, bool expected) =>
		actual.Should().Be(expected, because: "the response DTO must describe whether Kafka accepted the request");

	[AllureStep("Assert — response contains message ID '{messageId}'")]
	private static void AssertMessageIdIsPresent(Guid messageId) =>
		messageId.Should().NotBeEmpty(because: "the ID correlates the HTTP response with the Kafka record");

	[AllureStep("Assert — Kafka notification ID '{actualMessageId}' and text '{actualMessage}' match expected ID '{expectedMessageId}' and text '{expectedMessage}'")]
	private static void AssertNotification(Guid actualMessageId, Guid expectedMessageId, string actualMessage,
			string expectedMessage) {
		actualMessageId.Should().Be(expectedMessageId,
			because: "the consumed record must be the one published by this HTTP request");
		actualMessage.Should().Be(expectedMessage, because: "Kafka must preserve the posted text exactly");
	}

	[AllureStep("Assert — response error '{actual}' equals '{expected}'")]
	private static void AssertError(string actual, string expected) =>
		actual.Should().Be(expected, because: "the caller needs the documented actionable validation error");

	private sealed class PublishResult {
		public PublishResult(HttpStatusCode statusCode, PublishResponse response) {
			StatusCode = statusCode;
			Response = response;
		}

		public HttpStatusCode StatusCode { get; }
		public PublishResponse Response { get; }
	}

	private sealed class PublishResponse {
		public bool Success { get; set; }
		public Guid MessageId { get; set; }
		public string Error { get; set; }
	}
}
