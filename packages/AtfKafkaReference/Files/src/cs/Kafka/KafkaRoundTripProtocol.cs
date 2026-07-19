using System;
using System.Text.Json.Serialization;

namespace AtfKafkaReference.Kafka {

	public sealed class KafkaRequest {

		[JsonPropertyName("correlationId")]
		public Guid CorrelationId { get; set; }

		[JsonPropertyName("message")]
		public string Message { get; set; } = string.Empty;

		[JsonPropertyName("sentAtUtc")]
		public DateTime SentAtUtc { get; set; }
	}

	public sealed class KafkaReply {

		[JsonPropertyName("correlationId")]
		public Guid CorrelationId { get; set; }

		[JsonPropertyName("message")]
		public string Message { get; set; } = string.Empty;

		[JsonPropertyName("contactName")]
		public string ContactName { get; set; } = string.Empty;

		[JsonPropertyName("sentAtUtc")]
		public DateTime SentAtUtc { get; set; }
	}

	public sealed class KafkaNotification {

		[JsonPropertyName("messageId")]
		public Guid MessageId { get; set; }

		[JsonPropertyName("message")]
		public string Message { get; set; } = string.Empty;

		[JsonPropertyName("sentAtUtc")]
		public DateTime SentAtUtc { get; set; }
	}
}
