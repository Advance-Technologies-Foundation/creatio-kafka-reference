using System;
using System.Diagnostics.CodeAnalysis;
using AtfKafkaReference.Kafka;
using Terrasoft.Core;
using Terrasoft.Core.DB;

namespace AtfKafkaReference.Kafka {

	internal interface IContactNameProvider {
		string GetCurrentContactName();
	}

	// Creatio database access is covered by the deployed console-to-Creatio round-trip scenario.
	[ExcludeFromCodeCoverage]
	internal sealed class CreatioContactNameProvider : IContactNameProvider {

		private readonly Func<UserConnection> _userConnectionAccessor;

		public CreatioContactNameProvider(Func<UserConnection> userConnectionAccessor) {
			_userConnectionAccessor = userConnectionAccessor;
		}

		public string GetCurrentContactName() {
			UserConnection userConnection = _userConnectionAccessor();
			Select select = new Select(userConnection)
				.Column("Name")
				.From("Contact");
			select.Where("Id").IsEqual(Column.Parameter(userConnection.CurrentUser.ContactId));
			return select.ExecuteScalar<string>();
		}
	}

	internal interface IKafkaMessageHandler {
		KafkaReply Handle(KafkaRequest request);
	}

	internal sealed class KafkaMessageHandler : IKafkaMessageHandler {

		private readonly IContactNameProvider _contactNameProvider;

		public KafkaMessageHandler(IContactNameProvider contactNameProvider) {
			_contactNameProvider = contactNameProvider;
		}

		public KafkaReply Handle(KafkaRequest request) {
			if (request == null || request.CorrelationId == Guid.Empty || string.IsNullOrWhiteSpace(request.Message)) {
				return null;
			}

			string contactName = _contactNameProvider.GetCurrentContactName();
			return new KafkaReply {
				CorrelationId = request.CorrelationId,
				ContactName = contactName,
				Message = $"I see your message {request.Message} by {contactName}",
				SentAtUtc = DateTime.UtcNow
			};
		}
	}
}
