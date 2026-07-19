using System;
using System.Net;
using System.Runtime.Serialization;
using System.ServiceModel;
using System.ServiceModel.Activation;
using System.ServiceModel.Web;
using System.Threading;
using System.Web.SessionState;
using ErrorOr;
using Terrasoft.Web.Common;
using AtfKafkaReference.Kafka;

namespace AtfKafkaReference.EntryPoints.WebServices {

	[DataContract]
	public sealed class PublishKafkaMessageRequest {
		[DataMember(Name = "message")]
		public string Message { get; set; }
	}

	[DataContract]
	public sealed class PublishKafkaMessageResponse {
		[DataMember(Name = "success")]
		public bool Success { get; set; }

		[DataMember(Name = "messageId")]
		public Guid MessageId { get; set; }

		[DataMember(Name = "error")]
		public string Error { get; set; }
	}

	[ServiceContract]
	[AspNetCompatibilityRequirements(RequirementsMode = AspNetCompatibilityRequirementsMode.Required)]
	public sealed class KafkaMessageService : BaseService, IReadOnlySessionState {

		[OperationContract]
		[WebInvoke(Method = "POST", RequestFormat = WebMessageFormat.Json,
			ResponseFormat = WebMessageFormat.Json, BodyStyle = WebMessageBodyStyle.Bare)]
		public PublishKafkaMessageResponse Publish(PublishKafkaMessageRequest request) {
			if (string.IsNullOrWhiteSpace(request?.Message)) {
				SetStatusCode(400);
				return new PublishKafkaMessageResponse { Success = false, Error = "Message is required." };
			}

			ErrorOr<KafkaNotification> result = AtfKafkaReferenceApp.Instance
				.GetRequiredService<IKafkaNotificationPublisher>()
				.PublishAsync(request.Message, CancellationToken.None)
				.ConfigureAwait(false)
				.GetAwaiter()
				.GetResult();
			if (result.IsError) {
				SetStatusCode(502);
				return new PublishKafkaMessageResponse {
					Success = false,
					Error = result.FirstError.Description
				};
			}

			SetStatusCode(202);
			return new PublishKafkaMessageResponse {
				Success = true,
				MessageId = result.Value.MessageId
			};
		}

		private void SetStatusCode(int statusCode) {
#if NETSTANDARD2_0
			HttpContextAccessor.GetInstance().Response.StatusCode = statusCode;
#else
			WebOperationContext.Current.OutgoingResponse.StatusCode = (HttpStatusCode)statusCode;
#endif
		}
	}
}
