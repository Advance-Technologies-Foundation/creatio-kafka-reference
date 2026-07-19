using System;
using System.Net;
using System.Runtime.Serialization;
using System.ServiceModel;
using System.ServiceModel.Activation;
using System.ServiceModel.Web;
using System.Web.SessionState;
using Common.Logging;
using Terrasoft.Web.Common;

namespace AtfKafkaReference.EntryPoints.WebServices {

	[DataContract]
	public sealed class StopKafkaResponse {
		[DataMember(Name = "success")]
		public bool Success { get; set; }

		[DataMember(Name = "kafkaStopped")]
		public bool KafkaStopped { get; set; }

		[DataMember(Name = "error")]
		public string Error { get; set; }
	}

	/// <summary>
	/// Provides an explicit shutdown boundary for package-managed Kafka clients.
	/// </summary>
	[ServiceContract]
	[AspNetCompatibilityRequirements(RequirementsMode = AspNetCompatibilityRequirementsMode.Required)]
	public sealed class IntegrationMaintenanceService : BaseService, IReadOnlySessionState {
		private const string RequiredOperation = "CanManageSolution";
		private const string ForbiddenError = "The current user is not permitted to stop Kafka integrations.";
		private const string ShutdownError =
			"Kafka shutdown failed. See the Creatio application log for details.";

		[OperationContract]
		[WebInvoke(Method = "POST", RequestFormat = WebMessageFormat.Json,
			ResponseFormat = WebMessageFormat.Json, BodyStyle = WebMessageBodyStyle.Bare)]
		public StopKafkaResponse Stop() {
			try {
				if (!UserConnection.DBSecurityEngine.GetCanExecuteOperation(RequiredOperation)) {
					SetStatusCode(403);
					return new StopKafkaResponse { Success = false, Error = ForbiddenError };
				}
				IntegrationStopResult result = AtfKafkaReferenceApp.Instance
					.GetRequiredService<IIntegrationRuntimeController>().Stop();
				SetStatusCode(200);
				return new StopKafkaResponse { Success = true, KafkaStopped = result.KafkaStopped };
			} catch (Exception exception) {
				AtfKafkaReferenceApp.Instance.GetRequiredService<ILog>()
					.Error("Kafka shutdown request failed.", exception);
				SetStatusCode(500);
				return new StopKafkaResponse { Success = false, Error = ShutdownError };
			}
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
