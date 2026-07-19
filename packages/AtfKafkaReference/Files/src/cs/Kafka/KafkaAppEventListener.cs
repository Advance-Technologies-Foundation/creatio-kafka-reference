using Terrasoft.Web.Common;

namespace AtfKafkaReference.Kafka {

	public sealed class KafkaAppEventListener : AppEventListenerBase {

		public override void OnAppStart(AppEventContext context) {
			AtfKafkaReferenceApp.Instance.GetRequiredService<IKafkaWorkerHost>().Start();
		}

		public override void OnAppEnd(AppEventContext context) {
			AtfKafkaReferenceApp.Instance.GetRequiredService<IKafkaWorkerHost>().Stop();
		}
	}
}
