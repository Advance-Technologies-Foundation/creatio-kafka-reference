using System;
using AtfKafkaReference.Kafka;

namespace AtfKafkaReference {

	internal interface IIntegrationRuntimeController {
		IntegrationStopResult Stop();
	}

	internal sealed class IntegrationStopResult {
		internal bool KafkaStopped { get; set; }
	}

	internal sealed class IntegrationRuntimeController : IIntegrationRuntimeController {
		private readonly IKafkaWorkerHost _workerHost;
		private readonly IKafkaNotificationPublisher _publisher;

		public IntegrationRuntimeController(IKafkaWorkerHost workerHost, IKafkaNotificationPublisher publisher) {
			_workerHost = workerHost;
			_publisher = publisher;
		}

		public IntegrationStopResult Stop() {
			_workerHost.Stop();
			(_publisher as IDisposable)?.Dispose();
			return new IntegrationStopResult { KafkaStopped = true };
		}
	}
}
