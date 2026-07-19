using System;
using FluentAssertions;
using NSubstitute;
using NUnit.Framework;
using AtfKafkaReference;
using AtfKafkaReference.Kafka;

namespace AtfKafkaReference.Tests {

	[TestFixture]
	public class IntegrationRuntimeControllerTests {

		[Test]
		[Description("Stops Kafka workers and disposes the producer before reporting success.")]
		public void Stop_ShouldStopKafkaAndReportKafkaStopped() {
			// Arrange
			IKafkaWorkerHost host = Substitute.For<IKafkaWorkerHost>();
			IKafkaNotificationPublisher publisher = Substitute.For<IKafkaNotificationPublisher, IDisposable>();
			var sut = new IntegrationRuntimeController(host, publisher);

			// Act
			IntegrationStopResult result = sut.Stop();

			// Assert
			host.Received(1).Stop();
			((IDisposable)publisher).Received(1).Dispose();
			result.KafkaStopped.Should().BeTrue(because: "the Kafka runtime completed its shutdown boundary");
		}
	}
}
