using System;
using System.Collections.Generic;
using FluentAssertions;
using Microsoft.Extensions.DependencyInjection;
using NSubstitute;
using NUnit.Framework;
using AtfKafkaReference.Kafka;

namespace AtfKafkaReference.Tests.Kafka {

	[TestFixture(Category = "UnitTests")]
	public class KafkaAppEventListenerTests : BaseComposableAppTestFixture {
		private IKafkaWorkerHost _host;

		[SetUp]
		protected override void SetUp() {
			base.SetUp();
			AtfKafkaReference.AtfKafkaReferenceApp.Instance.Reset();
			_host = Substitute.For<IKafkaWorkerHost>();
			AtfKafkaReference.AtfKafkaReferenceApp.InjectedServices = new List<Func<IServiceCollection, IServiceCollection>> {
				services => services.AddSingleton(_host)
			};
		}

		[Test]
		[Description("Delegates application start and end events to the singleton Kafka worker host.")]
		public void LifecycleEvents_ShouldStartAndStopWorkerHost() {
			// Arrange
			var sut = new KafkaAppEventListener();

			// Act
			sut.OnAppStart(null);
			sut.OnAppEnd(null);

			// Assert
			_host.Received(1).Start();
			_host.Received(1).Stop();
			AtfKafkaReference.AtfKafkaReferenceApp.Instance.GetRequiredService<IKafkaWorkerHost>().Should().BeSameAs(_host,
				because: "the listener must resolve the injected singleton host");
		}
	}
}
