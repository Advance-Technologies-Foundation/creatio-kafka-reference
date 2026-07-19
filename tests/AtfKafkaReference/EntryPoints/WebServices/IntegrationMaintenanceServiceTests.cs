using System;
using System.Collections.Generic;
using FluentAssertions;
using Microsoft.Extensions.DependencyInjection;
using NSubstitute;
using NUnit.Framework;
using Terrasoft.Core.ServiceModelContract;
using Terrasoft.Web.Http.Abstractions;
using AtfKafkaReference;
using AtfKafkaReference.EntryPoints.WebServices;

namespace AtfKafkaReference.Tests.EntryPoints.WebServices {

	[TestFixture(Category = "UnitTests")]
	public class IntegrationMaintenanceServiceTests : BaseComposableAppTestFixture {
		private IIntegrationRuntimeController _controller;
		private HttpResponse _response;
		private IntegrationMaintenanceService _sut;

		[SetUp]
		protected override void SetUp() {
			base.SetUp();
			AtfKafkaReference.AtfKafkaReferenceApp.Instance.Reset();
			_controller = Substitute.For<IIntegrationRuntimeController>();
			AtfKafkaReference.AtfKafkaReferenceApp.InjectedServices = new List<Func<IServiceCollection, IServiceCollection>> {
				services => services.AddSingleton(_controller)
			};
			HttpContext context = Substitute.For<HttpContext>();
			_response = Substitute.For<HttpResponse>();
			context.Response.Returns(_response);
			IHttpContextAccessor accessor = CustomSetupHttpContextAccessor(context, UserConnection);
			_sut = new IntegrationMaintenanceService { HttpContextAccessor = accessor };
		}

		[Test]
		[Description("Returns HTTP 200 when the package Kafka runtime stops successfully.")]
		public void Stop_WhenIntegrationsStop_ShouldReturnOk() {
			// Arrange
			_controller.Stop().Returns(new IntegrationStopResult { KafkaStopped = true });

			// Act
			StopKafkaResponse result = _sut.Stop();

			// Assert
			result.Success.Should().BeTrue(because: "the runtime controller completed without an error");
			result.KafkaStopped.Should().BeTrue(because: "the response exposes the completed shutdown");
			_response.StatusCode.Should().Be(200, because: "the shutdown completed successfully");
		}

		[Test]
		[Description("Returns HTTP 500 and an actionable error when Kafka shutdown fails.")]
		public void Stop_WhenShutdownFails_ShouldReturnInternalServerError() {
			// Arrange
			_controller.When(controller => controller.Stop()).Do(_ => throw new InvalidOperationException("shutdown failed"));

			// Act
			StopKafkaResponse result = _sut.Stop();

			// Assert
			result.Success.Should().BeFalse(because: "the controller rejected the shutdown");
			result.Error.Should().Be("shutdown failed", because: "the operator needs the root failure");
			_response.StatusCode.Should().Be(500, because: "the server could not stop Kafka cleanly");
		}
	}
}
