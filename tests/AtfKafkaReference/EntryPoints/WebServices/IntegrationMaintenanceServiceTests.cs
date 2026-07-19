using System;
using System.Collections.Generic;
using Common.Logging;
using FluentAssertions;
using Microsoft.Extensions.DependencyInjection;
using NSubstitute;
using NUnit.Framework;
using Terrasoft.Core.DB;
using Terrasoft.Core.ServiceModelContract;
using Terrasoft.TestFramework;
using Terrasoft.Web.Http.Abstractions;
using AtfKafkaReference;
using AtfKafkaReference.EntryPoints.WebServices;

namespace AtfKafkaReference.Tests.EntryPoints.WebServices {

	[TestFixture(Category = "UnitTests")]
	public class IntegrationMaintenanceServiceTests : BaseComposableAppTestFixture {
		private IIntegrationRuntimeController _controller;
		private ILog _log;
		private HttpResponse _response;
		private IntegrationMaintenanceService _sut;

		[SetUp]
		protected override void SetUp() {
			base.SetUp();
			AtfKafkaReference.AtfKafkaReferenceApp.Instance.Reset();
			_controller = Substitute.For<IIntegrationRuntimeController>();
			_log = Substitute.For<ILog>();
			AtfKafkaReference.AtfKafkaReferenceApp.InjectedServices = new List<Func<IServiceCollection, IServiceCollection>> {
				services => services.AddSingleton(_controller).AddSingleton(_log)
			};
			SetCanManageSolution(true);
			HttpContext context = Substitute.For<HttpContext>();
			_response = Substitute.For<HttpResponse>();
			context.Response.Returns(_response);
			IHttpContextAccessor accessor = CustomSetupHttpContextAccessor(context, UserConnection);
			_sut = new IntegrationMaintenanceService { HttpContextAccessor = accessor };
		}

		private void SetCanManageSolution(bool canManageSolution) {
			DBSecurityEngine securityEngine = UserConnection.SetupDBSecurityEngine();
			securityEngine.GetCanExecuteOperation("CanManageSolution").Returns(canManageSolution);
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
			var exception = new InvalidOperationException("sensitive shutdown detail");
			_controller.When(controller => controller.Stop()).Do(_ => throw exception);

			// Act
			StopKafkaResponse result = _sut.Stop();

			// Assert
			result.Success.Should().BeFalse(because: "the controller rejected the shutdown");
			result.Error.Should().Be("Kafka shutdown failed. See the Creatio application log for details.",
				because: "the response must not disclose internal exception details");
			_response.StatusCode.Should().Be(500, because: "the server could not stop Kafka cleanly");
			_log.Received(1).Error("Kafka shutdown request failed.", exception);
		}

		[Test]
		[Description("Returns HTTP 403 without stopping Kafka when the user lacks the solution-management operation.")]
		public void Stop_WhenUserCannotManageSolutions_ShouldReturnForbidden() {
			// Arrange
			SetCanManageSolution(false);

			// Act
			StopKafkaResponse result = _sut.Stop();

			// Assert
			result.Success.Should().BeFalse(because: "the endpoint must fail closed for an unprivileged user");
			result.Error.Should().Be("The current user is not permitted to stop Kafka integrations.",
				because: "callers need a stable authorization error");
			_response.StatusCode.Should().Be(403, because: "CanManageSolution is required");
			_controller.DidNotReceive().Stop();
			_log.DidNotReceiveWithAnyArgs().Error(default(string), default(Exception));
		}
	}
}
