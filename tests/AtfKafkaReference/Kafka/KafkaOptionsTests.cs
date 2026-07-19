using System;
using System.Collections.Generic;
using FluentAssertions;
using NSubstitute;
using NUnit.Framework;
using Terrasoft.Configuration.Tests;
using Terrasoft.TestFramework;
using AtfKafkaReference;
using AtfKafkaReference.Kafka;

namespace AtfKafkaReference.Tests.Kafka {

	[MockSettings(RequireMock.All)]
	[TestFixture(Category = "UnitTests")]
	public class KafkaOptionsTests : BaseComposableAppTestFixture {

		[TestCase(-1, 1)]
		[TestCase(0, 1)]
		[TestCase(8, 8)]
		[TestCase(100, 32)]
		public void NormalizeWorkerCount_ShouldKeepValueWithinSafeRange(int value, int expected) {
			KafkaOptions.NormalizeWorkerCount(value).Should().Be(expected);
		}

		private readonly Dictionary<string, object> _values = new Dictionary<string, object> {
			[Constants.Kafka.BootstrapServersSettingCode] = "broker.example:9094",
			[Constants.Kafka.UsernameSettingCode] = "reference-user",
			[Constants.Kafka.PasswordSettingCode] = "reference-password",
			[Constants.Kafka.RequestTopicSettingCode] = "reference.requests",
			[Constants.Kafka.ReplyTopicSettingCode] = "reference.replies",
			[Constants.Kafka.ConsumerGroupSettingCode] = "reference.creatio",
			[Constants.Kafka.NotificationTopicSettingCode] = "reference.notifications",
			[Constants.Kafka.NativeLibraryPathSettingCode] = "/app/runtimes/librdkafka.so",
			[Constants.Kafka.WorkerCountSettingCode] = "8"
		};

		protected override void SetupSysSettings() {
			base.SetupSysSettings();
			List<FakeSysSettings> settings = new List<FakeSysSettings>();
			foreach (string code in _values.Keys) {
				settings.Add(new FakeSysSettings { Code = code });
			}
			FakeSysSettings.Setup(settings);

			FakeSysSettingsEngine engine = Substitute.For<FakeSysSettingsEngine>();
			FakeSysSettingsEngine.Setup(engine);
			engine.TryGetSettingsValue(Arg.Any<string>(), Arg.Any<Guid>(), out Arg.Any<object>())
				.Returns(callInfo => {
					string code = callInfo.ArgAt<string>(0);
					callInfo[2] = _values[code];
					return true;
				});
		}

		[Test]
		public void FromSysSettings_ShouldReadEveryKafkaOption() {
			KafkaOptions result = KafkaOptions.FromSysSettings(UserConnection);

			result.BootstrapServers.Should().Be("broker.example:9094");
			result.Username.Should().Be("reference-user");
			result.Password.Should().Be("reference-password");
			result.RequestTopic.Should().Be("reference.requests");
			result.ReplyTopic.Should().Be("reference.replies");
			result.ConsumerGroup.Should().Be("reference.creatio");
			result.NotificationTopic.Should().Be("reference.notifications");
			result.NativeLibraryPath.Should().Be("/app/runtimes/librdkafka.so");
			result.WorkerCount.Should().Be(8);
			result.IsConfigured.Should().BeTrue();
		}
	}
}
