using System;
using Terrasoft.Core;
using Terrasoft.Core.Configuration;

namespace AtfKafkaReference.Kafka {

	internal sealed class KafkaOptions {

		internal string BootstrapServers { get; set; }
		internal string Username { get; set; }
		internal string Password { get; set; }
		internal string RequestTopic { get; set; }
		internal string ReplyTopic { get; set; }
		internal string ConsumerGroup { get; set; }
		internal string NotificationTopic { get; set; }
		internal string NativeLibraryPath { get; set; }
		internal int WorkerCount { get; set; } = Constants.Kafka.DefaultWorkerCount;

		internal bool IsConfigured => !string.IsNullOrWhiteSpace(Password);

		internal static KafkaOptions FromSysSettings(UserConnection userConnection) {
			return new KafkaOptions {
				BootstrapServers = SysSettings.GetValue(userConnection, Constants.Kafka.BootstrapServersSettingCode,
					Constants.Kafka.DefaultBootstrapServers),
				Username = SysSettings.GetValue(userConnection, Constants.Kafka.UsernameSettingCode,
					Constants.Kafka.DefaultUsername),
				Password = SysSettings.GetValue(userConnection, Constants.Kafka.PasswordSettingCode, string.Empty),
				RequestTopic = SysSettings.GetValue(userConnection, Constants.Kafka.RequestTopicSettingCode,
					Constants.Kafka.DefaultRequestTopic),
				ReplyTopic = SysSettings.GetValue(userConnection, Constants.Kafka.ReplyTopicSettingCode,
					Constants.Kafka.DefaultReplyTopic),
				ConsumerGroup = SysSettings.GetValue(userConnection, Constants.Kafka.ConsumerGroupSettingCode,
					Constants.Kafka.DefaultConsumerGroup),
				NotificationTopic = SysSettings.GetValue(userConnection,
					Constants.Kafka.NotificationTopicSettingCode, Constants.Kafka.DefaultNotificationTopic),
				NativeLibraryPath = SysSettings.GetValue(userConnection,
					Constants.Kafka.NativeLibraryPathSettingCode, string.Empty),
				WorkerCount = ParseWorkerCount(SysSettings.GetValue(userConnection,
					Constants.Kafka.WorkerCountSettingCode, Constants.Kafka.DefaultWorkerCount.ToString()))
			};
		}

		internal static int NormalizeWorkerCount(int workerCount) {
			return Math.Max(1, Math.Min(Constants.Kafka.MaximumWorkerCount, workerCount));
		}

		internal static int ParseWorkerCount(string workerCount) {
			return int.TryParse(workerCount, out int value)
				? NormalizeWorkerCount(value)
				: Constants.Kafka.DefaultWorkerCount;
		}
	}
}
