namespace AtfKafkaReference {

	internal static class Constants {
		internal const string LoggerName = "AtfKafkaReference";
		internal const string PackageName = "AtfKafkaReference";

		internal static class Kafka {
			internal const string NativeRuntimeVersion = "2.6.1";
			internal const string NativeRuntimeArtifactFileName = "kafka-2.6.1-runtimes.zip";
			internal const string NativeArtifactsDirectoryName = "native-artifacts";
			internal const string ExternalNativeDirectoryName = "native";
			internal const string RuntimeProductName = "kafka";
			internal const string BootstrapServersSettingCode = "KafkaBootstrapServers";
			internal const string UsernameSettingCode = "KafkaUsername";
			internal const string PasswordSettingCode = "KafkaPassword";
			internal const string RequestTopicSettingCode = "KafkaRequestTopic";
			internal const string ReplyTopicSettingCode = "KafkaReplyTopic";
			internal const string ConsumerGroupSettingCode = "KafkaConsumerGroup";
			internal const string NotificationTopicSettingCode = "KafkaNotificationTopic";
			internal const string NativeLibraryPathSettingCode = "KafkaNativeLibraryPath";
			internal const string WorkerCountSettingCode = "KafkaWorkerCount";

			internal const string DefaultBootstrapServers = "kafka.example.com:9094";
			internal const string DefaultUsername = "kafka-user";
			internal const string DefaultRequestTopic = "atf-kafka-reference.requests";
			internal const string DefaultReplyTopic = "atf-kafka-reference.replies";
			internal const string DefaultConsumerGroup = "atf-kafka-reference.creatio";
			internal const string DefaultNotificationTopic = "atf-kafka-reference.notifications";
			internal const int DefaultWorkerCount = 1;
			internal const int MaximumWorkerCount = 32;
		}
	}
}
