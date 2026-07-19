using System.Diagnostics.CodeAnalysis;
using Confluent.Kafka;

namespace AtfKafkaReference.Kafka {

	internal interface IKafkaClientFactory {
		IConsumer<string, string> CreateConsumer(ConsumerConfig config, string nativeLibraryPath);
		IProducer<string, string> CreateProducer(ProducerConfig config, string nativeLibraryPath);
	}

	// Confluent client construction and native loading are exercised by the deployed Kafka E2E suite.
	[ExcludeFromCodeCoverage]
	internal sealed class KafkaClientFactory : IKafkaClientFactory {
		public IConsumer<string, string> CreateConsumer(ConsumerConfig config, string nativeLibraryPath) {
			KafkaNativeLibraryLoader.Load(nativeLibraryPath);
			return new ConsumerBuilder<string, string>(config).Build();
		}

		public IProducer<string, string> CreateProducer(ProducerConfig config, string nativeLibraryPath) {
			KafkaNativeLibraryLoader.Load(nativeLibraryPath);
			return new ProducerBuilder<string, string>(config).Build();
		}
	}
}
