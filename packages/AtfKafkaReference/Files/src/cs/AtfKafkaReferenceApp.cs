using System;
using System.Collections.Generic;
using System.Linq;
using Common.Logging;
using Microsoft.Extensions.DependencyInjection;
using Terrasoft.Core;
using Terrasoft.Core.Factories;
using AtfKafkaReference.Kafka;

namespace AtfKafkaReference {

	/// <summary>
	/// Provides access to the package-owned application services.
	/// </summary>
	public sealed class AtfKafkaReferenceApp {
		private static Lazy<AtfKafkaReferenceApp> _instance =
			new Lazy<AtfKafkaReferenceApp>(() => new AtfKafkaReferenceApp());
		private readonly Lazy<ServiceProvider> _serviceProvider = new Lazy<ServiceProvider>(Initialize);

		internal static UserConnection UserConnection => ClassFactory.Get<UserConnection>();
		internal static IEnumerable<Func<IServiceCollection, IServiceCollection>> InjectedServices;

		/// <summary>
		/// Gets the singleton package application.
		/// </summary>
		public static AtfKafkaReferenceApp Instance => _instance.Value;

		private static ServiceProvider Initialize() {
			var services = new ServiceCollection();
			services.AddSingleton<ILog>(LogManager.GetLogger(Constants.LoggerName));
			// Creatio owns UserConnection. The package exposes an accessor so DI never disposes it.
			services.AddTransient<Func<UserConnection>>(provider => () => UserConnection);
			services.AddSingleton<KafkaOptions>(provider =>
				KafkaOptions.FromSysSettings(provider.GetRequiredService<Func<UserConnection>>()()));
			services.AddSingleton<IContactNameProvider, CreatioContactNameProvider>();
			services.AddSingleton<IKafkaMessageHandler, KafkaMessageHandler>();
			services.AddSingleton<IKafkaClientFactory, KafkaClientFactory>();
			services.AddSingleton<IKafkaWorker, KafkaWorker>();
			services.AddSingleton<IKafkaWorkerHost, KafkaWorkerHost>();
			services.AddSingleton<IKafkaNotificationPublisher, KafkaNotificationPublisher>();
			services.AddSingleton<IIntegrationRuntimeController, IntegrationRuntimeController>();

			InjectedServices?.ToList().ForEach(registration => registration(services));
			return services.BuildServiceProvider();
		}

		internal AtfKafkaReferenceApp Reset() {
			if (_serviceProvider.IsValueCreated) {
				_serviceProvider.Value.Dispose();
			}
			_instance = new Lazy<AtfKafkaReferenceApp>(() => new AtfKafkaReferenceApp());
			return _instance.Value;
		}

		/// <summary>
		/// Resolves a required package service.
		/// </summary>
		public T GetRequiredService<T>() => _serviceProvider.Value.GetRequiredService<T>();

		private AtfKafkaReferenceApp() { }
	}
}
