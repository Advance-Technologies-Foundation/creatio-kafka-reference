using System;
using System.Collections.Generic;
using System.Threading;
using Common.Logging;

namespace AtfKafkaReference.Kafka {

	internal interface IKafkaWorkerHost {
		void Start();
		void Stop();
	}

	internal sealed class KafkaWorkerHost : IKafkaWorkerHost, IDisposable {

		private readonly object _syncRoot = new object();
		private readonly IKafkaWorker _worker;
		private readonly KafkaOptions _options;
		private readonly ILog _log;
		private CancellationTokenSource _cancellation;
		private readonly List<Thread> _threads = new List<Thread>();

		public KafkaWorkerHost(IKafkaWorker worker, KafkaOptions options, ILog log) {
			_worker = worker;
			_options = options;
			_log = log;
		}

		public void Start() {
			lock (_syncRoot) {
				if (!_options.IsConfigured) {
					_log.Warn("Kafka listener is disabled because the KafkaPassword system setting is not configured.");
					return;
				}
				if (_threads.Count > 0) {
					return;
				}

				_cancellation = new CancellationTokenSource();
				for (int index = 0; index < _options.WorkerCount; index++) {
					int workerNumber = index + 1;
					Thread thread = new Thread(() => RunSafely(_cancellation.Token, workerNumber)) {
						IsBackground = true,
						Name = $"AtfKafkaReference.Kafka.Worker.{workerNumber}"
					};
					_threads.Add(thread);
					thread.Start();
				}
				_log.InfoFormat("Started {0} Kafka worker(s).", _threads.Count);
			}
		}

		public void Stop() {
			Thread[] threads;
			lock (_syncRoot) {
				if (_threads.Count == 0) {
					return;
				}
				_cancellation.Cancel();
				threads = _threads.ToArray();
			}

			foreach (Thread thread in threads) {
				if (!thread.Join(TimeSpan.FromSeconds(10))) {
					_log.WarnFormat("Kafka listener {0} did not stop within 10 seconds.", thread.Name);
				}
			}

			lock (_syncRoot) {
				_cancellation.Dispose();
				_cancellation = null;
				_threads.Clear();
			}
		}

		private void RunSafely(CancellationToken cancellationToken, int workerNumber) {
			try {
				_worker.Run(cancellationToken);
			} catch (Exception exception) {
				_log.Error($"Kafka listener {workerNumber} terminated unexpectedly.", exception);
			}
		}

		public void Dispose() {
			Stop();
		}
	}
}
