using System;
using System.Threading;
using Common.Logging;
using FluentAssertions;
using NSubstitute;
using NUnit.Framework;
using AtfKafkaReference.Kafka;

namespace AtfKafkaReference.Tests.Kafka {

	[TestFixture]
	public class KafkaWorkerHostTests {

		[Test]
		public void Start_ShouldNotRunWorker_WhenPasswordIsNotConfigured() {
			IKafkaWorker worker = Substitute.For<IKafkaWorker>();
			KafkaWorkerHost sut = new KafkaWorkerHost(worker, new KafkaOptions(), Substitute.For<ILog>());

			sut.Start();

			worker.DidNotReceiveWithAnyArgs().Run(default);
		}

		[Test]
		public void StartAndStop_ShouldOwnWorkerLifetime() {
			IKafkaWorker worker = Substitute.For<IKafkaWorker>();
			ManualResetEventSlim started = new ManualResetEventSlim();
			ManualResetEventSlim stopped = new ManualResetEventSlim();
			worker.When(x => x.Run(Arg.Any<CancellationToken>())).Do(call => {
				CancellationToken token = call.Arg<CancellationToken>();
				started.Set();
				token.WaitHandle.WaitOne();
				stopped.Set();
			});
			KafkaOptions options = new KafkaOptions { Password = "not-a-real-secret" };
			KafkaWorkerHost sut = new KafkaWorkerHost(worker, options, Substitute.For<ILog>());

			sut.Start();
			started.Wait(TimeSpan.FromSeconds(2)).Should().BeTrue();
			sut.Stop();

			stopped.IsSet.Should().BeTrue();
			worker.Received(1).Run(Arg.Any<CancellationToken>());
		}

		[Test]
		public void StartAndStop_ShouldRunConfiguredNumberOfIndependentWorkerLoops() {
			IKafkaWorker worker = Substitute.For<IKafkaWorker>();
			CountdownEvent started = new CountdownEvent(4);
			worker.When(x => x.Run(Arg.Any<CancellationToken>())).Do(call => {
				started.Signal();
				call.Arg<CancellationToken>().WaitHandle.WaitOne();
			});
			KafkaOptions options = new KafkaOptions { Password = "secret", WorkerCount = 4 };
			KafkaWorkerHost sut = new KafkaWorkerHost(worker, options, Substitute.For<ILog>());

			sut.Start();
			started.Wait(TimeSpan.FromSeconds(2)).Should().BeTrue();
			sut.Stop();

			worker.Received(4).Run(Arg.Any<CancellationToken>());
		}
	}
}
