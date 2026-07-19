using System;
using System.IO;
using System.IO.Compression;
using FluentAssertions;
using NUnit.Framework;
using AtfKafkaReference.Kafka;

namespace AtfKafkaReference.Tests.Kafka {

	[TestFixture]
	public sealed class KafkaNativeRuntimeProvisionerTests {
		private string _testDirectory;

		[SetUp]
		public void SetUp() {
			_testDirectory = Path.Combine(Path.GetTempPath(), "KafkaNativeRuntimeProvisionerTests", Guid.NewGuid().ToString("N"));
			Directory.CreateDirectory(_testDirectory);
		}

		[TearDown]
		public void TearDown() {
			if (Directory.Exists(_testDirectory)) {
				Directory.Delete(_testDirectory, true);
			}
		}

		[Test]
		public void Provision_ShouldExtractOnlyRequestedRuntimeAndWriteIdentity() {
			string artifactPath = CreateArtifact("first");
			string destinationPath = Path.Combine(_testDirectory, "conf", "native", "kafka", "2.6.1", "win-x64");

			string result = new KafkaNativeRuntimeProvisioner().Provision(artifactPath, destinationPath,
				"win-x64", "librdkafka.dll");

			result.Should().Be(Path.Combine(destinationPath, "native", "librdkafka.dll"));
			File.ReadAllText(result).Should().Be("first");
			File.Exists(Path.Combine(destinationPath, ".artifact.sha256")).Should().BeTrue();
			File.Exists(Path.Combine(destinationPath, "linux-x64", "native", "librdkafka.so")).Should().BeFalse();
		}

		[Test]
		public void Provision_WhenArtifactIsAlreadyCurrent_ShouldBeIdempotent() {
			string artifactPath = CreateArtifact("same");
			string destinationPath = Path.Combine(_testDirectory, "runtime");
			KafkaNativeRuntimeProvisioner sut = new KafkaNativeRuntimeProvisioner();
			sut.Provision(artifactPath, destinationPath, "win-x64", "librdkafka.dll");

			Action action = () => sut.Provision(artifactPath, destinationPath, "win-x64", "librdkafka.dll");

			action.Should().NotThrow();
		}

		[Test]
		public void Provision_WhenVersionDirectoryHasDifferentArtifact_ShouldRejectReplacement() {
			string firstArtifactPath = CreateArtifact("first", "first.zip");
			string secondArtifactPath = CreateArtifact("second", "second.zip");
			string destinationPath = Path.Combine(_testDirectory, "runtime");
			KafkaNativeRuntimeProvisioner sut = new KafkaNativeRuntimeProvisioner();
			sut.Provision(firstArtifactPath, destinationPath, "win-x64", "librdkafka.dll");

			Action action = () => sut.Provision(secondArtifactPath, destinationPath, "win-x64", "librdkafka.dll");

			action.Should().Throw<InvalidDataException>()
				.WithMessage("*new runtime version*");
		}

		private string CreateArtifact(string content, string fileName = "runtime.zip") {
			string artifactPath = Path.Combine(_testDirectory, fileName);
			using (ZipArchive archive = ZipFile.Open(artifactPath, ZipArchiveMode.Create)) {
				WriteEntry(archive, "win-x64/native/librdkafka.dll", content);
				WriteEntry(archive, "win-x64/native/libssl-3-x64.dll", "dependency");
				WriteEntry(archive, "linux-x64/native/librdkafka.so", "linux");
			}
			return artifactPath;
		}

		private static void WriteEntry(ZipArchive archive, string path, string content) {
			using (StreamWriter writer = new StreamWriter(archive.CreateEntry(path).Open())) {
				writer.Write(content);
			}
		}
	}
}
