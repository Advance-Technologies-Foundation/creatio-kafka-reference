using NUnit.Framework;

namespace AtfKafkaReference.IntegrationTests.Infrastructure;

public abstract class CreatioIntegrationFixture {
	protected CreatioTestSettings Settings { get; private set; }

	[OneTimeSetUp]
	public void LoadCreatioSettings() {
		Settings = CreatioTestSettings.Load();
		TestContext.Progress.WriteLine(Settings.ToString());
	}
}
