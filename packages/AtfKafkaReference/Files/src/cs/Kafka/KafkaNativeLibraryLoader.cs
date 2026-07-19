using System;
using System.IO;
using System.Reflection;
using System.Runtime.InteropServices;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using Confluent.Kafka;

namespace AtfKafkaReference.Kafka {

	// Platform-specific probing is verified by build/deployment and the live Kafka E2E suite.
	[ExcludeFromCodeCoverage]
	internal static class KafkaNativeLibraryLoader {
		private static readonly KafkaNativeRuntimeProvisioner RuntimeProvisioner =
			new KafkaNativeRuntimeProvisioner();

		internal static void Load(string configuredPath) {
			if (Library.IsLoaded) {
				return;
			}

			string path = string.IsNullOrWhiteSpace(configuredPath) ? FindOrProvisionPath() : configuredPath;
			if (!File.Exists(path)) {
				throw new FileNotFoundException("The Kafka native library was not found. Configure the KafkaNativeLibraryPath system setting or preserve the package runtimes directory.", path);
			}
			Library.Load(path);
		}

		private static string FindOrProvisionPath() {
			string runtime;
			string fileName;
			Architecture architecture = RuntimeInformation.ProcessArchitecture;
			if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows)) {
				runtime = architecture == Architecture.X86 ? "win-x86" : "win-x64";
				fileName = "librdkafka.dll";
			} else if (RuntimeInformation.IsOSPlatform(OSPlatform.Linux)) {
				runtime = architecture == Architecture.Arm64 ? "linux-arm64" : "linux-x64";
				fileName = "librdkafka.so";
			} else if (RuntimeInformation.IsOSPlatform(OSPlatform.OSX)) {
				runtime = architecture == Architecture.Arm64 ? "osx-arm64" : "osx-x64";
				fileName = "librdkafka.dylib";
			} else {
				throw new PlatformNotSupportedException("No packaged librdkafka runtime is available for this operating system.");
			}

			string relativeRuntimePath = Path.Combine("runtimes", runtime, "native", fileName);
			foreach (string root in GetSearchRoots()) {
				if (IsApplicationRoot(root)) {
					string versionedRuntimePath = Path.Combine(root, "conf", Constants.Kafka.ExternalNativeDirectoryName,
						Constants.Kafka.RuntimeProductName, Constants.Kafka.NativeRuntimeVersion, runtime);
					string versionedLibraryPath = Path.Combine(versionedRuntimePath, "native", fileName);
					if (File.Exists(versionedLibraryPath)) {
						return versionedLibraryPath;
					}
					string artifactPath = FindArtifactPath(root);
					if (!string.IsNullOrWhiteSpace(artifactPath)) {
						return RuntimeProvisioner.Provision(artifactPath, versionedRuntimePath, runtime, fileName);
					}
				}
				string externalRuntimePath = Path.Combine(root, "conf", relativeRuntimePath);
				if (File.Exists(externalRuntimePath)) {
					return externalRuntimePath;
				}
				string directPath = Path.Combine(root, relativeRuntimePath);
				if (File.Exists(directPath)) {
					return directPath;
				}
				string packageBinPath = Path.Combine(root, "Terrasoft.Configuration", "Pkg",
					Constants.PackageName, "Files", "Bin");
				string frameworkPath = Path.Combine(packageBinPath, relativeRuntimePath);
				if (File.Exists(frameworkPath)) {
					return frameworkPath;
				}
				string netStandardPath = Path.Combine(packageBinPath, "netstandard", relativeRuntimePath);
				if (File.Exists(netStandardPath)) {
					return netStandardPath;
				}
			}
			throw new FileNotFoundException("The packaged Kafka native library could not be discovered. Configure the KafkaNativeLibraryPath system setting.", relativeRuntimePath);
		}

		private static string FindArtifactPath(string applicationRoot) {
			string packageBinPath = Path.Combine(applicationRoot, "Terrasoft.Configuration", "Pkg",
				Constants.PackageName, "Files", "Bin");
			string[] candidates = {
				Path.Combine(packageBinPath, Constants.Kafka.NativeArtifactsDirectoryName,
					Constants.Kafka.NativeRuntimeArtifactFileName),
				Path.Combine(packageBinPath, "netstandard", Constants.Kafka.NativeArtifactsDirectoryName,
					Constants.Kafka.NativeRuntimeArtifactFileName)
			};
			return Array.Find(candidates, File.Exists);
		}

		private static bool IsApplicationRoot(string path) {
			return Directory.Exists(Path.Combine(path, "conf")) &&
				Directory.Exists(Path.Combine(path, "Terrasoft.Configuration"));
		}

		private static IEnumerable<string> GetSearchRoots() {
			HashSet<string> roots = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
			string assemblyLocation = typeof(KafkaNativeLibraryLoader).GetTypeInfo().Assembly.Location;
			if (!string.IsNullOrWhiteSpace(assemblyLocation)) {
				AddWithParents(roots, Path.GetDirectoryName(assemblyLocation));
			}
			AddWithParents(roots, AppContext.BaseDirectory);
			AddWithParents(roots, Directory.GetCurrentDirectory());
			return roots;
		}

		private static void AddWithParents(ISet<string> roots, string path) {
			if (string.IsNullOrWhiteSpace(path)) {
				return;
			}
			DirectoryInfo directory = new DirectoryInfo(path);
			for (int level = 0; directory != null && level < 10; level++, directory = directory.Parent) {
				roots.Add(directory.FullName);
			}
		}
	}
}
