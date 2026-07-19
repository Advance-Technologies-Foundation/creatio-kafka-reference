# Native library lifecycle

`Confluent.Kafka` depends on native `librdkafka`. Loading a DLL or shared object directly from package content can
lock that file for the lifetime of a Creatio worker process and make later package updates fail.

This reference uses a two-stage, versioned delivery contract:

1. At build time, `Directory.Build.targets` packages all RID-specific assets from `librdkafka.redist` 2.6.1 into
   `Files/Bin[/netstandard]/native-artifacts/kafka-2.6.1-runtimes.zip` and removes loose native output.
2. On first Kafka client creation, `KafkaNativeLibraryLoader` selects the active RID.
3. `KafkaNativeRuntimeProvisioner` extracts only that RID into
   `<Creatio root>/conf/native/kafka/2.6.1/<rid>` through a staging directory and atomic move.
4. A SHA-256 marker proves that an existing immutable directory matches the packaged artifact.
5. Kafka loads the external versioned native library, so routine managed-package updates do not replace the loaded
   file.

## Safety rules

- Never overwrite an existing version directory. Publish a new versioned path for a changed native artifact.
- Validate ZIP entry paths before extraction; the provisioner rejects entries escaping its staging root.
- Extract atomically because multiple Creatio workers may provision the same version concurrently.
- Treat a native-runtime upgrade as a host operation. A managed stop switch does not unload native code; replacing a
  loaded version requires terminating every worker process that uses it.
- Ensure the Creatio worker identity can create the configured `conf/native` subtree.

The explicit `KafkaNativeLibraryPath` setting remains available for environments where platform operators provision
native libraries outside the application tree.
