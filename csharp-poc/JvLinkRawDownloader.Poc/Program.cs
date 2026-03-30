using System.Dynamic;
using System.Security.Cryptography;
using System.Text.Json;
using Microsoft.Win32;

namespace JvLinkRawDownloader.Poc;

internal static class Program
{
    private const string ProgId = "JVDTLab.JVLink";

    private static int Main(string[] args)
    {
        if (args.Length == 0)
        {
            PrintUsage();
            return 1;
        }

        return args[0] switch
        {
            "doctor" => RunDoctor(args),
            "status" => RunStatus(args),
            "verify" => RunVerify(args),
            _ => UnknownCommand(args[0]),
        };
    }

    private static int RunDoctor(string[] args)
    {
        string archiveDir = args.Length > 1 ? args[1] : @"D:\jvdata";
        string tempDir = args.Length > 2 ? args[2] : @"C:\JVLinkTemp";

        var checks = new[]
        {
            new DoctorCheck("windows", OperatingSystem.IsWindows() ? "PASS" : "FAIL", OperatingSystem.IsWindows() ? "Windows runtime detected." : "Windows only."),
            new DoctorCheck("process_bitness", Environment.Is64BitProcess ? "WARN" : "PASS", Environment.Is64BitProcess ? "64-bit process detected. JV-Link x86 execution should use a 32-bit process." : "x86 process detected."),
            new DoctorCheck("com_registration", ProbeComRegistration() ? "PASS" : "FAIL", ProbeComRegistration() ? $"{ProgId} is registered." : $"{ProgId} is not registered."),
            new DoctorCheck("com_activation", ProbeComActivation() ? "PASS" : "WARN", ProbeComActivation() ? "COM activation probe succeeded." : "COM activation probe skipped or failed."),
            new DoctorCheck("archive_dir", ProbeWritableDirectory(archiveDir) ? "PASS" : "FAIL", ProbeWritableDirectory(archiveDir) ? $"{archiveDir} is writable." : $"{archiveDir} is not writable."),
            new DoctorCheck("temp_dir", ProbeWritableDirectory(tempDir) ? "PASS" : "FAIL", ProbeWritableDirectory(tempDir) ? $"{tempDir} is writable." : $"{tempDir} is not writable."),
        };

        bool failed = false;
        foreach (var check in checks)
        {
            Console.WriteLine($"[{check.Status,-4}] {check.Name}: {check.Detail}");
            failed |= check.Status == "FAIL";
        }

        return failed ? 1 : 0;
    }

    private static int RunStatus(string[] args)
    {
        if (args.Length < 3)
        {
            Console.WriteLine("usage: status <archiveDir> <dataspec>");
            return 1;
        }

        var store = new ArchiveStore(args[1], args[2]);
        var current = store.LoadRef("current");
        if (current is null)
        {
            Console.WriteLine($"{args[2]}: not started");
            return 0;
        }

        Console.WriteLine($"{args[2]}: ready files={current.FileCount} timestamp={current.LastSuccessfulTimestamp} commit={current.CommitId}");
        return 0;
    }

    private static int RunVerify(string[] args)
    {
        if (args.Length < 3)
        {
            Console.WriteLine("usage: verify <archiveDir> <dataspec>");
            return 1;
        }

        var store = new ArchiveStore(args[1], args[2]);
        var result = store.Verify();
        Console.WriteLine($"{args[2]}: {(result.Ok ? "OK" : "FAILED")} commits={result.CheckedCommits.Count} objects={result.CheckedObjects}");
        foreach (var error in result.Errors)
        {
            Console.WriteLine($"  error: {error}");
        }

        return result.Ok ? 0 : 1;
    }

    private static int UnknownCommand(string command)
    {
        Console.WriteLine($"unknown command: {command}");
        PrintUsage();
        return 1;
    }

    private static void PrintUsage()
    {
        Console.WriteLine("JvLinkRawDownloader.Poc");
        Console.WriteLine("  doctor [archiveDir] [tempDir]");
        Console.WriteLine("  status <archiveDir> <dataspec>");
        Console.WriteLine("  verify <archiveDir> <dataspec>");
    }

    private static bool ProbeComRegistration()
    {
        foreach (RegistryView view in new[] { RegistryView.Registry32, RegistryView.Registry64 })
        {
            try
            {
                using var root = RegistryKey.OpenBaseKey(RegistryHive.ClassesRoot, view);
                using var key = root.OpenSubKey(ProgId);
                if (key is not null)
                {
                    return true;
                }
            }
            catch
            {
            }
        }

        return false;
    }

    private static bool ProbeComActivation()
    {
        try
        {
            Type? type = Type.GetTypeFromProgID(ProgId, throwOnError: false);
            if (type is null)
            {
                return false;
            }

            object? instance = Activator.CreateInstance(type);
            return instance is not null;
        }
        catch
        {
            return false;
        }
    }

    private static bool ProbeWritableDirectory(string rawPath)
    {
        try
        {
            Directory.CreateDirectory(rawPath);
            string tempFile = Path.Combine(rawPath, $"{Guid.NewGuid():N}.tmp");
            File.WriteAllText(tempFile, "probe");
            File.Delete(tempFile);
            return true;
        }
        catch
        {
            return false;
        }
    }
}

internal sealed record DoctorCheck(string Name, string Status, string Detail);

internal sealed record SnapshotRef(string Dataspec, string CommitId, string LastSuccessfulTimestamp, int FileCount);

internal sealed record ManifestEntry(string LogicalFilename, string ObjectSha256, long ByteCount, int RecordCount)
{
    public string FormatCode => LogicalFilename.Length >= 2 ? LogicalFilename[..2].ToUpperInvariant() : "_UNKNOWN";
    public string ViewRelPath => Path.Combine(FormatCode, $"{LogicalFilename}.jvdat");
}

internal sealed class VerifyResult
{
    public bool Ok => Errors.Count == 0;
    public List<string> Errors { get; } = new();
    public List<string> CheckedCommits { get; } = new();
    public int CheckedObjects { get; set; }
}

internal sealed class ArchiveStore
{
    private readonly string _archiveRoot;
    private readonly string _dataspec;

    public ArchiveStore(string archiveRoot, string dataspec)
    {
        _archiveRoot = archiveRoot;
        _dataspec = dataspec;
    }

    public SnapshotRef? LoadRef(string name)
    {
        string path = Path.Combine(_archiveRoot, _dataspec, "refs", $"{name}.json");
        if (!File.Exists(path))
        {
            return null;
        }

        using JsonDocument doc = JsonDocument.Parse(File.ReadAllText(path));
        JsonElement root = doc.RootElement;
        return new SnapshotRef(
            root.TryGetProperty("dataspec", out JsonElement dataspec) ? dataspec.GetString() ?? _dataspec : _dataspec,
            root.GetProperty("commit_id").GetString() ?? string.Empty,
            root.GetProperty("last_successful_timestamp").GetString() ?? string.Empty,
            root.GetProperty("file_count").GetInt32()
        );
    }

    public Dictionary<string, ManifestEntry> LoadManifest(string commitId)
    {
        var manifest = new Dictionary<string, ManifestEntry>(StringComparer.Ordinal);
        string path = Path.Combine(_archiveRoot, _dataspec, "commits", commitId, "manifest.jsonl");
        if (!File.Exists(path))
        {
            return manifest;
        }

        foreach (string line in File.ReadLines(path))
        {
            if (string.IsNullOrWhiteSpace(line))
            {
                continue;
            }

            using JsonDocument doc = JsonDocument.Parse(line);
            JsonElement root = doc.RootElement;
            var entry = new ManifestEntry(
                root.GetProperty("logical_filename").GetString() ?? string.Empty,
                root.GetProperty("object_sha256").GetString() ?? string.Empty,
                root.GetProperty("byte_count").GetInt64(),
                root.GetProperty("record_count").GetInt32()
            );
            manifest[entry.LogicalFilename] = entry;
        }

        return manifest;
    }

    public VerifyResult Verify()
    {
        var result = new VerifyResult();
        foreach (string refName in new[] { "current", "previous" })
        {
            SnapshotRef? snapshot = LoadRef(refName);
            if (snapshot is null)
            {
                continue;
            }

            result.CheckedCommits.Add(snapshot.CommitId);
            foreach ((string logicalFilename, ManifestEntry entry) in LoadManifest(snapshot.CommitId))
            {
                string objectPath = Path.Combine(_archiveRoot, _dataspec, "objects", entry.ObjectSha256[..2], $"{entry.ObjectSha256}.jvdat");
                if (!File.Exists(objectPath))
                {
                    result.Errors.Add($"{refName}: missing object for {logicalFilename}");
                    continue;
                }

                long length = new FileInfo(objectPath).Length;
                if (length != entry.ByteCount)
                {
                    result.Errors.Add($"{refName}: byte_count mismatch for {logicalFilename} ({length} != {entry.ByteCount})");
                }

                string hash = HashFile(objectPath);
                if (!string.Equals(hash, entry.ObjectSha256, StringComparison.OrdinalIgnoreCase))
                {
                    result.Errors.Add($"{refName}: sha256 mismatch for {logicalFilename}");
                }

                result.CheckedObjects += 1;
            }
        }

        return result;
    }

    private static string HashFile(string path)
    {
        using var sha = SHA256.Create();
        using var stream = File.OpenRead(path);
        return Convert.ToHexString(sha.ComputeHash(stream)).ToLowerInvariant();
    }
}

internal interface IJvLinkSession : IDisposable
{
    int Init(string sid);
    int SetSavePath(string path);
}

internal sealed class ComJvLinkSession : IJvLinkSession
{
    private dynamic? _com;

    public int Init(string sid)
    {
        Type? type = Type.GetTypeFromProgID("JVDTLab.JVLink", throwOnError: true);
        _com = Activator.CreateInstance(type!);
        return (int)_com!.JVInit(sid);
    }

    public int SetSavePath(string path)
    {
        return (int)_com!.JVSetSavePath(path);
    }

    public void Dispose()
    {
        if (_com is not null)
        {
            try
            {
                _com.JVClose();
            }
            catch
            {
            }

            _com = null;
        }
    }
}

