package de.metanome.algorithms.mvddet;

import java.io.*;
import java.lang.management.ManagementFactory;
import java.time.LocalDateTime;
import java.util.*;
import com.sun.management.OperatingSystemMXBean;

public final class LocalTiming {

  /** Receives one result file per data set, next to the timing log. */
  private static final String MVD_OUTPUT_DIR = "metanome/mvds";

  // Limits the run must be under, passed in by run_localtiming.sh. These are only used to
  // CHECK the real limits; the CSV records what is read back from the OS and the JVM.
  private static final int EXPECTED_CPU_CORES = envInt("DISTMVD_CPU_CORES", 16);
  private static final int EXPECTED_RAM_GB = envInt("DISTMVD_RAM_GB", 64);
  private static final int EXPECTED_DISK_GB = envInt("DISTMVD_DISK_GB", 64);
  private static final int EXPECTED_JAVA_HEAP_GB = envInt("DISTMVD_JAVA_HEAP_GB", 32);

  private static final double GB = 1024.0 * 1024.0 * 1024.0;

  /** Limits actually in force for this JVM, detected at startup. */
  private static EnforcedLimits enforced;

  private static int envInt(String key, int fallback) {
    String value = System.getenv(key);
    return value == null || value.trim().isEmpty() ? fallback : Integer.parseInt(value.trim());
  }

  public static void main(String[] args) throws Exception {
    enforced = EnforcedLimits.detect();
    enforced.verify();

    String configPath = args.length > 0 ? args[0] : "experiments.conf";
    List<Experiment> experiments = loadExperiments(configPath);

    // Optional experiment number (1-based) enables process isolation: the shell
    // script launches a fresh JVM for every experiment, avoiding old heap state.
    if (args.length > 1) {
      int experimentNumber = Integer.parseInt(args[1]);
      if (experimentNumber < 1 || experimentNumber > experiments.size()) {
        throw new IllegalArgumentException("Experiment index out of range: " + experimentNumber);
      }
      System.out.printf("Starting experiment %d of %d in fresh JVM%n",
          experimentNumber, experiments.size());
      runExperiment(experiments.get(experimentNumber - 1));
    } else {
      // Keep direct Java invocation backwards-compatible.
      for (Experiment exp : experiments) {
        runExperiment(exp);
      }
    }
  }

  /**
   * The resource limits actually enforced on this process, read back from the OS and JVM
   * (never copied from configuration):
   *   cores : CPUs in this process's affinity mask (Linux Cpus_allowed_list), as seen by the JVM
   *   RAM   : cgroup memory.max, else the address-space rlimit (ulimit -v)
   *   disk  : the file-size rlimit (ulimit -f); the run script also caps total output size
   *   heap  : Runtime.maxMemory(), i.e. the effective -Xmx
   */
  private static final class EnforcedLimits {
    int affinityCpus = -1;          // OS CPU affinity
    int jvmCpus;                    // CPUs the JVM uses for its thread pools / GC
    long ramBytes = -1;
    String ramBy = "none";
    long diskBytes = -1;
    String diskBy = "none";
    long heapBytes;

    static EnforcedLimits detect() {
      EnforcedLimits l = new EnforcedLimits();
      l.jvmCpus = Runtime.getRuntime().availableProcessors();
      l.heapBytes = Runtime.getRuntime().maxMemory();

      String cpuList = procField("/proc/self/status", "Cpus_allowed_list");
      if (cpuList != null) {
        l.affinityCpus = countCpuList(cpuList);
      }

      long cgroupMax = cgroupMemoryMax();
      long addressSpace = rlimit("Max address space");
      if (cgroupMax > 0) {
        l.ramBytes = cgroupMax;
        l.ramBy = "cgroup memory.max";
      } else if (addressSpace > 0) {
        l.ramBytes = addressSpace;
        l.ramBy = "ulimit -v";
      }

      long fileSize = rlimit("Max file size");
      if (fileSize > 0) {
        l.diskBytes = fileSize;
        l.diskBy = "1".equals(System.getenv("DISTMVD_ENFORCED"))
            ? "ulimit -f + total-output watchdog" : "ulimit -f";
      }
      return l;
    }

    /** Refuses to run unless every limit is really enforced at the expected value. */
    void verify() {
      List<String> problems = new ArrayList<>();
      if (affinityCpus != EXPECTED_CPU_CORES) {
        problems.add("CPU affinity is " + (affinityCpus < 0 ? "unknown" : affinityCpus + " CPUs")
            + ", expected " + EXPECTED_CPU_CORES);
      }
      if (jvmCpus != EXPECTED_CPU_CORES) {
        problems.add("JVM sees " + jvmCpus + " CPUs, expected " + EXPECTED_CPU_CORES);
      }
      if (ramBytes != (long) EXPECTED_RAM_GB << 30) {
        problems.add("RAM limit is " + fmtGb(ramBytes) + ", expected " + EXPECTED_RAM_GB + " GB");
      }
      if (diskBytes != (long) EXPECTED_DISK_GB << 30) {
        problems.add("disk (file size) limit is " + fmtGb(diskBytes) + ", expected " + EXPECTED_DISK_GB + " GB");
      }
      long heapExpected = (long) EXPECTED_JAVA_HEAP_GB << 30;
      // Some collectors report slightly less than -Xmx (one survivor space), never more.
      if (heapBytes > heapExpected || heapBytes < heapExpected * 9 / 10) {
        problems.add("Java max heap is " + fmtGb(heapBytes) + ", expected " + EXPECTED_JAVA_HEAP_GB + " GB");
      }

      System.out.println("Enforced limits: " + summary());
      if (!problems.isEmpty()) {
        String msg = "Resource limits not enforced as required:\n  - " + String.join("\n  - ", problems)
            + "\nStart the run with metanome/run_localtiming.sh on the Linux server.";
        if ("1".equals(System.getenv("DISTMVD_ALLOW_UNENFORCED"))) {
          System.err.println("WARNING (DISTMVD_ALLOW_UNENFORCED=1): " + msg);
        } else {
          throw new IllegalStateException(msg);
        }
      }
    }

    String summary() {
      return String.format(java.util.Locale.ROOT,
          "CPU=%d cores (affinity) / %d (JVM) | RAM=%s via %s | Disk=%s via %s | Java heap=%s",
          affinityCpus, jvmCpus, fmtGb(ramBytes), ramBy, fmtGb(diskBytes), diskBy, fmtGb(heapBytes));
    }

    private static String fmtGb(long bytes) {
      return bytes < 0 ? "unlimited" : String.format(java.util.Locale.ROOT, "%.2f GB", bytes / GB);
    }
  }

  /** Value of a "Key:  value" line in a /proc file, or null. */
  private static String procField(String file, String key) {
    try {
      for (String line : java.nio.file.Files.readAllLines(java.nio.file.Paths.get(file))) {
        if (line.startsWith(key + ":")) {
          return line.substring(key.length() + 1).trim();
        }
      }
    } catch (IOException | RuntimeException e) {
      // not Linux / not readable
    }
    return null;
  }

  /** Number of CPUs in a list such as "0-7,16-23". */
  private static int countCpuList(String list) {
    int n = 0;
    for (String part : list.split(",")) {
      part = part.trim();
      if (part.isEmpty()) continue;
      int dash = part.indexOf('-');
      if (dash < 0) {
        n++;
      } else {
        n += Integer.parseInt(part.substring(dash + 1)) - Integer.parseInt(part.substring(0, dash)) + 1;
      }
    }
    return n;
  }

  /** Soft limit in bytes from /proc/self/limits (e.g. "Max file size"), or -1 if unlimited/unknown. */
  private static long rlimit(String name) {
    try {
      for (String line : java.nio.file.Files.readAllLines(java.nio.file.Paths.get("/proc/self/limits"))) {
        if (line.startsWith(name)) {
          String soft = line.substring(name.length()).trim().split("\\s+")[0];
          return "unlimited".equals(soft) ? -1 : Long.parseLong(soft);
        }
      }
    } catch (IOException | RuntimeException e) {
      // not Linux / not readable
    }
    return -1;
  }

  /** memory.max of this process's cgroup (cgroup v2) in bytes, or -1 if "max"/unknown. */
  private static long cgroupMemoryMax() {
    String cg = null;
    try {
      for (String line : java.nio.file.Files.readAllLines(java.nio.file.Paths.get("/proc/self/cgroup"))) {
        if (line.startsWith("0::")) cg = line.substring(3);
      }
      if (cg == null) return -1;
      java.nio.file.Path p = java.nio.file.Paths.get("/sys/fs/cgroup" + cg, "memory.max");
      String v = new String(java.nio.file.Files.readAllBytes(p)).trim();
      return "max".equals(v) ? -1 : Long.parseLong(v);
    } catch (IOException | RuntimeException e) {
      return -1;
    }
  }

  private static List<Experiment> loadExperiments(String configPath) throws IOException {
    Properties props = new Properties();
    try (FileReader reader = new FileReader(configPath)) {
      props.load(reader);
    }

    int count = Integer.parseInt(props.getProperty("exp.count", "1"));
    List<Experiment> experiments = new ArrayList<>();

    for (int i = 1; i <= count; i++) {
      String prefix = "exp." + i + ".";

      Experiment exp = new Experiment(props.getProperty(prefix + "dataset"))
        .delimiter(props.getProperty(prefix + "delimiter", ","))
        .hasHeader(Boolean.parseBoolean(props.getProperty(prefix + "hasHeader", "false")))
        .pruningType(Integer.parseInt(props.getProperty(prefix + "pruningType", "5")))
        .removeDuplicates(Boolean.parseBoolean(props.getProperty(prefix + "removeDuplicates", "true")))
        .markUniqueValues(Boolean.parseBoolean(props.getProperty(prefix + "markUniqueValues", "true")))
        .convertToIntTuples(Boolean.parseBoolean(props.getProperty(prefix + "convertToIntTuples", "true")))
        .usePLIs(Boolean.parseBoolean(props.getProperty(prefix + "usePLIs", "true")))
        .maxPrint(Integer.parseInt(props.getProperty(prefix + "maxPrint", "-1")));

      experiments.add(exp);
    }

    return experiments;
  }

  private static void runExperiment(Experiment exp) throws Exception {
    MvDAlgorithmConfig config = new MvDAlgorithmConfig();
    config.setPruningType(exp.pruningTypeId);
    config.setRemoveDuplicates(exp.removeDuplicates);
    config.setMarkUniqueValues(exp.markUniqueValues);
    config.setConvertToIntTuples(exp.convertToIntTuples);
    config.setUsePLIs(exp.usePLIs);

    OperatingSystemMXBean osBean =
      (OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();

    // Clean baseline so the peak we sample reflects this run, not leftover garbage.
    System.gc();

    long diskWrittenStart = readProcIoWriteBytes();
    long cpuStartNs = osBean.getProcessCpuTime();

    ResourceSampler sampler = new ResourceSampler(osBean);
    sampler.start();

    long start = System.nanoTime();
    long wallStartMs = System.currentTimeMillis();

    CsvReadResult csv = readCsv(exp.datasetPath, exp.delimiter, exp.hasHeader);
    List<List<String>> records = csv.records;

    Relation rel = new Relation(records);
    long loadRelationMs = System.currentTimeMillis() - wallStartMs;

    List<MvD> mvds = MvdFinder.findMvDs(rel, config, wallStartMs, loadRelationMs);

    long end = System.nanoTime();
    double timeS = (end - start) / 1_000_000_000.0;

    sampler.sampleOnce();   // final sample while results are still live (catches very short runs)
    sampler.stopSampling();
    sampler.join();
    long cpuEndNs = osBean.getProcessCpuTime();

    if (exp.maxPrint != 0) {
      int limit = exp.maxPrint < 0 ? mvds.size() : Math.min(exp.maxPrint, mvds.size());
      for (int i = 0; i < limit; i++) {
        System.out.println(formatMvdWithColumnNames(mvds.get(i), csv.columnLabels));
      }
    }

    File mvdFile = writeMvdFile(exp, csv.columnLabels, mvds, records.size(), records.get(0).size(), timeS);

    // ---- Measured resource usage of this experiment (not limits, not config values) ----
    double wallNs = Math.max(1, end - start);
    double coresUsedAvg = (cpuStartNs < 0 || cpuEndNs < 0) ? -1 : (cpuEndNs - cpuStartNs) / wallNs;
    double coresUsedPeak = Math.max(sampler.peakCores(), coresUsedAvg);

    long ramUsedPeakBytes = readProcStatusKb("VmHWM") * 1024;   // peak resident set (Linux)
    String ramSource = "process peak RSS";
    if (ramUsedPeakBytes <= 0) {
      ramUsedPeakBytes = sampler.peakRssBytes();
    }
    if (ramUsedPeakBytes <= 0) {                                // non-Linux fallback
      ramUsedPeakBytes = sampler.peakJvmCommittedBytes();
      ramSource = "JVM committed heap+non-heap";
    }
    long heapUsedPeakBytes = sampler.peakHeapBytes();

    long diskWrittenEnd = readProcIoWriteBytes();
    long diskWrittenBytes;
    String diskSource;
    if (diskWrittenStart >= 0 && diskWrittenEnd >= 0) {
      diskWrittenBytes = diskWrittenEnd - diskWrittenStart;
      diskSource = "process bytes written";
    } else {                                                    // non-Linux fallback
      diskWrittenBytes = mvdFile.length();
      diskSource = "MVD output file size";
    }

    double mb = 1024.0 * 1024.0;
    System.out.printf(java.util.Locale.ROOT,
      "Dataset: %s | %.6f s | Rows: %d | Cols: %d | MVDs: %d%n",
      exp.datasetPath, timeS, records.size(), records.get(0).size(), mvds.size()
    );
    System.out.printf(java.util.Locale.ROOT,
      "Used resources: cores avg %.3f / peak %.3f | RAM peak %.1f MB (%s) | Java heap peak %.1f MB | Disk written %.1f KB (%s)%n",
      coresUsedAvg, coresUsedPeak, ramUsedPeakBytes / mb, ramSource,
      heapUsedPeakBytes / mb, diskWrittenBytes / 1024.0, diskSource
    );

    writeLog(
      "metanome/experiment_log.csv",
      exp,
      config,
      records.size(),
      records.get(0).size(),
      mvds.size(),
      timeS
    );
  }

  /** {@code write_bytes} from /proc/self/io (bytes this process sent to storage), or -1. */
  private static long readProcIoWriteBytes() {
    try {
      for (String line : java.nio.file.Files.readAllLines(java.nio.file.Paths.get("/proc/self/io"))) {
        if (line.startsWith("write_bytes:")) {
          return Long.parseLong(line.substring("write_bytes:".length()).trim());
        }
      }
    } catch (IOException | RuntimeException e) {
      // not Linux / not readable
    }
    return -1;
  }

  /** A "kB" field (e.g. VmRSS, VmHWM) from /proc/self/status, or -1. */
  private static long readProcStatusKb(String field) {
    try {
      for (String line : java.nio.file.Files.readAllLines(java.nio.file.Paths.get("/proc/self/status"))) {
        if (line.startsWith(field + ":")) {
          return Long.parseLong(line.substring(field.length() + 1).replace("kB", "").trim());
        }
      }
    } catch (IOException | RuntimeException e) {
      // not Linux / not readable
    }
    return -1;
  }

  /**
   * Background thread tracking high-water marks of this JVM's actual usage:
   * heap used, process resident memory, JVM committed memory, and cores busy
   * (process CPU time / wall time over 100 ms windows).
   */
  private static final class ResourceSampler extends Thread {
    private static final long CORE_WINDOW_NS = 100_000_000L;

    private final OperatingSystemMXBean osBean;
    private final java.lang.management.MemoryMXBean memBean = ManagementFactory.getMemoryMXBean();
    private volatile boolean running = true;
    private volatile long peakHeapBytes = 0;
    private volatile long peakRssBytes = 0;
    private volatile long peakJvmCommittedBytes = 0;
    private volatile double peakCores = 0;
    private long windowStartWallNs = System.nanoTime();
    private long windowStartCpuNs;

    ResourceSampler(OperatingSystemMXBean osBean) {
      this.osBean = osBean;
      this.windowStartCpuNs = osBean.getProcessCpuTime();
      setDaemon(true);
    }

    @Override
    public void run() {
      while (running) {
        sampleOnce();
        try {
          Thread.sleep(5);
        } catch (InterruptedException e) {
          break;
        }
      }
    }

    synchronized void sampleOnce() {
      java.lang.management.MemoryUsage heap = memBean.getHeapMemoryUsage();
      java.lang.management.MemoryUsage nonHeap = memBean.getNonHeapMemoryUsage();
      peakHeapBytes = Math.max(peakHeapBytes, heap.getUsed());
      peakJvmCommittedBytes = Math.max(peakJvmCommittedBytes, heap.getCommitted() + nonHeap.getCommitted());

      long rssKb = readProcStatusKb("VmRSS");
      if (rssKb > 0) {
        peakRssBytes = Math.max(peakRssBytes, rssKb * 1024);
      }

      long nowWall = System.nanoTime();
      if (nowWall - windowStartWallNs >= CORE_WINDOW_NS) {
        long nowCpu = osBean.getProcessCpuTime();
        if (nowCpu >= 0 && windowStartCpuNs >= 0) {
          peakCores = Math.max(peakCores, (nowCpu - windowStartCpuNs) / (double) (nowWall - windowStartWallNs));
        }
        windowStartWallNs = nowWall;
        windowStartCpuNs = nowCpu;
      }
    }

    long peakHeapBytes() { return peakHeapBytes; }
    long peakRssBytes() { return peakRssBytes; }
    long peakJvmCommittedBytes() { return peakJvmCommittedBytes; }
    double peakCores() { return peakCores; }

    void stopSampling() {
      running = false;
      interrupt();
    }
  }

  private static final class Experiment {
    private final String datasetPath;

    private String delimiter = ",";
    private boolean hasHeader = false;

    private int pruningTypeId = 5;
    private boolean removeDuplicates = true;
    private boolean markUniqueValues = true;
    private boolean convertToIntTuples = true;
    private boolean usePLIs = true;
    /** 0 = do not print MVDs; {@code >0} = first N; {@code -1} = all. */
    private int maxPrint = -1;

    private Experiment(String datasetPath) {
      this.datasetPath = datasetPath;
    }

    private Experiment delimiter(String v) { this.delimiter = v; return this; }
    private Experiment hasHeader(boolean v) { this.hasHeader = v; return this; }

    private Experiment pruningType(int v) { this.pruningTypeId = v; return this; }
    private Experiment removeDuplicates(boolean v) { this.removeDuplicates = v; return this; }
    private Experiment markUniqueValues(boolean v) { this.markUniqueValues = v; return this; }
    private Experiment convertToIntTuples(boolean v) { this.convertToIntTuples = v; return this; }
    private Experiment usePLIs(boolean v) { this.usePLIs = v; return this; }
    private Experiment maxPrint(int v) { this.maxPrint = v; return this; }
  }

  private static final class CsvReadResult {
    final List<List<String>> records;
    final List<String> columnLabels;

    CsvReadResult(List<List<String>> records, List<String> columnLabels) {
      this.records = records;
      this.columnLabels = columnLabels;
    }
  }

  private static CsvReadResult readCsv(String path, String delimiter, boolean hasHeader) throws IOException {
    List<List<String>> records = new ArrayList<>();
    List<String> headerRow = null;
    char delim = delimiter.isEmpty() ? ',' : delimiter.charAt(0);

    try (BufferedReader reader = new BufferedReader(new FileReader(path))) {
      String line;
      boolean first = true;

      while ((line = reader.readLine()) != null) {
        if (first && hasHeader) {
          first = false;
          if (!line.isEmpty()) {
            headerRow = parseCsvLine(line, delim);
          }
          continue;
        }
        first = false;

        if (line.isEmpty()) continue;

        records.add(parseCsvLine(line, delim));
      }
    }

    int cols = records.isEmpty() ? 0 : records.get(0).size();
    List<String> columnLabels = buildColumnLabels(headerRow, cols, hasHeader && headerRow != null);
    return new CsvReadResult(records, columnLabels);
  }

  private static List<String> buildColumnLabels(List<String> headerRow, int cols, boolean useHeader) {
    List<String> labels = new ArrayList<>(cols);
    for (int i = 0; i < cols; i++) {
      if (useHeader && headerRow != null && i < headerRow.size()) {
        String h = headerRow.get(i);
        labels.add(h.isEmpty() ? ("col" + i) : h);
      } else {
        labels.add(String.valueOf(i));
      }
    }
    return labels;
  }

  /**
   * Splits one CSV line on {@code delim}, honoring double-quoted fields so that
   * delimiters inside quotes (e.g. commas in a free-text column) are not treated
   * as field separators. A doubled quote ("") inside a quoted field is a literal quote.
   */
  private static List<String> parseCsvLine(String line, char delim) {
    List<String> row = new ArrayList<>();
    StringBuilder field = new StringBuilder();
    boolean inQuotes = false;

    for (int i = 0; i < line.length(); i++) {
      char c = line.charAt(i);

      if (inQuotes) {
        if (c == '"') {
          if (i + 1 < line.length() && line.charAt(i + 1) == '"') {
            field.append('"');   // escaped quote inside quoted field
            i++;
          } else {
            inQuotes = false;    // closing quote
          }
        } else {
          field.append(c);
        }
      } else if (c == '"') {
        inQuotes = true;
      } else if (c == delim) {
        row.add(field.toString().trim());
        field.setLength(0);
      } else {
        field.append(c);
      }
    }

    row.add(field.toString().trim());
    return row;
  }

  /**
   * Writes every MVD of one run to {@code metanome/mvds/<dataset>.txt}, where {@code <dataset>}
   * is the input file name without its extension, so {@code 11_complement.csv} becomes
   * {@code 11_complement.txt}. This is independent of {@code maxPrint}, which only limits what
   * goes to the console. A rerun on the same data set overwrites its file.
   */
  private static File writeMvdFile(Experiment exp,
                                   List<String> columnLabels,
                                   List<MvD> mvds,
                                   int rows,
                                   int cols,
                                   double timeS) throws IOException {

    File outFile = new File(MVD_OUTPUT_DIR, baseName(exp.datasetPath) + ".txt");
    File parent = outFile.getParentFile();
    if (parent != null) {
      parent.mkdirs();
    }

    try (BufferedWriter bw = new BufferedWriter(new FileWriter(outFile))) {
      bw.write("# dataset: " + exp.datasetPath);
      bw.newLine();
      bw.write("# rows: " + rows + " | cols: " + cols + " | mvds: " + mvds.size());
      bw.newLine();
      bw.write(String.format(java.util.Locale.ROOT, "# time: %.6f s | pruning: %s", timeS, exp.pruningTypeId));
      bw.newLine();
      bw.write("# generated: " + LocalDateTime.now());
      bw.newLine();

      for (MvD mvd : mvds) {
        bw.write(formatMvdWithColumnNames(mvd, columnLabels));
        bw.newLine();
      }
    }

    System.out.println("MVDs written to " + outFile.getPath());
    return outFile;
  }

  /** File name of {@code path} without its directories or extension. */
  private static String baseName(String path) {
    String name = new File(path).getName();
    int dot = name.lastIndexOf('.');
    return dot > 0 ? name.substring(0, dot) : name;
  }

  /** Resource columns hold the limits actually ENFORCED on the run, read back from OS/JVM. */
  private static final String CSV_HEADER =
    "timestamp,dataset_path,rows,cols,delimiter,has_header,pruning_type,remove_duplicates,mark_unique_values,convert_to_int_tuples,use_plis,result_count,time_s,"
    + "enforced_cpu_cores,enforced_ram_gb,ram_enforced_by,enforced_disk_gb,disk_enforced_by,enforced_java_heap_gb";

  /**
   * If the existing log has a different (older) header, move it aside to
   * experiment_log_old_<timestamp>.csv so schemas are never mixed and no data is lost.
   */
  private static void prepareLog(File logFile) throws IOException {
    if (!logFile.exists() || logFile.length() == 0) return;
    String header;
    try (BufferedReader br = new BufferedReader(new FileReader(logFile))) {
      header = br.readLine();
    }
    if (CSV_HEADER.equals(header)) return;
    String stamp = LocalDateTime.now().toString().replace(":", "").replace(".", "_");
    File old = new File(logFile.getParentFile(), "experiment_log_old_" + stamp + ".csv");
    java.nio.file.Files.move(logFile.toPath(), old.toPath());
    System.out.println("Existing log has an older format; moved it to " + old.getPath());
  }

  private static void writeLog(String logPath,
                               Experiment exp,
                               MvDAlgorithmConfig config,
                               int rows,
                               int cols,
                               int resultCount,
                               double timeS) throws IOException {

    File logFile = new File(logPath);
    logFile.getParentFile().mkdirs();
    prepareLog(logFile);
    boolean exists = logFile.exists() && logFile.length() > 0;

    try (BufferedWriter bw = new BufferedWriter(new FileWriter(logFile, true))) {
      if (!exists) {
        bw.write(CSV_HEADER);
        bw.newLine();
      }
      bw.write(String.format(java.util.Locale.ROOT,
        "%s,%s,%d,%d,%s,%b,%s,%b,%b,%b,%b,%d,%.6f,%s,%s,%s,%s,%s,%s",
        csv(LocalDateTime.now().toString()),
        csv(exp.datasetPath),
        rows, cols,
        csv(exp.delimiter), exp.hasHeader,
        csv(String.valueOf(config.getPruningType())),
        config.isRemoveDuplicates(), config.isMarkUniqueValues(),
        config.isConvertToIntTuples(), config.isUsePLIs(),
        resultCount, timeS,
        enforced.affinityCpus < 0 ? "" : String.valueOf(enforced.affinityCpus),
        gbCell(enforced.ramBytes), csv(enforced.ramBy),
        gbCell(enforced.diskBytes), csv(enforced.diskBy),
        gbCell(enforced.heapBytes)
      ));
      bw.newLine();
    }
  }

  /** GB with 2 decimals, or empty when the limit is not set. */
  private static String gbCell(long bytes) {
    return bytes < 0 ? "" : String.format(java.util.Locale.ROOT, "%.2f", bytes / GB);
  }

  private static String csv(String value) {
    if (value == null) return "";
    return "\"" + value.replace("\"", "\"\"") + "\"";
  }

  private static String formatMvdWithColumnNames(MvD mvd, List<String> columnLabels) {
    return "[" + joinAttrNames(mvd.getLeftHandSide(), columnLabels) + " ->> "
        + joinAttrNames(mvd.getRightHandSide(), columnLabels) + "]";
  }

  private static String joinAttrNames(List<Integer> indices, List<String> columnLabels) {
    if (indices.isEmpty()) {
      return "empty Set";
    }
    StringBuilder sb = new StringBuilder();
    for (int k = 0; k < indices.size(); k++) {
      if (k > 0) {
        sb.append(", ");
      }
      int idx = indices.get(k);
      if (idx >= 0 && idx < columnLabels.size()) {
        sb.append(columnLabels.get(idx));
      } else {
        sb.append(idx);
      }
    }
    return sb.toString();
  }
}