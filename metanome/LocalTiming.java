package de.metanome.algorithms.mvddet;

import java.io.*;
import java.lang.management.ManagementFactory;
import java.time.LocalDateTime;
import java.util.*;
import com.sun.management.OperatingSystemMXBean;

public final class LocalTiming {

  /** Receives one result file per data set, next to the timing log. */
  private static final String MVD_OUTPUT_DIR = "metanome/mvds";

  public static void main(String[] args) throws Exception {
    String configPath = args.length > 0 ? args[0] : "experiments.conf";
    List<Experiment> experiments = loadExperiments(configPath);

    for (Experiment exp : experiments) {
      runExperiment(exp);
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
    long totalRamMb = osBean.getTotalPhysicalMemorySize() / (1024 * 1024);

    // Clean baseline so the peak we sample reflects this run, not leftover garbage.
    System.gc();

    MemorySampler sampler = new MemorySampler(osBean);
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

    sampler.stopSampling();
    sampler.join();

    int cores = Runtime.getRuntime().availableProcessors();
    long peakSysUsedMb = sampler.peakSysUsedMb();   // machine-wide RAM in use at the busiest moment
    long peakHeapMb = sampler.peakHeapMb();          // this JVM's heap high-water mark
    long maxHeapMb = Runtime.getRuntime().maxMemory() / (1024 * 1024);

    System.out.printf(
      "Dataset: %s | %.6f s | Rows: %d | Cols: %d | MVDs: %d%n",
      exp.datasetPath, timeS, records.size(), records.get(0).size(), mvds.size()
    );

    System.out.printf(
      "Cores: %d | System RAM: %d/%d MB used | JVM peak heap: %d MB (of %d MB max)%n",
      cores, peakSysUsedMb, totalRamMb, peakHeapMb, maxHeapMb
    );

    if (exp.maxPrint != 0) {
      int limit = exp.maxPrint < 0 ? mvds.size() : Math.min(exp.maxPrint, mvds.size());
      for (int i = 0; i < limit; i++) {
        System.out.println(formatMvdWithColumnNames(mvds.get(i), csv.columnLabels));
      }
    }

    writeMvdFile(exp, csv.columnLabels, mvds, records.size(), records.get(0).size(), timeS);

    writeLog(
      "metanome/experiment_log.csv",
      exp,
      config,
      records.size(),
      records.get(0).size(),
      mvds.size(),
      timeS,
      cores,
      totalRamMb,
      peakSysUsedMb,
      peakHeapMb,
      maxHeapMb
    );
  }

  /** Polls heap and physical RAM on a background thread and tracks the high-water marks. */
  private static final class MemorySampler extends Thread {
    private final OperatingSystemMXBean osBean;
    private volatile boolean running = true;
    private volatile long peakHeapBytes = 0;
    private volatile long peakSysUsedBytes = 0;

    MemorySampler(OperatingSystemMXBean osBean) {
      this.osBean = osBean;
      setDaemon(true);
    }

    @Override
    public void run() {
      Runtime rt = Runtime.getRuntime();
      while (running) {
        long heapUsed = rt.totalMemory() - rt.freeMemory();
        if (heapUsed > peakHeapBytes) {
          peakHeapBytes = heapUsed;
        }
        long sysUsed = osBean.getTotalPhysicalMemorySize() - osBean.getFreePhysicalMemorySize();
        if (sysUsed > peakSysUsedBytes) {
          peakSysUsedBytes = sysUsed;
        }
        try {
          Thread.sleep(5);
        } catch (InterruptedException e) {
          break;
        }
      }
    }

    long peakHeapMb() {
      return peakHeapBytes / (1024 * 1024);
    }

    long peakSysUsedMb() {
      return peakSysUsedBytes / (1024 * 1024);
    }

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
  private static void writeMvdFile(Experiment exp,
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
      bw.write(String.format("# time: %.6f s | pruning: %s", timeS, exp.pruningTypeId));
      bw.newLine();
      bw.write("# generated: " + LocalDateTime.now());
      bw.newLine();

      for (MvD mvd : mvds) {
        bw.write(formatMvdWithColumnNames(mvd, columnLabels));
        bw.newLine();
      }
    }

    System.out.println("MVDs written to " + outFile.getPath());
  }

  /** File name of {@code path} without its directories or extension. */
  private static String baseName(String path) {
    String name = new File(path).getName();
    int dot = name.lastIndexOf('.');
    return dot > 0 ? name.substring(0, dot) : name;
  }

  private static void writeLog(String logPath,
                               Experiment exp,
                               MvDAlgorithmConfig config,
                               int rows,
                               int cols,
                               int resultCount,
                               double timeS,
                               int cores,
                               long totalRamMb,
                               long peakSysUsedMb,
                               long peakHeapMb,
                               long maxHeapMb) throws IOException {

    File logFile = new File(logPath);
    logFile.getParentFile().mkdirs();

    boolean exists = logFile.exists() && logFile.length() > 0;

    try (BufferedWriter bw = new BufferedWriter(new FileWriter(logFile, true))) {
      if (!exists) {
        bw.write("timestamp,dataset_path,rows,cols,cores,sys_ram_total_mb,sys_ram_peak_used_mb,jvm_peak_heap_mb,jvm_max_heap_mb,delimiter,has_header,pruning_type,remove_duplicates,mark_unique_values,convert_to_int_tuples,use_plis,result_count,time_s");
        bw.newLine();
      }

      bw.write(String.format(
        "%s,%s,%d,%d,%d,%d,%d,%d,%d,%s,%b,%s,%b,%b,%b,%b,%d,%.6f",
        csv(LocalDateTime.now().toString()),
        csv(exp.datasetPath),
        rows,
        cols,
        cores,
        totalRamMb,
        peakSysUsedMb,
        peakHeapMb,
        maxHeapMb,
        csv(exp.delimiter),
        exp.hasHeader,
        csv(String.valueOf(config.getPruningType())),
        config.isRemoveDuplicates(),
        config.isMarkUniqueValues(),
        config.isConvertToIntTuples(),
        config.isUsePLIs(),
        resultCount,
        timeS
      ));

      bw.newLine();
    }
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