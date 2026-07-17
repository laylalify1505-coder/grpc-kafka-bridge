package com.bridge.decoder;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Decodes the {@code data} field from the gRPC DataEnvelope into structured
 * Java objects, handling both time.TimeData and sweep.SweepData formats.
 */
@Service
public class DataDecoder {

    private static final Logger log = LoggerFactory.getLogger(DataDecoder.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    /**
     * Decode a batch of features from the string payload.
     *
     * @param dataType  the SSE event name, e.g. "time.TimeData" or "sweep.SweepData"
     * @param dataJson  the raw JSON string from the {@code data} field (an array of features)
     * @return a list of decoded feature maps suitable for inclusion in the Kafka payload
     */
    public List<Map<String, Object>> decode(String dataType, String dataJson) {
        try {
            List<Map<String, Object>> features = MAPPER.readValue(
                    dataJson, new TypeReference<List<Map<String, Object>>>() {});

            return features.stream()
                    .map(feature -> decodeFeature(dataType, feature))
                    .collect(Collectors.toList());

        } catch (Exception e) {
            log.warn("Failed to decode {} data: {}", dataType, e.getMessage());
            return List.of();
        }
    }

    private Map<String, Object> decodeFeature(String dataType, Map<String, Object> raw) {
        Map<String, Object> result = new LinkedHashMap<>(raw);

        // Remove raw sample arrays — replace with decoded summaries
        switch (dataType) {
            case "time.TimeData":
                decodeTimeData(result);
                break;
            case "sweep.SweepData":
                decodeSweepData(result);
                break;
            default:
                log.debug("Unknown data_type '{}' — passing through raw fields", dataType);
        }

        return result;
    }

    // ── TimeData decoder ─────────────────────────────────────────────────

    /**
     * time.TimeData represents IQ time-domain samples:
     * <pre>
     * {
     *   "GPS": {"latitude":..., "longitude":..., "altitude":0.0},
     *   "center_frequency": 40000000.0,
     *   "bandwidth": 20000.0,
     *   "antenna": 0,
     *   "i": [I0, I1, ...],           // I-branch samples
     *   "q": [Q0, Q1, ...],           // Q-branch samples
     *   "sample_period": 3.2768E-5,
     *   "timestamp": "2026-07-08T13:04:41.67394192",
     *   "event_uuid": "...",
     *   "task_id": "..."
     * }
     * </pre>
     */
    @SuppressWarnings("unchecked")
    private void decodeTimeData(Map<String, Object> feature) {
        List<Double> iSamples = toDoubleList(feature.remove("i"));
        List<Double> qSamples = toDoubleList(feature.remove("q"));

        if (iSamples == null || qSamples == null || iSamples.isEmpty()) return;

        int n = Math.min(iSamples.size(), qSamples.size());

        // Compute magnitude (dB) for each IQ pair: 20*log10(sqrt(I²+Q²))
        double[] magDB = new double[n];
        double maxMag = Double.MIN_VALUE;
        for (int k = 0; k < n; k++) {
            double mag = Math.sqrt(iSamples.get(k) * iSamples.get(k) + qSamples.get(k) * qSamples.get(k));
            magDB[k] = 20 * Math.log10(Math.max(mag, 1e-15));
            if (magDB[k] > maxMag) maxMag = magDB[k];
        }

        // Compute basic statistics
        DoubleSummaryStatistics stats = Arrays.stream(magDB).summaryStatistics();

        // Build a decimated summary of the IQ magnitude envelope (max 256 points)
        int summaryPoints = Math.min(n, 256);
        double[] decimatedMag = new double[summaryPoints];
        for (int k = 0; k < summaryPoints; k++) {
            int idx = (int) ((long) k * n / summaryPoints);
            decimatedMag[k] = Math.round(magDB[idx] * 100.0) / 100.0;
        }

        // Convert I/Q to ci32_le binary (interleaved int32 LE)
        byte[] ci32 = IqConverter.toCi32Le(iSamples, qSamples);
        String ci32Base64 = java.util.Base64.getEncoder().encodeToString(ci32);

        Map<String, Object> decoded = new LinkedHashMap<>();
        decoded.put("sample_count", n);
        decoded.put("sample_period", feature.get("sample_period"));
        decoded.put("magnitude_db_min", Math.round(stats.getMin() * 100.0) / 100.0);
        decoded.put("magnitude_db_max", Math.round(stats.getMax() * 100.0) / 100.0);
        decoded.put("magnitude_db_mean", Math.round(stats.getAverage() * 100.0) / 100.0);
        decoded.put("magnitude_db_envelope", decimatedMag);

        // Compute approximate power spectral density (via FFT approximation)
        // Simple periodogram: sum of squared magnitudes per sample
        double totalPower = 0;
        for (int k = 0; k < n; k++) {
            totalPower += iSamples.get(k) * iSamples.get(k) + qSamples.get(k) * qSamples.get(k);
        }
        decoded.put("total_power", Math.round(totalPower * 1e12) / 1e12);
        decoded.put("avg_power_per_sample", Math.round(totalPower / n * 1e15) / 1e15);

        // ci32_le binary payload (interleaved int32, little-endian)
        decoded.put("iq_format", "ci32_le");
        decoded.put("iq_bytes", ci32.length);
        decoded.put("iq_scale_factor", IqConverter.computeScale(iSamples, qSamples, n));
        // Add the binary payload as base64 for Kafka transport
        decoded.put("iq_data_base64", ci32Base64);

        feature.put("_decoded", decoded);
    }

    // ── SweepData decoder ────────────────────────────────────────────────

    /**
     * sweep.SweepData represents PSD frequency-sweep data:
     * <pre>
     * {
     *   "GPS": {"latitude":..., "longitude":..., "altitude":0.0},
     *   "accumulation_count": 1,
     *   "start_frequency": 10000000.0,
     *   "end_frequency": 15001068.115,
     *   "resolution_bandwidth": 7647.0,
     *   "agc": "TRACKING",
     *   "antenna": 0,
     *   "event_uuid": "...",
     *   "processing": "NONE",
     *   "unit": "POWER",
     *   "is_signal_compressed": false,
     *   "loops": 1,
     *   "values": [p0, p1, ...],       // PSD power in dBm per frequency bin
     *   "task_id": "...",
     *   "timestamp": "..."
     * }
     * </pre>
     */
    @SuppressWarnings("unchecked")
    private void decodeSweepData(Map<String, Object> feature) {
        List<Double> rawValues = toDoubleList(feature.remove("values"));
        if (rawValues == null || rawValues.isEmpty()) return;

        int n = rawValues.size();

        DoubleSummaryStatistics stats = rawValues.stream()
                .mapToDouble(Double::doubleValue)
                .summaryStatistics();

        double startFreq = getDouble(feature, "start_frequency");
        double endFreq = getDouble(feature, "end_frequency");
        double bw = (endFreq - startFreq) / n;

        // Build the frequency axis and find signal peaks
        List<Map<String, Object>> peaks = new ArrayList<>();
        List<Double> freqAxis = new ArrayList<>(n);
        double[] vals = new double[n];
        for (int k = 0; k < n; k++) {
            double f = startFreq + k * bw;
            freqAxis.add(f);
            vals[k] = rawValues.get(k);
        }

        // Simple peak detection: find samples above mean + 1 stddev
        double mean = stats.getAverage();
        double threshold = mean + 10;  // 10 dB above average floor
        for (int k = 1; k < n - 1; k++) {
            if (vals[k] > threshold && vals[k] >= vals[k - 1] && vals[k] >= vals[k + 1]) {
                Map<String, Object> peak = new LinkedHashMap<>();
                peak.put("frequency_hz", Math.round(freqAxis.get(k)));
                peak.put("power_dbm", Math.round(vals[k] * 100.0) / 100.0);
                peaks.add(peak);
            }
        }

        // Decimate values array for the envelope (max 512 points)
        int summaryPoints = Math.min(n, 512);
        double[] decimated = new double[summaryPoints];
        for (int k = 0; k < summaryPoints; k++) {
            int idx = (int) ((long) k * n / summaryPoints);
            decimated[k] = Math.round(vals[idx] * 100.0) / 100.0;
        }

        Map<String, Object> decoded = new LinkedHashMap<>();
        decoded.put("bin_count", n);
        decoded.put("resolution_bw_hz", Math.round(bw));
        decoded.put("power_dbm_min", Math.round(stats.getMin() * 100.0) / 100.0);
        decoded.put("power_dbm_max", Math.round(stats.getMax() * 100.0) / 100.0);
        decoded.put("power_dbm_mean", Math.round(stats.getAverage() * 100.0) / 100.0);
        decoded.put("power_dbm_median", Math.round(percentile(vals, 50) * 100.0) / 100.0);
        decoded.put("power_noise_floor", Math.round(percentile(vals, 10) * 100.0) / 100.0);
        decoded.put("peak_count", peaks.size());
        decoded.put("peaks", peaks);
        decoded.put("power_envelope", decimated);

        feature.put("_decoded", decoded);
    }

    // ── Helpers ───────────────────────────────────────────────────────────

    @SuppressWarnings("unchecked")
    private List<Double> toDoubleList(Object value) {
        if (value instanceof List) {
            List<?> list = (List<?>) value;
            List<Double> result = new ArrayList<>(list.size());
            for (Object o : list) {
                if (o instanceof Number) result.add(((Number) o).doubleValue());
                else return null;
            }
            return result;
        }
        return null;
    }

    private double getDouble(Map<String, Object> map, String key) {
        Object v = map.get(key);
        if (v instanceof Number) return ((Number) v).doubleValue();
        return 0.0;
    }

    private double percentile(double[] sorted, double p) {
        // Quick-select using partial sort for percentile
        double[] copy = sorted.clone();
        Arrays.sort(copy);
        int idx = (int) Math.ceil(p / 100.0 * copy.length) - 1;
        return copy[Math.max(0, Math.min(idx, copy.length - 1))];
    }
}
