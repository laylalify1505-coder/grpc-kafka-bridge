package com.bridge.decoder;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.List;

/**
 * Converts EMP time.TimeData I/Q float arrays to complex interleaved binary formats.
 *
 * The EMP API returns I and Q as JSON float arrays (μV).
 * This converter transforms them into standard SDR formats suitable for
 * downstream signal processing (GNU Radio, MATLAB, etc.).
 *
 * Supported output formats:
 * <ul>
 *   <li>{@code ci32_le} — Complex Int32, interleaved, little-endian (recommended)</li>
 *   <li>{@code ci64_le} — Complex Int64, interleaved, little-endian</li>
 * </ul>
 *
 * {@code ci32_le} is the recommended format:
 * <ul>
 *   <li>Matches the ~24-bit effective resolution of the EMP frontend</li>
 *   <li>Half the size of ci64_le</li>
 *   <li>Widely supported by SDR tools</li>
 * </ul>
 */
public class IqConverter {

    private static final double DEFAULT_SCALE = 1e8;

    private IqConverter() {}

    /**
     * Convert I/Q float arrays to ci32_le binary.
     *
     * Auto-scales the samples to maximise dynamic range without clipping.
     * Format: I0 (int32 LE), Q0 (int32 LE), I1, Q1, ...
     *
     * @param iSamples  I-branch samples (μV)
     * @param qSamples  Q-branch samples (μV)
     * @return little-endian interleaved int32 bytes
     */
    public static byte[] toCi32Le(List<Double> iSamples, List<Double> qSamples) {
        int n = Math.min(iSamples.size(), qSamples.size());
        if (n == 0) return new byte[0];

        double scale = computeScale(iSamples, qSamples, n);

        ByteBuffer buf = ByteBuffer.allocate(n * 8); // 2 × int32 × n
        buf.order(ByteOrder.LITTLE_ENDIAN);

        for (int k = 0; k < n; k++) {
            buf.putInt((int) (iSamples.get(k) * scale));
            buf.putInt((int) (qSamples.get(k) * scale));
        }

        return buf.array();
    }

    /**
     * Convert I/Q float arrays to ci64_le binary.
     *
     * Format: I0 (int64 LE), Q0 (int64 LE), I1, Q1, ...
     *
     * @param iSamples  I-branch samples (μV)
     * @param qSamples  Q-branch samples (μV)
     * @return little-endian interleaved int64 bytes
     */
    public static byte[] toCi64Le(List<Double> iSamples, List<Double> qSamples) {
        int n = Math.min(iSamples.size(), qSamples.size());
        if (n == 0) return new byte[0];

        double scale = computeScale(iSamples, qSamples, n);

        ByteBuffer buf = ByteBuffer.allocate(n * 16); // 2 × int64 × n
        buf.order(ByteOrder.LITTLE_ENDIAN);

        for (int k = 0; k < n; k++) {
            buf.putLong((long) (iSamples.get(k) * scale));
            buf.putLong((long) (qSamples.get(k) * scale));
        }

        return buf.array();
    }

    /**
     * Convert I/Q float arrays to ci32_le and wrap in a description map
     * suitable for inclusion in the Kafka decoded payload.
     */
    public static java.util.Map<String, Object> toCi32LeWithMetadata(
            List<Double> iSamples, List<Double> qSamples,
            Double samplePeriod, Double centerFrequency, Double bandwidth) {

        byte[] raw = toCi32Le(iSamples, qSamples);
        int n = Math.min(iSamples.size(), qSamples.size());

        java.util.Map<String, Object> meta = new java.util.LinkedHashMap<>();
        meta.put("format", "ci32_le");
        meta.put("sample_count", n);
        meta.put("byte_count", raw.length);
        meta.put("sample_rate_hz", samplePeriod != null && samplePeriod > 0
                ? Math.round(1.0 / samplePeriod) : 0);
        meta.put("center_frequency_hz", centerFrequency != null
                ? Math.round(centerFrequency) : 0);
        meta.put("bandwidth_hz", bandwidth != null
                ? Math.round(bandwidth) : 0);
        meta.put("scale_factor", computeScale(iSamples, qSamples, n));
        meta.put("_data_base64", java.util.Base64.getEncoder().encodeToString(raw));
        return meta;
    }

    // ── Scale computation ─────────────────────────────────────────────────

    /**
     * Compute an optimal scale factor that maps the peak I/Q magnitude
     * close to the maximum int32 value without clipping.
     *
     * The EMP API returns I/Q in μV. Typical values are ~1e-5 to 1e-3.
     * We scale so that max(abs(sample)) ≈ 80% of Integer.MAX_VALUE.
     */
    public static double computeScale(List<Double> iSamples, List<Double> qSamples, int n) {
        double maxAbs = 1e-15;
        for (int k = 0; k < n; k++) {
            double i = Math.abs(iSamples.get(k));
            double q = Math.abs(qSamples.get(k));
            if (i > maxAbs) maxAbs = i;
            if (q > maxAbs) maxAbs = q;
        }
        // Target: 80% of int32 range to leave headroom
        double target = 0.8 * Integer.MAX_VALUE;
        return target / maxAbs;
    }
}
