package org.ekstep.ep.samza.service;

import java.util.List;
import java.util.Map;

import org.ekstep.ep.samza.core.JobMetrics;
import org.ekstep.ep.samza.core.Logger;
import org.ekstep.ep.samza.domain.Telemetry;
import org.ekstep.ep.samza.task.TelemetryExtractorConfig;
import org.ekstep.ep.samza.task.TelemetryExtractorSink;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import com.google.gson.Gson;

public class TelemetryExtractorService {

	static Logger LOGGER = new Logger(TelemetryExtractorService.class);

	private DateTimeFormatter df = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'").withZoneUTC();
	private JobMetrics metrics;

	public TelemetryExtractorService(TelemetryExtractorConfig config, JobMetrics metrics) {
		this.metrics = metrics;
	}

	@SuppressWarnings("unchecked")
	public void process(String message, TelemetryExtractorSink sink) {

		try {
			Map<String, Object> batchEvent = (Map<String, Object>) new Gson().fromJson(message, Map.class);
			long syncts = getSyncTS(batchEvent);
			String syncTimestamp = df.print(syncts);
			List<Map<String, Object>> events = (List<Map<String, Object>>) batchEvent.get("events");
			for (Map<String, Object> event : events) {
				event.put("syncts", syncts);
				event.put("@timestamp", syncTimestamp);
				String json = new Gson().toJson(event);
				sink.toSuccessTopic(json);
			}
			metrics.incSuccessCounter();
			generateAuditEvent(batchEvent, syncts, syncTimestamp, sink);
		} catch (Exception ex) {
			LOGGER.info("", "Failed to process events: " + ex.getMessage());
			sink.toErrorTopic(message);
		}

	}

	private long getSyncTS(Map<String, Object> batchEvent) {

		if (batchEvent.containsKey("syncts")) {
			Object obj = batchEvent.get("syncts");
			if (obj instanceof Long) {
				return ((Long) obj).longValue();
			}
		}

		return System.currentTimeMillis();
	}

	/**
	 * Create LOG event to audit telemetry sync
	 * 
	 * @param eventSpec
	 * @param syncts
	 * @param syncTimestamp
	 * @param sink
	 */
	private void generateAuditEvent(Map<String, Object> eventSpec, long syncts, String syncTimestamp,
			TelemetryExtractorSink sink) {

		try {
			Telemetry v3spec = new Telemetry(eventSpec, syncts, syncTimestamp);
			String auditEvent = v3spec.toJson();
			sink.toSuccessTopic(auditEvent);
		} catch (Exception e) {
			e.printStackTrace();
			LOGGER.info("", "Failed to generate LOG event: " + e.getMessage());
		}
	}

}
