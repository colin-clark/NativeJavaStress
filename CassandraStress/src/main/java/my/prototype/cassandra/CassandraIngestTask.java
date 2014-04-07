package my.prototype.cassandra;

import java.util.ArrayList;
import java.util.Date;
import java.util.Formatter;
import java.util.List;
import java.util.Locale;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.utils.UUIDs;

public class CassandraIngestTask implements Runnable {
	
	public static final Logger LOGGER = LoggerFactory.getLogger(CassandraIngestTask.class);
	
	private static final int REPEAT_CYCLE = 1000;
	
	private final CassandraTableAccessor tableAccessor;
	private final StatsReporter statsReporter;
	private final int numInserts;
	private final int maxFutures;	

	public CassandraIngestTask(CassandraTableAccessor tableAccessor, StatsReporter statsReporter, int numInserts, int maxFutures) {

		this.tableAccessor = tableAccessor;
		this.statsReporter = statsReporter;
		this.numInserts = numInserts;
		this.maxFutures = maxFutures;
	}

	@Override
	public void run() {

		long threadId = Thread.currentThread().getId();
		
		LOGGER.info("Thread {} is starting...", threadId);
		
		List<ResultSetFuture> resultFutures = new ArrayList<>(maxFutures);
		
		for (int i = 0; i < numInserts; i++) {			
			
			UUID id = UUIDs.timeBased();
			String name = new Formatter(Locale.US).format("Name_%d_%d", threadId%REPEAT_CYCLE, i%REPEAT_CYCLE).toString();
			UUID parent_id = UUIDs.timeBased();
			String type = new Formatter(Locale.US).format("Ty%d", i%20).toString();
			Date date = new Date((i%REPEAT_CYCLE)*10000000);
			boolean flag = ((i%2) == 0);
			
			resultFutures.add(tableAccessor.insertAsync(id, name, parent_id, type, date, flag));
			
			statsReporter.incrementInsertCountBy(1);
			
			resultFutures = waitForResultsIfNecessary(resultFutures);
		}

		waitForResultsIfNecessary(resultFutures);

		LOGGER.info("Thread {} is shutting down", threadId);
	}

	private List<ResultSetFuture> waitForResultsIfNecessary(List<ResultSetFuture> resultFutures) {
		
		if (resultFutures.size() >= maxFutures) {
			
			for (ResultSetFuture resultFuture : resultFutures) {
				try {
					resultFuture.get();
				} catch (InterruptedException | ExecutionException e) {
					LOGGER.error("Error encountered while waiting for result futures {}", e);
				}
			}
			
			return new ArrayList<>(maxFutures);
		}
		
		return resultFutures; 
	}
}
