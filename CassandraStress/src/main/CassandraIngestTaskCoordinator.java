package my.prototype.cassandra;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CassandraIngestTaskCoordinator {
	
	private static final Logger LOGGER = LoggerFactory.getLogger(CassandraIngestTaskCoordinator.class);
	
	private final CassandraTableAccessor tableAccessor;
	private final StatsReporter statsReporter;
	private final int numThreads;
	private final int numInsertsPerThread;
	private final int maxFuturesPerThread;

	public CassandraIngestTaskCoordinator(CassandraTableAccessor tableAccessor,
			StatsReporter statsReporter,
			int numThreads,
			int numInsertsPerThread,
			int maxFuturesPerThread) {
		
		this.tableAccessor = tableAccessor;
		this.statsReporter = statsReporter;
		this.numThreads = numThreads;
		this.numInsertsPerThread = numInsertsPerThread;
		this.maxFuturesPerThread = maxFuturesPerThread;
	}
	
	public void run() {
		
		ExecutorService executor = Executors.newFixedThreadPool(numThreads);
		
		statsReporter.startReporting();
		
		for (int i = 0; i < numThreads; i++) {
			
			Runnable task = new CassandraIngestTask(tableAccessor, statsReporter, numInsertsPerThread, maxFuturesPerThread);
			
			executor.execute(task);
		}

		try {
			
			executor.shutdown();
			executor.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
			
			LOGGER.info("All worker threads have stopped");


		} catch (InterruptedException e) {

			LOGGER.error("Interrupted while attempting to terminate worker threads {}", e);;
		}
		
		statsReporter.stopReporting();
	}
}
