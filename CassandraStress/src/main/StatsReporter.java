package my.prototype.cassandra;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.atomic.AtomicLong;

public class StatsReporter {
	
	private static final AtomicLong insertCount = new AtomicLong();
	
	private final int reportInterval;
	
	boolean report = true;
	
	public StatsReporter(int reportInterval) {
		
		this.reportInterval = reportInterval;
	}

	public void incrementInsertCountBy(long num) {
		insertCount.addAndGet(num);
	}
	
	public void startReporting() {
		
		Runnable reportTask = new Runnable() {

			private final SimpleDateFormat dateFormatter = new SimpleDateFormat("hh:mm:ss");
			
			@Override
			public void run() {
				
				Date startTime = new Date();
				
				System.out.printf("Start time: %s\n\n", dateFormatter.format(startTime));
				
				System.out.println("interval(secs),items inserted during last interval,items inserted");
				
				long itemsInsertedSoFar = 0;
				int i;
				
				for (i = 1; report; i++) {
					try {
						Thread.sleep(reportInterval*1000);
					} catch (InterruptedException e) {
					}
					
					long currentCount = insertCount.get();
					
					System.out.printf("%5d,%9d,%9d\n", i*reportInterval, currentCount-itemsInsertedSoFar, currentCount);
					
					itemsInsertedSoFar = currentCount;
				}

				Date endTime = new Date();
				long runDurationSecs 
					= (endTime.getTime() - startTime.getTime()) / 1000; // This is the running time for the stats 
																		// reporter thread but should be a close
																		// approximation for how long threads were
																		// busy inserting into Cassandra
				
				System.out.printf("\nEnd time: %s\n", dateFormatter.format(endTime));
				System.out.printf("Items inserted: %d\n", insertCount.get());				
				System.out.printf("Insert rate (items/second): %.2f\n", (float)(insertCount.get()/runDurationSecs));
			}				
		};
		
		Thread reportThread = new Thread(reportTask);
		
		reportThread.start();
	}
	
	public void stopReporting() {
		report = false;
	}
}

