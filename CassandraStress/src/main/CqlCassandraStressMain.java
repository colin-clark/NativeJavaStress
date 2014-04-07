package my.prototype.cassandra;

import org.apache.log4j.PropertyConfigurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.support.ClassPathXmlApplicationContext;

public class CqlCassandraStressMain {
	
	private static final Logger LOGGER = LoggerFactory.getLogger(CqlCassandraStressMain.class);
	
	static {
        PropertyConfigurator.configureAndWatch("log4j.properties", 5000);
	}

	public static void main(String[] args) {
		
		try (ClassPathXmlApplicationContext context 
				= new ClassPathXmlApplicationContext("cql-cassandra-stress-context.xml")) {	
			
			CassandraIngestTaskCoordinator cassandraIngestTaskCoordinator 
				= context.getBean("cassandraIngestTaskCoordinator", CassandraIngestTaskCoordinator.class);
			
			cassandraIngestTaskCoordinator.run();			
			
		} catch (Exception e) {
			LOGGER.error("{}", e);
		}
	}
}
