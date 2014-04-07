package my.prototype.cassandra;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.HostDistance;
import com.datastax.driver.core.PoolingOptions;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.policies.ExponentialReconnectionPolicy;
import com.datastax.driver.core.policies.RoundRobinPolicy;
import com.datastax.driver.core.policies.TokenAwarePolicy;


public class CassandraSession implements AutoCloseable {
	
	private static final String CREATE_KEYSPACE_TEMPLATE 
	= "CREATE KEYSPACE IF NOT EXISTS %s WITH replication = {'class':'SimpleStrategy', 'replication_factor':%d};";
	
	private final String[] nodes;
	private final String keyspaceName;
	private final int replicationFactor;
	private final int minSimultaneousRequestsForLocal;
	private final int maxSimultaneousRequestsForLocal;
	private final int coreConnectionsForLocal;
	private final int maxConnectionsForLocal;
	private final int baseDelayMs;
	private final int maxDelayMs;
	
	
	private Cluster cluster;
	private Session session;
	
	public CassandraSession(String[] nodes, 
			String keyspaceName,
			int replicationFactor,
			int minSimultaneousRequestsForLocal,
			int maxSimultaneousRequestsForLocal,
			int coreConnectionsForLocal,
			int maxConnectionsForLocal,
			int baseDelayMs,
			int maxDelayMs) {
		
		this.nodes = nodes;
		this.keyspaceName = keyspaceName;
		this.replicationFactor = replicationFactor;
		this.minSimultaneousRequestsForLocal = minSimultaneousRequestsForLocal;
		this.maxSimultaneousRequestsForLocal = maxSimultaneousRequestsForLocal;
		this.coreConnectionsForLocal = coreConnectionsForLocal;
		this.maxConnectionsForLocal = maxConnectionsForLocal;
		this.baseDelayMs = baseDelayMs;
		this.maxDelayMs = maxDelayMs;
	}

	public void connect() {
		cluster = Cluster.builder()
				.addContactPoints(nodes)
				.withLoadBalancingPolicy(new TokenAwarePolicy(new RoundRobinPolicy()))
				.withPoolingOptions(
						new PoolingOptions()
						.setMinSimultaneousRequestsPerConnectionThreshold(HostDistance.LOCAL, minSimultaneousRequestsForLocal)
						.setMaxSimultaneousRequestsPerConnectionThreshold(HostDistance.LOCAL, maxSimultaneousRequestsForLocal)
						.setCoreConnectionsPerHost(HostDistance.LOCAL, coreConnectionsForLocal)
						.setMaxConnectionsPerHost(HostDistance.LOCAL, maxConnectionsForLocal)
						)
				.withReconnectionPolicy(new ExponentialReconnectionPolicy(
						baseDelayMs, maxDelayMs))
				.build();
		
		Session systemSession = cluster.connect("system");
		
		systemSession.execute(String.format(CREATE_KEYSPACE_TEMPLATE, keyspaceName, replicationFactor));
		
		this.session = cluster.connect(keyspaceName);
	}
	
	public Session getSession() {
		return session;
	}

	public void close() throws Exception {

		if (session != null) {
			session.close();
		}
		
		if (cluster != null) {
			cluster.close();
		}
	}
}
