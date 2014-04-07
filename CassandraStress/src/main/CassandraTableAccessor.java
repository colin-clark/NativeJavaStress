package my.prototype.cassandra;

import java.util.Date;
import java.util.UUID;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSetFuture;

public class CassandraTableAccessor {
	
	private static final String CREATE_TABLE = "CREATE TABLE IF NOT EXISTS table_1 ( " +
								            "id timeuuid, name varchar, parent_id uuid, " +
								            "type varchar, date timestamp, flag boolean, " +
								            "PRIMARY KEY (id));";
	
	private static final String INSERT_STATEMENT 
										= "INSERT INTO table_1 (id, name, parent_id, " +
										  "type, date, flag) VALUES (?,?,?,?,?,?)";

	private final CassandraSession cassandraSession;
	private final ConsistencyLevel consistencyLevel;
	
	private PreparedStatement insertStatement;
	
	public CassandraTableAccessor(CassandraSession cassandraSession, ConsistencyLevel consistencyLevel) {
		
		this.cassandraSession = cassandraSession;
		this.consistencyLevel = consistencyLevel;		
	}
	
	public void configureSchema() {
		
		createTable(CREATE_TABLE);
		
		this.insertStatement = cassandraSession.getSession().prepare(INSERT_STATEMENT).setConsistencyLevel(consistencyLevel);
	}
	
	public ResultSetFuture insertAsync(UUID id, String name, UUID parent_id, String type, Date date, boolean flag) {
		
		BoundStatement statement = insertStatement.bind(id, name, parent_id, type, date, flag);
		
		return cassandraSession.getSession().executeAsync(statement);
		
	}
	
	private void createTable(String createTableStatement) {
		cassandraSession.getSession().execute(createTableStatement);
	}
}
