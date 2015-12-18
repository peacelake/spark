from cassandra.cluster import Cluster

cluster = Cluster(
        contact_points=['10.144.57.200'],
#        default_retry_policy = RetryPolicy()
#        load_balancing_policy= TokenAwarePolicy(DCAwareRoundRobinPolicy(local_dc='datacenter1')),
        )
session = cluster.connect('test')
session.default_timeout = 180 # in seconds

# Insert one record into the users table
prepared_stmt = session.prepare ( "INSERT INTO users (name, address, description) VALUES (?, ?, ?)")
bound_stmt = prepared_stmt.bind(['Jones', 'Austin2', 'bob@example.com'])
stmt = session.execute(bound_stmt)

# Use select to get the user we just entered
prepared_stmt = session.prepare ( "SELECT * FROM users limit 100")
#bound_stmt = prepared_stmt.bind(['Jones'])
stmt = session.execute(prepared_stmt)
for x in stmt: print x.name, x.address, x[2]

