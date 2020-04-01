from cassandra.cluster import Cluster
from sql_queries import list_drop_tables, list_create_tables


def insert_from_dataframe(IPCluster, keyspace, table, dataframe, column_names):
    cluster, session = keyspace_connection(IPCluster, keyspace)

    cql_query = """
        INSERT INTO {table_name} ({col_names})
        VALUES (""" + ','.join(['%s' for x in list(dataframe.columns.values)]) + """)
        """

    for index, row in dataframe.iterrows():
        try:
            session.execute(cql_query.format(table_name=table, col_names=','.join(map(str, column_names))),
                            (row.values.tolist()))
        except Exception as e:
            print("Error execute query")
            print(e)

    session.shutdown()
    cluster.shutdown()


def keyspace_connection(IPCluster, keyspace):
    try:
        cluster = Cluster([IPCluster])
        session = cluster.connect()
    except cluster.Error as e:
        print("Error cluster connection")
        print(e)

    try:
        session.execute("""CREATE KEYSPACE IF NOT EXISTS sparkifydb WITH REPLICATION = { 'class' : 'SimpleStrategy', 
                        'replication_factor' : 3 };""")
    except Exception as e:
        print("Error init keyspace")
        print(e)

    try:
        session.set_keyspace(keyspace)
    except Exception as e:
        print(e)

    return cluster, session


def main(IPCluster, keyspace):
    cluster, session = keyspace_connection(IPCluster, keyspace)

    for i_drop in list_drop_tables:
        session.execute(i_drop)

    for i_create in list_create_tables:
        session.execute(i_create)

    session.shutdown()
    cluster.shutdown()


if __name__ == "__main__":
    main("127.0.0.1", "sparkifydb")