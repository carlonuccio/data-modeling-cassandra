from cassandra.cluster import Cluster
from sql_queries import list_drop_tables, list_create_tables


def insert_from_dataframe(IPCluster, keyspace, table, dataframe, column_names):
    """
    Insert Rows in a keyspace table from a dataframe
    :param IPCluster: Host Cassandra Address
    :param keyspace: Keyspace Name
    :param table: Table Name
    :param dataframe: Pandas Dataframe
    :param column_names: list column names
    """
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


def query_keyspace(IPCluster, keyspace, cql_query):
    """
    query in a keyspace table
    :param IPCluster: Host Cassandra Address
    :param keyspace: Keyspace Name
    :param table: Table Name
    :param column_names: list column names
    :param where_clause: list where clause
    """
    cluster, session = keyspace_connection(IPCluster, keyspace)

    try:
        rows = session.execute(cql_query)
        for row in rows:
            print(row)
    except Exception as e:
        print("Error execute query")
        print(e)

    session.shutdown()
    cluster.shutdown()


def keyspace_connection(IPCluster, keyspace):
    """
    - creates the keyspace if not exists and return cluster and session
    :param IPCluster: Host Cassandra Address
    :param keyspace: keyspace Name
    :return: cluster and session
    """

    # connect to cluster
    try:
        cluster = Cluster([IPCluster])
        session = cluster.connect()
    except cluster.Error as e:
        print("Error cluster connection")
        print(e)

    # create sparkify keyspace with SimpleStrategy class
    try:
        session.execute("""CREATE KEYSPACE IF NOT EXISTS sparkifydb WITH REPLICATION = { 'class' : 'SimpleStrategy', 
                        'replication_factor' : 3 };""")
    except Exception as e:
        print("Error init keyspace")
        print(e)

    # set session
    try:
        session.set_keyspace(keyspace)
    except Exception as e:
        print(e)

    return cluster, session


def main(IPCluster, keyspace):
    """
    - Creates and set session to the sparkify keyspace.
    - Drops all the tables.
    - Creates all tables needed.
    - Finally, closes the connection.
    :param hostname: Host Cassandra Address
    :param dbname: Database Name
    """

    cluster, session = keyspace_connection(IPCluster, keyspace)

    # Drops each table using the queries in `list_drop_tables` list.
    for i_drop in list_drop_tables:
        session.execute(i_drop)

    # Creates each table using the queries in `list_create_tables` list.
    for i_create in list_create_tables:
        session.execute(i_create)

    session.shutdown()
    cluster.shutdown()


if __name__ == "__main__":
    main("127.0.0.1", "sparkifydb")