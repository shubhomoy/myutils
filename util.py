def run_query(conn, query):
    curr = conn.cursor()
    curr.execute(query)
    conn.commit()
    curr.close()
