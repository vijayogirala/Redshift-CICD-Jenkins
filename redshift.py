import psycopg2
def handler(event,context):
    conn = psycopg2.connect(
        host="redshift-cluster-1.cns3utfiedth.ap-southeast-2.redshift.amazonaws.com",
        database="dev",
        user="awsuser",
        port=5439,
        password="9666578821Pr")
    cur = conn.cursor()
    print("connection succussfully established")
    cur.execute("select * from users")
    print(cur.fetchall())
    print(".........")
    print()
    conn.close()
