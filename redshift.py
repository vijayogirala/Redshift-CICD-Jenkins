import psycopg2
conn = psycopg2.connect(
    host="redshift-cluster-soundarya.cpiazh88ds78.us-east-1.redshift.amazonaws.com",
    database="dev",
    user="awsuser",
    port=5439,
    password="Admin12345")
cur = conn.cursor()
cur.execute("CREATE TABLE emp(id integer PRIMARY KEY, num integer, data varchar);")
print("connection succussfully established")
#cur.execute("select city from users")
print(cur.fetchall())
print(".........")
print()
conn.close()
