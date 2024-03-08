from SNM2SQL.import_key_setup import connect_to_sql

connector = connect_to_sql()
cursor = connector.cursor()
cursor.execute("SELECT * FROM SSC_NMS.ssc_nts WHERE STATION='ssc' ORDER BY DATETIME DESC LIMIT 1")
result = cursor.fetchall()

print(result)
connector.commit()

connector.close()
