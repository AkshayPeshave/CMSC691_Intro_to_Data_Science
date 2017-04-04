import sys, mysql.connector
from tabulate import tabulate

def getDbTableRowCounts(dbName):
	mysqlConn = mysql.connector.connect(host='localhost', user='root', password='root')
	cursor = mysqlConn.cursor()

	# Check if database exists
	mysqlQuery = '''
			SELECT COUNT(*)
			FROM INFORMATION_SCHEMA.SCHEMATA 
			WHERE SCHEMA_NAME = \''''+dbName+'''\'
			'''
	cursor.execute(mysqlQuery)
	for count in cursor:
		if count[0]==0: # DB does not exist
			print('\nDatabase\''+ dbName +'\' does not exist.\n')
			return
	
	# Retrieve row counts for tables in DB
	mysqlQuery = '''
			SELECT TABLE_NAME, TABLE_ROWS
			FROM INFORMATION_SCHEMA.TABLES
			WHERE TABLE_SCHEMA=\''''+dbName+'''\'
			ORDER BY TABLE_NAME
			'''
	cursor.execute(mysqlQuery)

	results = []
	for row in cursor:
		results.append(list(row))
	
	# Display results in tabular format
	print('\nTable Row Counts for Databse "'+dbName+'"\n')
	print(tabulate(results, headers=['Table Name','Row Count']))
	print

if __name__ == '__main__':
	getDbTableRowCounts(sys.argv[1])


################## PROGRAM OUTPUT ###################
#
# [axxe@localhost 02]$ python main.py classicmodels
# 
# Table Row Counts for Databse "classicmodels"
# 
# Table Name      Row Count
# ------------  -----------
# customers             122
# employees              23
# offices                 7
# orderdetails         2996
# orders                326
# payments              273
# productlines            7
# products              110
# 
# [axxe@localhost 02]$
#
