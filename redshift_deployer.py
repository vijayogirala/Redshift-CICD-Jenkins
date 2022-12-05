import sys
import psycopg2 as pg
import os
from io import open

###################################################################################################
# RedShift Deployment Tool
###################################################################################################
# - This script runs the supplied SQL script files (.sql) pointed at the supplied database.
# - Exits with code 0 upon success, 200 on error.
# - Scripts must be organized in the one of the following directories in order to assure they are
#	deployed in the appropriate order:
#	Under SQL -> 
#		-> {schema}
#			-> pre_deployment
#			-> tables
#			-> views
#			-> lbviews (late-binding views)
#			-> functions
#			-> procedures
#			-> constraints
#			-> post_deployment

# - Deployment happens within a single transaction.  Everything will rollback completely upon failure.
###################################################################################################

pre_deployment = set()
tables = set()
functions = set()
procedures = set()
views = set()
lbviews = set()
constraints = set()
post_deployment = set()

func_owner_change_sql = '''
select nsp.nspname||'.'||p.proname||'('||oidvectortypes(p.proargtypes)||') owner to ' as dyna_sql
from pg_proc p
join pg_namespace nsp ON p.pronamespace = nsp.oid
where nsp.nspname = %s and p.proname = %s;
'''

def takeTableBackup(filenames, cursor):
	for filename in filenames:
		schema_name, tbl_name = extract_obj_name(filename)
		print("Check and backup the table data: " + schema_name + '.' + tbl_name)
		cursor.execute('call rs_utils.recreate_table_with_data(%s, %s, true)', (schema_name, tbl_name))
		
def restoreTableData(filenames, cursor):
	for filename in filenames:
		schema_name, tbl_name = extract_obj_name(filename)
		print("Check and backup the table data: " + schema_name + '.' + tbl_name)
		cursor.execute('call rs_utils.restore_table_with_data(%s, %s)', (schema_name, tbl_name))

def extract_obj_name(filename):
	try:
		eles=filename.split('/')
		obj_name = None
		if len(eles) > 4:
			obj_name=eles[4:]
		else:
			obj_name=eles[3:]
		obj_full_name = obj_name[0].split('.')
		return obj_full_name[0], obj_full_name[1]
	except Exception as e:
		print('Unable to fetch schema name for # ' +filename)
	return '', ''

def generate_owner_change_sql(cursor, schema_name, obj_name, object_type, new_owner):
	cursor.execute(func_owner_change_sql,(schema_name, obj_name))
	obj_list = cursor.fetchall()
	sql_list = list(set([x[0] for x in obj_list]))
	final_sql = ""
	for sql in sql_list:
		final_sql = final_sql + 'alter ' + object_type + ' '+ sql + new_owner +'; '
	return final_sql

def writeToDB(filenames, cursor, obj_type, schema_owner_map, update_owner):

	for filename in filenames:
		# Specify encoding since GitHub puts garbage characters at beginning of file
		if os.path.exists(filename):
			with open(filename, 'r', encoding='ISO-8859–1') as sqlFile:
				print("Attempting to deploy: " + filename)
				sqlContent = sqlFile.readlines()

				if len(''.join(sqlContent).strip()) > 0:
					cursor.execute(''.join(sqlContent))
					schema_name, obj_name = extract_obj_name(filename)
					if update_owner == True and schema_name in schema_owner_map:
						if (obj_type == 'table' or obj_type == 'view') and filename.find('default_late_binding_views') < 0:
							cursor.execute('alter '+obj_type+' '+schema_name+'.'+obj_name+' owner to '+schema_owner_map[schema_name])
						elif (obj_type == 'function' or obj_type == 'procedure'):
							function_owner_change_sqls = generate_owner_change_sql(cursor, schema_name, obj_name, obj_type, schema_owner_map[schema_name])
							cursor.execute(function_owner_change_sqls)
					print("Successfully deployed: " + filename)
				else:
					print("Empty File: " + filename)
		else:
			print("File Not Found : " + filename)


def addCommitedFile(commit_item):
	if commit_item.endswith('.sql'):
		if commit_item.find('/pre_deployment/') > 0:
			pre_deployment.add(commit_item)
		elif commit_item.find('/functions/') > 0:
			functions.add(commit_item)
		elif commit_item.find('/procedures/') > 0:
			procedures.add(commit_item)
		elif commit_item.find('/views/') > 0:
			views.add(commit_item)
		elif commit_item.find('/lbviews/') > 0:
			lbviews.add(commit_item)
		elif commit_item.find('/constraints/') > 0:
			constraints.add(commit_item)
		elif commit_item.find('/post_deployment/') > 0:
			post_deployment.add(commit_item)

	if commit_item.endswith('.ddl'):
		if commit_item.find('/tables/') > 0 and commit_item not in tables:
				tables.add(commit_item)

def fetch_schema_owners(cursor, build_fsso):
	if build_fsso == '502825978':	# Required only for Central CICD Job, other jobs uses their own FSSO to build code
	    	cursor.execute('select rs_schema, owner from rs_utils.schema_owner_mapping where active = true')
	    	result1 = cursor.fetchall()
	    	schema_owner_map = dict([(x[0],'"'+x[1]+'"') for x in result1])
	else:
		print("Skipped ownership change as it is not central CICD job")
		schema_owner_map = {}
	return schema_owner_map

def main():

	print("Running RS Deployer...")

	conn = None
	cur = None
	try:

		# Argument validation: make sure the correct number of arguments was supplied
		if len(sys.argv) != 7 or len(sys.argv[6]) < 1:
			raise Exception("Invalid command line arguments. Expected <host> <database> <port> <username> <password> <commit_id> where <commit_id> is the hash for the most \\n recent commit. Note that this error may also appear if you do not have any tags on your project. You need at least one to start off.")


		# Build lists of SQL objects to be deployed. Split the new commit changes
		# UPDATE 5/2021: We are now parsing a temp file due to a limitation on the # of characters that 
		# can be passed via sys.argv. This was an issue on large commits.
		#git_commit_list = ['.' + os.sep + x for x in sys.argv[6].splitlines()]
		commit_file = "/tmp/git_diff_files_" + sys.argv[6]
		git_commit_list = []	
		with open(commit_file) as f:	
			for line in f.read().splitlines():	
			  git_commit_list.append('.' + os.sep + line)			  								
		print(git_commit_list)


		#list_file = 'SQL/rs_utils/post_deployment/BUILD_RS_DB_OBJECTS.LIST'
		list_file = 'BUILD_RS_DB_OBJECTS'
		read_list_file = True

		for commit_item in git_commit_list:

			#Ignore missing files for now. Deleted files on GIT are breaking the job
			if os.path.isfile(commit_item) == False:
			   print("File could not be found: " + commit_item + ". Skipping...")

			if os.path.isfile(commit_item) == True:
				addCommitedFile(commit_item)
				if (commit_item.find(list_file) > 0) and commit_item.endswith('.LIST') and read_list_file:
                   #Compile list of files to execute, regardless of whether the actual file is in the current commit. 
                   #If it is in the commit, the set() will ensure its only added once.
				   #read_list_file = False
				   print("Preparing to open list file...")
				   with open(commit_item) as f:
					   print("List file opened...")
					   lst_files = f.read().splitlines()
					   print("Special list files found to process:")
					   print(lst_files)
					   for file_item in lst_files:
						   if len(file_item.strip()) > 0:						
						   	addCommitedFile(file_item)
			else:
				print("File not found: " +  commit_item)

		# Deploy SQL objects in transaction
		if len(pre_deployment) > 0 or len(tables) > 0 or len(functions) > 0 or len(procedures) > 0 or len(views) > 0 or len(lbviews) > 0 or len(constraints) > 0 or len(post_deployment) > 0:
			conn = pg.connect(host=sys.argv[1], user=sys.argv[4], password=sys.argv[5], database=sys.argv[2], port=sys.argv[3], sslmode="require")
			cur = conn.cursor()
			build_fsso = sys.argv[4]
			print("Fetch Schema Owners SQL...")
			schema_owner_map = fetch_schema_owners(cur, build_fsso)
			
			print("Deploying Objects...")

			if len(pre_deployment) > 0:
				pre_deployment_list = sorted(list(pre_deployment)) #Order by filename
				writeToDB(pre_deployment_list, cur, None, None, False)
			if len(lbviews) > 0:
				writeToDB(sorted(lbviews), cur, 'table', schema_owner_map, True)
			if len(tables) > 0:
				takeTableBackup(tables, cur)
				writeToDB(tables, cur, 'table', schema_owner_map, True)
				restoreTableData(tables, cur)
			if len(views) > 0:
				writeToDB(sorted(views), cur, 'table', schema_owner_map, True)
			if len(functions) > 0:
				writeToDB(functions, cur, 'function', schema_owner_map, True)
			if len(procedures) > 0:
				writeToDB(procedures, cur, 'procedure', schema_owner_map, True)
			if len(constraints) > 0:
				writeToDB(constraints, cur, None, None, False)
			if len(post_deployment) > 0:
				post_deployment_list = sorted(list(post_deployment)) #Order by filename
				writeToDB(post_deployment_list, cur, None, None, False)

			conn.commit()
			print('-----------------------------------------------------------------------')
			print('---------------------- Deployment Summary Report ----------------------')
			print('-----------------------------------------------------------------------')
			if len(tables) > 0:
				print("Total number of tables : "+str(len(tables)))				
			if len(lbviews) > 0:
				print("Total number of LBVs : "+str(len(lbviews)))				
			if len(views) > 0:
				print("Total number of views : "+str(len(views)))
			if len(functions) > 0:
				print("Total number of functions : "+str(len(functions)))
			if len(procedures) > 0:
				print("Total number of procedures : "+str(len(procedures)))
			print('-----------------------------------------------------------------------')
		else:
			print("No valid files found to deploy in <files_to_deploy>: " + commit_file)

	except Exception as e:
		print("DEPLOYER ERROR: " + getattr(e, 'strerror', str(e)))

		if cur is not None:
			conn.rollback()

		sys.exit(200)

	finally:
		if cur is not None:
			cur.close()
		if conn is not None:
			conn.close()

	print("Done")
	sys.exit(0)


if __name__ == '__main__':
	main()
