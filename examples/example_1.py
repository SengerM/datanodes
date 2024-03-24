import datanodes

dn = datanodes.create_datanode('.', 'testing_datanode', if_exists='override')

with dn.handle_task('a_task') as task:
	with open(task.path_to_directory_of_my_task/'blah','w') as ofile:
		print('Whatever...', file=ofile)
