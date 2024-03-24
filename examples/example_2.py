from datanodes import DatanodeHandler, create_datanode
import pandas
import numpy
import plotly.express as px

def measure_one_device(device_dn:DatanodeHandler, n_points:int):
	with device_dn.handle_task('measure_device', check_datanode_class='device_dn') as measure_data_task:
		measured_data = pandas.DataFrame(
			dict(
				voltage = numpy.random.rand(n_points),
				current = numpy.random.rand(n_points)*.1,
			)
		)
		measured_data.to_csv(measure_data_task.path_to_directory_of_my_task/'measured_data.csv')
		
		# Now simulate an error in one of the devices:
		if device_dn.datanode_name == 'device_4':
			raise RuntimeError('Cannot measure device')
			# The previous failure is automatically "hardcoded" to disk in
			# the task `measure_data_task`, so then it does not go unnoticed.

def compute_stats(device_dn:DatanodeHandler):
	with device_dn.handle_task(
		task_name = 'compute_stats', 
		check_datanode_class = 'device_dn', # To be sure this is not attempted to be run on a wrong datanode.
		check_required_tasks = 'measure_device', # Before starting, check that all the required tasks within this datanode are completed.
	) as task:
		measured_data = pandas.read_csv(device_dn.path_to_directory_of_task('measure_device')/'measured_data.csv') # Here it will automatically check that `measure_device` was successfully completed with no errors, otherwise it will raise an error.
		measured_data = measured_data.describe()
		measured_data.to_csv(task.path_to_directory_of_my_task/'stats.csv')

def plot_measured_data_and_stats(device_dn:DatanodeHandler):
	with device_dn.handle_task(
		task_name = 'plot_measured_data_and_averages',
		check_datanode_class = 'device_dn',  # To be sure this is not attempted to be run on a wrong datanode.
		check_required_tasks = {'measure_device','compute_stats'}, # Before starting, check that all the required tasks within this datanode are completed.
	) as task:
		measured_data = pandas.read_csv(device_dn.path_to_directory_of_task('measure_device')/'measured_data.csv') # Here it will automatically check that `measure_device` was successfully completed with no errors, otherwise it will raise an error.
		stats = pandas.read_csv(device_dn.path_to_directory_of_task('compute_stats')/'stats.csv') # Here it will automatically check that `compute_stats` was successfully completed with no errors, otherwise it will raise an error.
		px.line(measured_data, x='voltage', y='current', markers=True).write_html(task.path_to_directory_of_my_task/'measured_data.html', include_plotlyjs='cdn')

def analyze_one_device(device_dn:DatanodeHandler):
	compute_stats(device_dn)
	plot_measured_data_and_stats(device_dn)

def measure_multiple_devices(measurements_dn:DatanodeHandler, devices_to_measure:set):
	with measurements_dn.handle_task('measure_devices', 'measurements_dn') as measure_multiple_devices_task:
		for device_name in devices_to_measure:
			try:
				measure_one_device(
					device_dn = measure_multiple_devices_task.create_subdatanode(device_name, 'device_dn'),
					n_points = 99,
				)
			except RuntimeError as e:
				if 'cannot measure device' in repr(e).lower():
					continue
				else:
					raise e

def analyze_all_devices(measurements_dn:DatanodeHandler):
	for device_dn in measurements_dn.list_subdatanodes_of_task('measure_devices'):
		analyze_one_device(device_dn)

if __name__ == '__main__':
	top_level_datanode = create_datanode(
		path_where_to_create_the_datanode = '.',
		datanode_name = 'testing_datanodes_clases',
		datanode_class = 'measurements_dn',
		if_exists = 'override',
	)

	measure_multiple_devices(
		measurements_dn = top_level_datanode,
		devices_to_measure = {f'device_{n}' for n in [1,2,3,4,5,6,7,8]},
	)
	analyze_all_devices(top_level_datanode)
	
	analyze_one_device(top_level_datanode) # This will raise an error, because `analyze_one_device` expects to operate on datanodes of class `device_dn`, but here we are passing a `measurements_dn` which is of the wrong kind.
