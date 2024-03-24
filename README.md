# datanodes

A package to help organizing data and results in a directory structure, automating error checking.

# Installation

```
pip install https://github.com/SengerM/datanodes
```

# Usage

Simple usage example:

```python
import datanodes

dn = datanodes.create_datanode(
	path_where_to_create_the_datanode = '.', 
	datanode_name = 'testing_datanode', 
	if_exists='override',
)

with dn.handle_task('a_task') as task:
	with open(task.path_to_directory_of_my_task/'blah','w') as ofile:
		print('Whatever...', file=ofile)
```

For more examples, see [here](examples).
