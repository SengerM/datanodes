# datanodes

A package to help organizing data in a directory structure, automating error checking, and providing an easy interface within Python.

**Note** This is an evolution of [the_bureaucrat](https://github.com/SengerM/the_bureaucrat).

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

- For more examples, see [here](examples). 
- A "real life example" can be found in the [230614_AIDAinnova_test_beam](https://github.com/SengerM/230614_AIDAinnova_test_beam) repository, specifically in [the commit f33d3ec386c7d517316c5223cdaf8ff75709b3ca](https://github.com/SengerM/230614_AIDAinnova_test_beam/tree/f33d3ec386c7d517316c5223cdaf8ff75709b3ca).

I wish I had developed this earlier in my PhD. Better later than never.
