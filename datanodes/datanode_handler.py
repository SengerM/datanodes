from pathlib import Path
from shutil import rmtree
import warnings
import time
import datetime
import tempfile
import traceback
import shutil
import json
from inspect import isclass

def find_ugly_characters_better_to_avoid_in_paths(path:Path):
	"""Returns a set of characters that are considered as not nice options
	within a Path (e.g. a white space, better to avoid), if any of such
	characters are found. This function is based on [this](https://stackoverflow.com/a/1976172/8849755)."""
	if not isinstance(path, (Path,str)):
		raise ValueError(f'`path` must be an instance of Path.')
	NICE_CHARACTERS_FOR_FILE_AND_DIRECTORY_NAMES = {'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z', 'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z', '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', '.', '_', '-'} # https://stackoverflow.com/a/1976172/8849755
	return set(str(path))-NICE_CHARACTERS_FOR_FILE_AND_DIRECTORY_NAMES-{'/'}

def exists_datanode(path_to_datanode:Path)->bool:
	"""Returns `True` or `False` depending on whether the path points to
	an existing datanode."""
	return (path_to_datanode/'datanode.json').is_file()

def delete_directory_and_or_file_and_subtree(p:Path):
	"""Delete whatever is in `p` and all its contents."""
	if p.is_file() or p.is_symlink():
		p.unlink()
	elif p.is_dir():
		rmtree(p)

class DatanodeHandler:
	def __init__(self, path_to_datanode:Path, check_datanode_class:str=None):
		"""
		Arguments
		---------
		path_to_datanode: Path
			The path to the datanode.
		check_datanode_class: str, optional
			If a string is provided, it will be checked whether the datanode
			in `path_to_datanode` is of `check_datanode_class` class. If
			not, a `RuntimeError` is raised.
		"""
		path_to_datanode = Path(path_to_datanode) # Cast to a Path object.
		
		# If we are given a path to the `datanode.json` file, let's accept it as valid:
		if path_to_datanode.name == 'datanode.json':
			path_to_datanode = path_to_datanode.parent
		
		if not exists_datanode(path_to_datanode):
			raise RuntimeError(f'Datanode in {path_to_datanode} does not exist.')
		self._path_to_the_datanode = path_to_datanode
		
		with open(self.path_to_datanode_directory/'datanode.json','r') as ifile:
			self._datanode_metadata = json.load(ifile)
		
		if check_datanode_class is not None:
			self.check_datanode_class(check_datanode_class)
	
	@property
	def path_to_datanode_directory(self)->Path:
		"""Returns a `Path` object pointing to the datanode directory
		in the file system."""
		return self._path_to_the_datanode
	
	@property
	def datanode_name(self)->str:
		"""Return a string with the name of the datanode."""
		return self._path_to_the_datanode.parts[-1]
	
	@property
	def path_to_temporary_directory(self)->Path:
		"""Returns a `Path` pointing to a temporary directory to store stuff. 
		It will	be lost after Python ends."""
		if not hasattr(self, '_temporary_directory'):
			self._temporary_directory = tempfile.TemporaryDirectory()
		return Path(self._temporary_directory.name)
	
	@property
	def parent(self):
		"""Returns a `DatanodeHandler` pointing to the parent of this 
		datanode handler. If it does not exist, returns `None`."""
		p = self.path_to_datanode_directory.parent.parent.parent
		if exists_datanode(p) == False:
			return None
		return DatanodeHandler(p)
	
	@property
	def pseudopath(self)->Path:
		"""Returns the 'pseudopath' to the datanode, this means the
		path counting only the datanodes instances, not the actual directories.
		If the datanode does not exist in the file system, `None` is returned."""
		if not hasattr(self, '_pseudopath'):
			pseudopath = [self]
			while pseudopath[0].parent is not None:
				pseudopath.insert(0, pseudopath[0].parent)
			self._pseudopath = '/'.join([b.datanode_name for b in pseudopath])
			self._pseudopath = Path(self._pseudopath)
		return self._pseudopath
	
	@property
	def datanode_class(self)->str:
		"""Returns the class of a datanode as a string, or `None` if it
		has no class assigned."""
		return self._datanode_metadata.get('datanode_class')
	
	def check_datanode_class(self, datanode_class:str, raise_error:bool=True)->bool:
		"""Check the class of the datanode.
		
		Arguments
		---------
		datanode_class: str
			The datanode class to be checked for this datanode.
		raise_error: bool, default True
			If `True`, a `RuntimeError` will be raised if the datanode
			class is not coincident with `datanode_class`.
		"""
		is_the_same_class = datanode_class == self.datanode_class
		if raise_error == True and not is_the_same_class:
			raise RuntimeError(f'Datanode "{self.pseudopath}" is of class "{self.datanode_class}" and not of class "{datanode_class}". ')
		return is_the_same_class
			
	def _path_to_directory_of_subdatanodes_of_task(self, task_name:str)->Path:
		"""Returns a `Path` pointing to where the subruns should be found."""
		return self.path_to_datanode_directory/f'{task_name}/subdatanodes'
		
	def path_to_directory_of_task(self, task_name:str, check_task_completed:bool=True)->Path:
		"""Returns a path to the directory of a task inside the datanode. 
		
		Arguments
		---------
		task_name: str
			The name of the task for which you want the path to its directory.
		check_task_completed: bool, default True
			If `True`, an error will be raised if the task cannot be verified
			to be completed. If `False`, the path will be returned without
			checking for task completion beforehand.
		"""
		if check_task_completed == True:
			self.check_these_tasks_were_run_successfully(task_name)
		return self.path_to_datanode_directory/task_name
	
	def list_subdatanodes_of_task(self, task_name:str)->list:
		"""Returns a list of `DatanodeHandler`s pointing to the subdatanodes
		of the task. If the task does not exist or was not completed successfully, 
		a `RuntimeError` is raised.
		
		Arguments
		---------
		task_name: str
			The name of the task.
		"""
		self.check_these_tasks_were_run_successfully(task_name)
		if self._path_to_directory_of_subdatanodes_of_task(task_name).exists():
			return [DatanodeHandler(p) for p in (self._path_to_directory_of_subdatanodes_of_task(task_name)).iterdir() if p.is_dir()]
		else:
			return []
	
	def was_task_run_successfully(self, task_name:str)->bool:
		"""If `task_name` was successfully run beforehand returns `True`,
		otherwise (task does not exist or it does but was not completed)
		returns `False`.
		
		Arguments
		---------
		task_name: str
			The name of the task.
		
		Returns
		-------
		was_run: bool
			`True` or `False` telling if the tasks was run successfully
			or not.
		"""
		was_run = False
		try:
			with open(self.path_to_directory_of_task(task_name, check_task_completed=False)/'datanode_task.json','r') as ifile:
				task_status = json.load(ifile)
			was_run = task_status.get('success') == True
		except FileNotFoundError as e:
			pass
		return was_run
	
	def check_these_tasks_were_run_successfully(self, tasks_names:list, raise_error:bool=True)->bool:
		"""Check that certain tasks were run successfully beforehand.
		
		Arguments
		---------
		tasks_names: list of str, or str
			A list with the names of the tasks to check for, or a string
			with the name of a single task to check for.
		raise_error: bool, default True
			If `True` then a `RuntimeError` will be raised if any of the
			tasks in `tasks_names` was not run successfully beforehand.
			If `False` then no error is raised.
		
		Returns
		-------
		all_tasls_were_run: bool
			`True` if all the tasks were run, else `False`.
		"""
		if isinstance(tasks_names, str):
			tasks_names = [tasks_names]
		if not isinstance(tasks_names, (list,set)) or not all([isinstance(task_name, str) for task_name in tasks_names]):
			raise ValueError(f'`tasks_names` must be a list of strings.')
		tasks_not_run = [task_name for task_name in tasks_names if not self.was_task_run_successfully(task_name)]
		all_tasks_were_run = len(tasks_not_run) == 0
		if raise_error == True and not all_tasks_were_run:
			raise RuntimeError(f"Task(s) {tasks_not_run} was(were)n't successfully run beforehand on run {self.pseudopath} located in {self.path_to_datanode_directory}.")
		return all_tasks_were_run
	
	def handle_task(self, task_name:str, check_datanode_class:str=None, check_required_tasks:set=None, keep_old_data:bool=False, allowed_exceptions:set=None):
		"""This method is used to create a new "subordinate bureaucrat" 
		of type `TaskBureaucrat` that will manage a task (instead of a
		run) within the run being managed by the current `RunBureaucrat`.
		
		This method was designed for being used together with a `with` 
		statement, e.g.
		```
		with a_run_bureaucrat.handle_task('some_task') as subordinated_task_bureaucrat:
			blah blah blah
		```
		
		Arguments
		---------
		task_name: str
			The name of the task to handle by the new bureaucrat.
		check_datanode_class: str, default None
			Checks the class of the datanode before starting a new task,
			and raises an error if it does not coincide. If `None` (default)
			the datanode class checking is omitted.
		check_required_tasks: set of str, str, default None
			Checks that the tasks were completed successfully in this 
			datanode before starting the new task, and raises an error if
			not. If `None` (default) this checking is omitted.
		keep_old_data: bool, dafault False
			If `False` then any data that may exist in the given task e.g.
			from a previous execution will be deleted. This ensures that
			in the end all the contents belong to the latest execution
			and it is not mixed with old stuff.
		allowed_exceptions: set of exceptions, default None
			A set of exceptions that if happen they are not considered errors,
			for example you may want that if you manualy stop the execution
			that is not an error so you then `allowed_exceptions={KeyboardInterrupt}`
			would handle that.
		
		Returns
		-------
		new_bureaucrat: TaskBureaucrat
			A bureaucrat to handle the task.
		"""
		if check_datanode_class is not None:
			self.check_datanode_class(check_datanode_class)
		if check_required_tasks is not None:
			self.check_these_tasks_were_run_successfully(check_required_tasks)
		return DatanodeTaskHandler(
			datanode_handler = self,
			task_name = task_name,
			keep_old_data = keep_old_data,
			allowed_exceptions = allowed_exceptions,
		)
	
	def as_type(self, convert_to):
		"""Returns a new `DatanodeHandler` pointing to the same datanode
		but with a new class.
		
		Example
		-------
		This method is useful when working with subclasses of `DatanodeHandler`:
		```
		class DatanodeHandlerMyDatanode(DatanodeHandler):
			def __init__(self, path_to_datanode:Path):
				super().__init__(path_to_datanode, check_datanode_class='MyDatanode') # Here we force this class to operate only on datanodes of class `MyDatanode`.
		```
		Now consider the following code:
		```
		dn = DatanodeHandler(some_path)
		my_datanode = dn.parent.as_type(DatanodeHandlerMyDatanode) # Here we convert the parent to our class.
		```
		The previous snippet is equivalent to:
		```
		dn = DatanodeHandler(some_path)
		my_datanode = DatanodeHandlerMyDatanode(dn.parent.path_to_datanode_directory) # Here we convert the parent to our class.
		```
		"""
		if not isclass(convert_to):
			raise ValueError(f'`convert_to` must be a class definition. ')
		if not issubclass(convert_to, DatanodeHandler):
			raise ValueError(f'`convert_to` must be a subclass of {DatanodeHandler}. ')
		return convert_to(self.path_to_datanode_directory)
	
class DatanodeTaskHandler:
	def __init__(self, datanode_handler:DatanodeHandler, task_name:str, keep_old_data:bool=False, allowed_exceptions:set=None):
		"""Create a `DatanodeTaskHandler`.
		
		Arguments
		---------
		datanode_handler: DatanodeHandler
			A `DatanodeHandler` object pointing to the datanode within which
			to create the task.
		task_name: str
			The name of the task.
		keep_old_data: bool, default False
			If `False`, the directory for this task will be cleaned when
			this handler begins operating. Otherwise, any previous data
			will be untouched.
		allowed_exceptions: set of exceptions, default None
			A set of exceptions that if happen they are not considered errors,
			for example you may want that if you manualy stop the execution
			that is not an error so you then `allowed_exceptions={KeyboardInterrupt}`
			would handle that.
		"""
		if len(find_ugly_characters_better_to_avoid_in_paths(task_name)) != 0:
			warnings.warn(f'Your `task_name` is {repr(task_name)} and contains the character/s {find_ugly_characters_better_to_avoid_in_paths(task_name)} which is better to avoid, as this is going to be a path in the file system.')
		self._datanode_handler = datanode_handler
		self._task_name = task_name
		self._drop_old_data = keep_old_data == False
		self._allowed_exceptions = allowed_exceptions if allowed_exceptions is not None else {}
	
	@property
	def task_name(self)->str:
		"""Returns a string with the name of the task."""
		return self._task_name
	
	@property
	def path_to_directory_of_my_task(self)->Path:
		"""Returns a `Path` object pointing to the directory of the current
		task."""
		return self._datanode_handler.path_to_directory_of_task(self.task_name, check_task_completed=False)
	
	def __enter__(self):
		if hasattr(self, '_already_did_my_job'):
			raise RuntimeError(f'A {DatanodeTaskHandler} can only be used once, and this one has already been used! You have to create a new one.')
		
		if self._drop_old_data == True and self.path_to_directory_of_my_task.is_dir():
			delete_directory_and_or_file_and_subtree(self.path_to_directory_of_my_task)
		
		self.path_to_directory_of_my_task.mkdir(exist_ok=True)
		
		return self
		
	def __exit__(self, exc_type, exc_value, exc_traceback):
		self._already_did_my_job = True
		
		task_status = dict(
			completed_on = str(datetime.datetime.now()),
			success = all([exc is None for exc in [exc_type, exc_value, exc_traceback]]) or exc_type in self._allowed_exceptions, # This means there was no error, see https://docs.python.org/3/reference/datamodel.html#object.__exit__
			task_name = self.task_name,
		)
		
		if task_status['success'] == False:
			task_status |= dict(
				exc_type = repr(exc_type),
				exc_value = repr(exc_value),
				exc_traceback = repr(exc_traceback),
				traceback_str = ''.join(traceback.format_stack()) + f'\n{exc_type.__name__}: {exc_value}',
			)
		
		with open(self.path_to_directory_of_my_task/'datanode_task.json', 'w') as ofile:
			json.dump(
				task_status,
				fp = ofile, 
				indent = '\t',
			)
		
	def create_subdatanode(self, subdatanode_name:str, subdatanode_class:str=None, if_exists:str='raise error')->DatanodeHandler:
		"""Create a subrun within the current task.
		
		Arguments
		---------
		subdatanode_name: str
			The name for the new subdatanode.
		if_exists: str, default 'raise error'
			Determines the behavior to follow if this method is called
			and the datanode already exists.
		
		Returns
		-------
		subdatanode_handler: DatanodeHandler
			A `DatanodeHandler` pointing to the newly created datanode.
		"""
		path_to_subdatanode = self._datanode_handler._path_to_directory_of_subdatanodes_of_task(self.task_name)/subdatanode_name
		create_datanode(
			path_where_to_create_the_datanode = path_to_subdatanode.parent,
			datanode_name = path_to_subdatanode.name,
			datanode_class = subdatanode_class,
			if_exists = if_exists,
		)
		subdatanode_handler = DatanodeHandler(path_to_subdatanode)
		return subdatanode_handler

def create_datanode(path_where_to_create_the_datanode:Path, datanode_name:str, datanode_class:str=None, if_exists:str='raise error')->DatanodeHandler:
	"""Creates a datanode.
		
	Arguments
	---------
	path_where_to_create_the_datanode: Path
		Where in the file system to create the datanode.
	datanode_name: str
		The name for the datanode.
	if_exists: str, default 'raise error'
		Determines the behavior to follow if this method is called
		and the run already exists according to the following options:
		- `'raise error'`: A `RuntimeError` is raised if the run
		already exists.
		- `'override'`: If the run already exists, it will be deleted
		(together with all its contents) and a new run will be 
		created instead.
		- `'skip'`: If the run already exists, nothing is done.
	"""
	def _create_datanode(path_where_to_create_the_datanode:Path, datanode_name:str, datanode_class:str)->Path:
		"""Creates a new datanode in the file system."""
		path_to_datanode = path_where_to_create_the_datanode/datanode_name
		if find_ugly_characters_better_to_avoid_in_paths(path_to_datanode):
			warnings.warn(f'While creating a datanode in {path_to_datanode} I noticed this path contains the character(s) {find_ugly_characters_better_to_avoid_in_paths(path_to_datanode)} which is(are) better to avoid.')
		if path_to_datanode.is_dir():
			raise RuntimeError(f'Cannot create run {datanode_name} in {path_where_to_create_the_datanode} because it already exists.')
		path_to_datanode.mkdir(parents=True)
		with open(path_to_datanode/'datanode.json', 'w') as ofile:
			json.dump(
				dict(
					datanode_created_on = str(datetime.datetime.now()),
					datanode_name = datanode_name,
					datanode_class = datanode_class,
				),
				fp = ofile, 
				indent = '\t',
			)
		return path_to_datanode
	
	OPTIONS_FOR_IF_EXISTS_ARGUMENT = {'raise error','override','skip'}
	if if_exists not in OPTIONS_FOR_IF_EXISTS_ARGUMENT:
		raise ValueError(f'`if_exists` must be one of {OPTIONS_FOR_IF_EXISTS_ARGUMENT}, received {repr(if_exists)}. ')
	
	path_where_to_create_the_datanode = Path(path_where_to_create_the_datanode)
	
	if exists_datanode(path_where_to_create_the_datanode/datanode_name):
		if if_exists == 'raise error':
			raise RuntimeError(f'Cannot create run {repr(datanode_name)} in {path_where_to_create_the_datanode} because it already exists.')
		elif if_exists == 'override':
			delete_directory_and_or_file_and_subtree(path_where_to_create_the_datanode/datanode_name)
		elif if_exists == 'skip':
			return
		else:
			raise ValueError(f'Unexpected value received for argument `if_exists`. ')
	
	datanode_path = _create_datanode(path_where_to_create_the_datanode=path_where_to_create_the_datanode, datanode_name=datanode_name, datanode_class=datanode_class)
	return DatanodeHandler(datanode_path)
