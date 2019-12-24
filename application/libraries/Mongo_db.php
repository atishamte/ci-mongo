<?php if (!defined('BASEPATH'))
{
	exit('No direct script access allowed');
}

/**
 * CodeIgniter MongoDB Library
 *
 * @package   CodeIgniter
 * @author    Atish Amte
 * @copyright Copyright (c) 2019, Atish Amte.
 * @version   Version 1.0
 */
Class Mongo_db
{
	# Database connection params
	private $hostname;
	private $port;
	private $database;
	private $username;
	private $password;
	private $ssl_enable = false;
	private $server_selection_try_once = false;
	private $replica_set = '';
	private $auth_source = '';
	private $authentication = false;

	# Configuration params
	private $config = [];
	private $debug;
	private $legacy_support;
	private $read_concern;
	private $read_preference;
	private $param = [];
	private $active;
	private $collection = '';
	private $projection = '';

	# Local database object
	private $db;

	# Query execution params
	private $fields = [];
	private $selects = [];
	private $updates = [];
	private $wheres = [];
	private $limit = 999999;
	private $offset = 0;
	private $sorts = [];
	private $distinct = false;
	private $count = false;

	/**
	 * Constructor
	 *
	 * Check if the Mongo PECL extension has been installed/enabled.
	 * If yes then check for all the params, configs are set and connection is successful or not.
	 *
	 * @param $param
	 */
	function __construct($param)
	{
		try
		{
			if (!class_exists('MongoDB\Driver\Manager'))
			{
				exit('The MongoDB PECL extension has not been installed or enabled');
			}
			$CI =& get_instance();
			$CI->load->config('mongo_db');
			$this->config = $CI->config->item('mongo_db');
			$this->param = $param;

			if (is_array($this->param) && count($this->param) > 0 && isset($this->param['active']) == true)
			{
				$this->active = $this->param['active'];
			}
			else
			{
				if (isset($this->config['active']) && !empty($this->config['active']))
				{
					$this->active = $this->config['active'];
				}
				else
				{
					exit('MongoDB configuration is missing');
				}
			}

			if (isset($this->config[$this->active]) == true)
			{
				if (empty($this->config[$this->active]['hostname']))
				{
					exit('Hostname missing from mongodb config group : ' . $this->active);
				}
				else
				{
					$this->hostname = trim($this->config[$this->active]['hostname']);
				}

				if (empty($this->config[$this->active]['port']))
				{
					exit('Port number missing from mongodb config group : ' . $this->active);
				}
				else
				{
					$this->port = trim($this->config[$this->active]['port']);
				}

				if (isset($this->config[$this->active]['authentication']) && empty($this->config[$this->active]['username']))
				{
					exit('Username missing from mongodb config group : ' . $this->active);
				}
				else
				{
					$this->username = trim($this->config[$this->active]['username']);
				}

				if (isset($this->config[$this->active]['authentication']) && empty($this->config[$this->active]['password']))
				{
					exit('Password missing from mongodb config group : ' . $this->active);
				}
				else
				{
					$this->password = trim($this->config[$this->active]['password']);
				}

				if (empty($this->config[$this->active]['database']))
				{
					exit('Database name missing from mongodb config group : ' . $this->active);
				}
				else
				{
					$this->database = trim($this->config[$this->active]['database']);
				}

				if (empty($this->config[$this->active]['db_debug']))
				{
					$this->debug = false;
				}
				else
				{
					$this->debug = $this->config[$this->active]['db_debug'];
				}

				if (empty($this->config[$this->active]['legacy_support']))
				{
					$this->legacy_support = false;
				}
				else
				{
					$this->legacy_support = $this->config[$this->active]['legacy_support'];
				}

				if (empty($this->config[$this->active]['read_preference']) || !isset($this->config[$this->active]['read_preference']))
				{
					$this->read_preference = MongoDB\Driver\ReadPreference::RP_PRIMARY;
				}
				else
				{
					$this->read_preference = $this->config[$this->active]['read_preference'];
				}

				if (empty($this->config[$this->active]['read_concern']) || !isset($this->config[$this->active]['read_concern']))
				{
					$this->read_concern = MongoDB\Driver\ReadConcern::MAJORITY;
				}
				else
				{
					$this->read_concern = $this->config[$this->active]['read_concern'];
				}

				$this->replica_set = trim($this->config[$this->active]['replica_set']);
				$this->auth_source = trim($this->config[$this->active]['auth_source']);
				$this->authentication = $this->config[$this->active]['authentication'];
				$this->ssl_enable = $this->config[$this->active]['ssl_enable'];
				$this->server_selection_try_once = $this->config[$this->active]['server_selection_try_once'];

				$options = [];

				$dns = 'mongodb://' . $this->hostname . ':' . $this->port . '/' . $this->database;

				if ($this->authentication)
				{
					$options['username'] = $this->username;
					$options['password'] = $this->password;
				}

				if ($this->replica_set !== '')
				{
					$options['replicaSet'] = $this->replica_set;
				}

				if ($this->auth_source !== '')
				{
					$options['authSource'] = $this->auth_source;
				}

				$options['ssl'] = $this->ssl_enable;
				$options['serverSelectionTryOnce'] = $this->server_selection_try_once;

				$this->db = new MongoDB\Driver\Manager($dns, $options);
			}
			else
			{
				exit('mongodb config group :  <strong>' . $this->active . '</strong> does not exist');
			}
		}
		catch (MongoDB\Driver\Exception\Exception $e)
		{
			if (isset($this->debug) == true && $this->debug == true)
			{
				exit('Unable to connect to MongoDB: ' . $e->getMessage());
			}
			else
			{
				exit('Unable to connect to MongoDB');
			}
		}
	}

	/**
	 * Destructor
	 */
	function __destruct()
	{
	}

	/**
	 * PUBLIC METHODS
	 */

	/**
	 * Insert
	 *
	 * Insert a new document into the passed collection
	 *
	 * @usage : $this->mongo_db->insert('collection', $data = []);
	 *
	 * @param string $collection
	 * @param array  $insert
	 *
	 * @return array
	 */
	public function insert($collection = '', $insert = [])
	{
		if (empty($collection))
		{
			exit('No Mongo collection provided');
		}

		if (!is_array($insert) || count($insert) == 0)
		{
			exit('Nothing to insert into Mongo collection or insert is not an array');
		}

		if (isset($insert['_id']) === false)
		{
			$insert['_id'] = new MongoDB\BSON\ObjectId;
		}

		$bulk = new MongoDB\Driver\BulkWrite();
		$bulk->insert($insert);

		$writeConcern = new MongoDB\Driver\WriteConcern(MongoDB\Driver\WriteConcern::MAJORITY, 1000);

		try
		{
			$this->db->executeBulkWrite($this->database . "." . $collection, $bulk, $writeConcern);
			return $this->_convert_document_id($insert);
		}
		catch (MongoDB\Driver\Exception\BulkWriteException $e)
		{
			$result = $e->getWriteResult();

			if ($writeConcernError = $result->getWriteConcernError())
			{
				if (isset($this->debug) == true && $this->debug == true)
				{
					exit('WriteConcern failure : ' . $writeConcernError->getMessage());
				}
				else
				{
					exit('WriteConcern failure');
				}
			}
		}
		catch (MongoDB\Driver\Exception\Exception $e)
		{
			if (isset($this->debug) == true && $this->debug == true)
			{
				exit('Insert of data into MongoDB failed: ' . $e->getMessage());
			}
			else
			{
				exit('Insert of data into MongoDB failed');
			}
		}
	}

	/**
	 * Batch Insert
	 *
	 * Insert a multiple document into the collection
	 *
	 * @usage : $this->mongo_db->batch_insert('collection', $data = []);
	 *
	 * @param string $collection
	 * @param array  $insert
	 *
	 * @return \MongoDB\Driver\WriteResult : bool or array : if query fail then false else array of _id successfully inserted.
	 */
	public function batch_insert($collection = '', $insert = [])
	{
		if (empty($collection))
		{
			exit('No Mongo collection selected to insert into');
		}

		if (!is_array($insert) || count($insert) == 0)
		{
			exit('Nothing to insert into Mongo collection or insert is not an array');
		}

		$doc = new MongoDB\Driver\BulkWrite();

		foreach ($insert as $ins)
		{
			if (isset($ins['_id']) === false)
			{
				$ins['_id'] = new MongoDB\BSON\ObjectId;
			}
			$doc->insert($ins);
		}

		$writeConcern = new MongoDB\Driver\WriteConcern(MongoDB\Driver\WriteConcern::MAJORITY, 1000);

		try
		{
			return $this->db->executeBulkWrite($this->database . "." . $collection, $doc, $writeConcern);
		}
		catch (MongoDB\Driver\Exception\BulkWriteException $e)
		{
			$result = $e->getWriteResult();

			if ($writeConcernError = $result->getWriteConcernError())
			{
				if (isset($this->debug) == true && $this->debug == true)
				{
					exit('WriteConcern failure : ' . $writeConcernError->getMessage());
				}
				else
				{
					exit('WriteConcern failure');
				}
			}
		}
		catch (MongoDB\Driver\Exception\Exception $e)
		{
			if (isset($this->debug) == true && $this->debug == true)
			{
				exit('Insert of data into MongoDB failed: ' . $e->getMessage());
			}
			else
			{
				exit('Insert of data into MongoDB failed');
			}
		}
	}

	/**
	 * Select
	 *
	 * Determine which fields to include OR which to exclude during the query process.
	 * If you want to only choose fields to exclude, leave $includes an empty array().
	 *
	 * @usage: $this->mongo_db->select(array('field1', 'field2'))->from('collection')->get()->result();
	 *
	 * @param array $includes
	 * @param array $excludes
	 *
	 * @return $this
	 */
	public function select($includes = [], $excludes = [])
	{
		if (!is_array($includes))
		{
			$includes = [];
		}
		if (!is_array($excludes))
		{
			$excludes = [];
		}
		if (!empty($includes))
		{
			foreach ($includes as $key => $col)
			{
				if (is_array($col))
				{
					$this->selects[$key] = $col;
				}
				else
				{
					$this->selects[$col] = 1;
				}
			}
		}
		if (!empty($excludes))
		{
			foreach ($excludes as $col)
			{
				$this->selects[$col] = 0;
			}
		}
		return $this;
	}

	/**
	 * Where
	 *
	 * Get the documents based on these search parameters. The $wheres array should
	 * be an associative array with the field as the key and the value as the search
	 * criteria.
	 *
	 * @usage : $this->mongo_db->where(array('field' => 'value'))->from('collection')->get()->result();
	 *
	 * @param array|string $wheres
	 * @param null|string  $value
	 *
	 * @return $this
	 */
	public function where($wheres, $value = null)
	{
		if (is_array($wheres))
		{
			foreach ($wheres as $wh => $val)
			{
				$this->wheres[$wh] = $val;
			}
		}
		else
		{
			$this->wheres[$wheres] = $value;
		}
		return $this;
	}

	/**
	 * or where
	 *
	 * Get the documents where the value of a $field may be something else
	 *
	 * @usage : $this->mongo_db->where_or(array('field1'=>'value1', 'field2'=>'value2'))->from('collection')->get()->result();
	 *
	 * @param array $wheres
	 *
	 * @return $this
	 */
	public function where_or($wheres = [])
	{
		if (is_array($wheres) && count($wheres) > 0)
		{
			if (!isset($this->wheres['$or']) || !is_array($this->wheres['$or']))
			{
				$this->wheres['$or'] = [];
			}

			foreach ($wheres as $wh => $val)
			{
				$this->wheres['$or'][] = [$wh => $val];
			}

			return $this;
		}
		else
		{
			exit('Where value should be an array');
		}
	}

	/**
	 * Where in
	 *
	 * Get the documents where the value of a $field is in a given $in [].
	 *
	 * @usage : $this->mongo_db->where_in('field', array('value1', 'value2', 'value3'))->from('collection')->get()->result();
	 *
	 * @param string $field
	 * @param array  $in
	 *
	 * @return $this
	 */
	public function where_in($field = '', $in = [])
	{
		if (empty($field))
		{
			exit('Mongo field is require to perform where in query');
		}

		if (is_array($in) && count($in) > 0)
		{
			$this->_wheres($field);
			$this->wheres[$field]['$in'] = $in;
			return $this;
		}
		else
		{
			exit('in value should be an array');
		}
	}

	/**
	 * Where in all
	 *
	 * Get the documents where the value of a $field is in all of a given $in array().
	 *
	 * @usage : $this->mongo_db->where_in_all('field', array('value1', 'value2', 'value3'))->from('collection')->get()->result();
	 *
	 * @param string $field
	 * @param array  $in
	 *
	 * @return $this
	 */
	public function where_in_all($field = '', $in = [])
	{
		if (empty($field))
		{
			exit('Mongo field is require to perform where all in query');
		}

		if (is_array($in) && count($in) > 0)
		{
			$this->_wheres($field);
			$this->wheres[$field]['$all'] = $in;
			return $this;
		}
		else
		{
			exit('in value should be an array');
		}
	}

	/**
	 * Where not in
	 *
	 * Get the documents where the value of a $field is not in a given $in array().
	 *
	 * @usage : $this->mongo_db->where_not_in('field', array('value1', 'value2', 'value3'))->from('collection')->get()->result();
	 *
	 * @param string $field
	 * @param array  $in
	 *
	 * @return $this
	 */
	public function where_not_in($field = '', $in = [])
	{
		if (empty($field))
		{
			exit('Mongo field is require to perform where not in query');
		}

		if (is_array($in) && count($in) > 0)
		{
			$this->_wheres($field);
			$this->wheres[$field]['$nin'] = $in;
			return $this;
		}
		else
		{
			exit('in value should be an array');
		}
	}

	/**
	 * Where greater than
	 *
	 * Get the documents where the value of a $field is greater than $value
	 *
	 * @usage : $this->mongo_db->where_gt('field', 'value1')->from('collection')->get()->result();
	 *
	 * @param string $field
	 * @param string $value
	 *
	 * @return $this
	 */
	public function where_gt($field = '', $value = '')
	{
		if (!isset($field))
		{
			exit('Mongo field is require to perform greater then query');
		}

		if (!isset($value))
		{
			exit('Mongo field\'s value is require to perform greater then query');
		}

		$this->_wheres($field);
		$this->wheres[$field]['$gt'] = $value;
		return $this;
	}

	/**
	 * Where greater than or equal to
	 *
	 * Get the documents where the value of a $field is greater than or equal to $value
	 *
	 * @usage : $this->mongo_db->where_gte('field', 'value1')->from('collection')->get()->result();
	 *
	 * @param string $field
	 * @param string $value
	 *
	 * @return $this
	 */
	public function where_gte($field = '', $value = '')
	{
		if (!isset($field))
		{
			exit('Mongo field is require to perform greater then or equal query');
		}

		if (!isset($value))
		{
			exit('Mongo field\'s value is require to perform greater then or equal query');
		}

		$this->_wheres($field);
		$this->wheres[$field]['$gte'] = $value;
		return $this;
	}

	/**
	 * Where less than
	 *
	 * Get the documents where the value of a $field is less than $value
	 *
	 * @usage : $this->mongo_db->where_lt('field', 'value1')->from('collection')->get()->result();
	 *
	 * @param string $field
	 * @param string $value
	 *
	 * @return $this
	 */
	public function where_lt($field = '', $value = '')
	{
		if (!isset($field))
		{
			exit('Mongo field is require to perform less then query');
		}

		if (!isset($value))
		{
			exit('Mongo field\'s value is require to perform less then query');
		}

		$this->_wheres($field);
		$this->wheres[$field]['$lt'] = $value;
		return $this;
	}

	/**
	 * Where less than or equal to
	 *
	 * Get the documents where the value of a $field is less than or equal to $value
	 *
	 * @usage : $this->mongo_db->where_lte('field', 'value1')->from('collection')->get()->result();
	 *
	 * @param string $field
	 * @param string $value
	 *
	 * @return $this
	 */
	public function where_lte($field = '', $value = '')
	{
		if (!isset($field))
		{
			exit('Mongo field is require to perform less then or equal to query');
		}

		if (!isset($value))
		{
			exit('Mongo field\'s value is require to perform less then or equal to query');
		}

		$this->_wheres($field);
		$this->wheres[$field]['$lte'] = $value;
		return $this;
	}

	/**
	 * Where between
	 *
	 * Get the documents where the value of a $field is between $lower and $upper
	 *
	 * @usage : $this->mongo_db->where_between('field', 'value1', 'value2')->from('collection')->get()->result();
	 *
	 * @param string $field
	 * @param string $lower
	 * @param string $upper
	 *
	 * @return $this
	 */
	public function where_between($field = '', $lower = '', $upper = '')
	{
		if (!isset($field))
		{
			exit('Mongo field is require to perform greater then or equal to query');
		}

		if (!isset($lower))
		{
			exit('Mongo field\'s start value is require to perform greater then or equal to query');
		}

		if (!isset($upper))
		{
			exit('Mongo field\'s end value is require to perform greater then or equal to query');
		}

		$this->_wheres($field);
		$this->wheres[$field]['$gte'] = $lower;
		$this->wheres[$field]['$lte'] = $upper;
		return $this;
	}

	/**
	 * Where between and but not equal to
	 *
	 * Get the documents where the value of a $field is between but not equal to $lower and $upper
	 *
	 * @usage : $this->mongo_db->where_between_ne('field', 'value1', 'value2')->from('collection')->get()->result();
	 *
	 * @param string $field
	 * @param string $lower
	 * @param string $upper
	 *
	 * @return $this
	 */
	public function where_between_ne($field = '', $lower = '', $upper = '')
	{
		if (!isset($field))
		{
			exit('Mongo field is require to perform between and but not equal to query');
		}

		if (!isset($lower))
		{
			exit('Mongo field\'s start value is require to perform between and but not equal to query');
		}

		if (!isset($upper))
		{
			exit('Mongo field\'s end value is require to perform between and but not equal to query');
		}

		$this->_wheres($field);
		$this->wheres[$field]['$gt'] = $lower;
		$this->wheres[$field]['$lt'] = $upper;
		return $this;
	}

	/**
	 * Where not equal
	 *
	 * Get the documents where the value of a $field is not equal to $value
	 *
	 * @usage : $this->mongo_db->where_ne('field', 'value1')->from('collection')->get()->result();
	 *
	 * @param string $field
	 * @param string $value
	 *
	 * @return $this
	 */
	public function where_ne($field = '', $value = '')
	{
		if (!isset($field))
		{
			exit('Mongo field is require to perform Where not equal to query');
		}

		if (!isset($value))
		{
			exit('Mongo field\'s value is require to perform Where not equal to query');
		}

		$this->_wheres($field);
		$this->wheres[$field]['$ne'] = $value;
		return $this;
	}

	/**
	 * Like
	 *
	 * Get the documents where the (string) value of a $field is like a value. The defaults allow for a case-insensitive search.
	 *
	 * @param string $field
	 * @param string $value
	 * @param string $flags
	 * Allows for the typical regular expression flags:
	 * i = case insensitive
	 * m = multiline
	 * x = can contain comments
	 * l = locale
	 * s = dot-all, "." matches everything, including newlines
	 * u = match unicode
	 *
	 * @param bool   $enable_start_wildcard
	 * If set to anything other than true, a starting line character "^" will be prepended
	 * to the search value, representing only searching for a value at the start of
	 * a new line.
	 *
	 * @param bool   $enable_end_wildcard
	 * If set to anything other than true, an ending line character "$" will be appended
	 * to the search value, representing only searching for a value at the end of
	 * a line.
	 *
	 * @return $this
	 * @usage : $this->mongo_db->like('foo', 'bar', 'im', false, true)->from('collection')->get()->result();
	 */
	public function like($field = '', $value = '', $flags = "i", $enable_start_wildcard = false, $enable_end_wildcard = false)
	{
		if (empty($field))
		{
			exit('Mongo field is require to perform like query');
		}

		if (empty($value))
		{
			exit('Mongo field\'s value is require to like query');
		}

		$field = (string)trim($field);
		$this->_wheres($field);
		$value = (string)trim($value);
		$value = quotemeta($value);
		if ($enable_start_wildcard)
		{
			$value = "^" . $value;
		}
		if ($enable_end_wildcard)
		{
			$value .= "$";
		}

		$this->_wheres($field);
		$this->wheres[$field]['$regex'] = $value;
		$this->wheres[$field]['$options'] = $flags;

		return $this;
	}

	/**
	 * Get
	 *
	 * Get the documents based upon the passed parameters
	 *
	 * @usage : $this->mongo_db->get('foo')->result();
	 *
	 * @param string $collection
	 *
	 * @return array|object
	 */
	public function get($collection = '')
	{
		if ($collection != '')
		{
			$this->collection = $collection;
		}

		if ($this->projection == '')
		{
			$this->projection = $this->selects;
		}
		return $this;
	}

	/**
	 * From
	 *
	 * @param string $collection
	 *
	 * @return $this
	 */
	public function from($collection = '')
	{
		if (empty($collection))
		{
			exit('In order to retrieve documents from MongoDB, a collection name must be passed');
		}

		$this->collection = $collection;

		return $this;
	}

	/**
	 * Get where
	 *
	 * Get the documents based upon the passed parameters
	 *
	 * @usage : $this->mongo_db->get_where('collection', array('field' => 'value'))->result();
	 *
	 * @param string $collection
	 * @param array  $where
	 *
	 * @return array|object
	 */
	public function get_where($collection = '', $where = [])
	{
		if (is_array($where) && count($where) > 0)
		{
			$this->where($where);
			$this->get($collection);
			return $this;
		}
		else
		{
			exit('Nothing passed to perform search or value is empty');
		}
	}

	/**
	 * Count
	 *
	 * Count the documents based upon the passed parameters
	 *
	 * @usage : $this->mongo_db->count('foo');
	 */
	public function count()
	{
		$this->projection = ['_id' => 1];
		$this->count = true;
		return $this;
	}

	/**
	 * Count All Results
	 *
	 * Alias to count method for compatibility with CI Query Builder
	 *
	 * @usage : $this->mongo_db->count('foo');
	 *
	 * @param string $collection
	 *
	 * @return array|object
	 */
	public function count_all_results($collection = '')
	{
		$this->count();
		$this->get($collection);
		return $this;
	}

	/**
	 * Set
	 *
	 * Sets a field to a value
	 *
	 * @usage: $this->mongo_db->where(array('field'=>'value'))->set('field', 'value')->update('collection');
	 *
	 * @param string|array $fields
	 * @param null|string  $value
	 *
	 * @return $this
	 */
	public function set($fields, $value = null)
	{
		$this->_updates('$set');
		if (is_string($fields))
		{
			$this->updates['$set'][$fields] = $value;
		}
		elseif (is_array($fields))
		{
			foreach ($fields as $field => $value)
			{
				$this->updates['$set'][$field] = $value;
			}
		}
		return $this;
	}

	/**
	 * Unset
	 *
	 * Unset's a field (or fields)
	 *
	 * @usage: $this->mongo_db->where(array('field'=>'value'))->unset_field('field')->update('collection');
	 *
	 * @param string|array $fields
	 *
	 * @return $this
	 */
	public function unset_field($fields)
	{
		$this->_updates('$unset');
		if (is_string($fields))
		{
			$this->updates['$unset'][$fields] = 1;
		}
		elseif (is_array($fields))
		{
			foreach ($fields as $field)
			{
				$this->updates['$unset'][$field] = 1;
			}
		}
		return $this;
	}

	/**
	 * Add to set
	 *
	 * Adds value to the array only if its not in the array already
	 *
	 * @usage: $this->mongo_db->where(array('field'=>'value'))->add_to_set('field', 'value')->update('collection');
	 *
	 * @param string       $field
	 * @param string|array $values
	 *
	 * @return $this
	 */
	public function add_to_set($field, $values)
	{
		$this->_updates('$addToSet');
		if (is_string($values))
		{
			$this->updates['$addToSet'][$field] = $values;
		}
		elseif (is_array($values))
		{
			$this->updates['$addToSet'][$field] = ['$each' => $values];
		}
		return $this;
	}

	/**
	 * Push
	 *
	 * Pushes values into a field (field must be an array)
	 *
	 * @usage: $this->mongo_db->where(array('field' => 'value'))->push('field2', array('field'=>'value'))->update('collection');
	 *
	 * @param string|array $fields
	 * @param array        $value
	 *
	 * @return $this
	 */
	public function push($fields, $value = [])
	{
		$this->_updates('$push');
		if (is_string($fields))
		{
			$this->updates['$push'][$fields] = $value;
		}
		elseif (is_array($fields))
		{
			foreach ($fields as $field => $value)
			{
				$this->updates['$push'][$field] = $value;
			}
		}
		return $this;
	}

	/**
	 * Pop
	 *
	 * Pops the last value from a field (field must be an array)
	 *
	 * @usage: $this->mongo_db->where(array('field' => 'value'))->pop('field2')->update('collection');
	 *
	 * @param string|array $field
	 *
	 * @return $this
	 */
	public function pop($field)
	{
		$this->_updates('$pop');
		if (is_string($field))
		{
			$this->updates['$pop'][$field] = -1;
		}
		elseif (is_array($field))
		{
			foreach ($field as $pop_field)
			{
				$this->updates['$pop'][$pop_field] = -1;
			}
		}
		return $this;
	}

	/**
	 * Pull
	 *
	 * Removes by an array by the value of a field
	 *
	 * @usage: $this->mongo_db->pull('field', array('field2' => 'value'))->update('collection');
	 *
	 * @param string $field
	 * @param array  $value
	 *
	 * @return $this
	 */
	public function pull($field = '', $value = [])
	{
		$this->_updates('$pull');
		$this->updates['$pull'] = [$field => $value];
		return $this;
	}

	/**
	 * Rename field
	 *
	 * Renames a field
	 *
	 * @usage: $this->mongo_db->where(array('field' => 'value'))->rename_field('field2', 'field3')->update('collection');
	 *
	 * @param $old
	 * @param $new
	 *
	 * @return $this
	 */
	public function rename_field($old, $new)
	{
		$this->_updates('$rename');
		$this->updates['$rename'] = [$old => $new];
		return $this;
	}

	/**
	 * Increment
	 *
	 * Increments the value of a field
	 *
	 * @usage: $this->mongo_db->where(array('field' => 'value'))->increment(array('field' => 1))->update('collection');
	 *
	 * @param array $fields
	 * @param int   $value
	 *
	 * @return $this
	 */
	public function increment($fields = [], $value = 0)
	{
		$this->_updates('$inc');
		if (is_string($fields))
		{
			$this->updates['$inc'][$fields] = $value;
		}
		elseif (is_array($fields))
		{
			foreach ($fields as $field => $value)
			{
				$this->updates['$inc'][$field] = $value;
			}
		}
		return $this;
	}

	/**
	 * Multiple
	 *
	 * Multiple the value of a field
	 *
	 * @usage: $this->mongo_db->where(array('field' => 'value'))->mul(array('field' => 3))->update('collection');
	 *
	 * @param array $fields
	 * @param int   $value
	 *
	 * @return $this
	 */
	public function multiple($fields = [], $value = 0)
	{
		$this->_updates('$mul');
		if (is_string($fields))
		{
			$this->updates['$mul'][$fields] = $value;
		}
		elseif (is_array($fields))
		{
			foreach ($fields as $field => $value)
			{
				$this->updates['$mul'][$field] = $value;
			}
		}
		return $this;
	}

	/**
	 * Maximum
	 *
	 * The $max operator updates the value of the field to a specified value if the specified value is greater than the current value of the field.
	 *
	 * @usage: $this->mongo_db->where(array('field' => 'value'))->max(array('field' => 3))->update('collection');
	 *
	 * @param array $fields
	 * @param int   $value
	 *
	 * @return $this
	 */
	public function max($fields = [], $value = 0)
	{
		$this->_updates('$max');
		if (is_string($fields))
		{
			$this->updates['$max'][$fields] = $value;
		}
		elseif (is_array($fields))
		{
			foreach ($fields as $field => $value)
			{
				$this->updates['$max'][$field] = $value;
			}
		}
		return $this;
	}

	/**
	 * Minimum
	 *
	 * The $min updates the value of the field to a specified value if the specified value is less than the current value of the field.
	 *
	 * @usage: $this->mongo_db->where(array('field' => 'value'))->min(array('field' => 3))->update('collection');
	 *
	 * @param array $fields
	 * @param int   $value
	 *
	 * @return $this
	 */
	public function min($fields = [], $value = 0)
	{
		$this->_updates('$min');
		if (is_string($fields))
		{
			$this->updates['$min'][$fields] = $value;
		}
		elseif (is_array($fields))
		{
			foreach ($fields as $field => $value)
			{
				$this->updates['$min'][$field] = $value;
			}
		}
		return $this;
	}

	/**
	 * Distinct
	 *
	 * Finds the distinct values for a specified field across a single collection
	 *
	 * @usage: $this->mongo_db->distinct('field')->from('collection')->get()->row();
	 *
	 * @param string $field
	 *
	 * @return $this
	 */
	public function distinct($field = '')
	{
		if (empty($field))
		{
			exit('Need Collection field information for performing distinct query');
		}

		$this->distinct = true;
		$this->fields = $field;

		return $this;
	}

	/**
	 * Update
	 *
	 * Updates a single document in Mongo
	 *
	 * @usage: $this->mongo_db->update('collection', $data = []);
	 *
	 * @param string $collection
	 * @param array  $options
	 *
	 * @return \MongoDB\Driver\WriteResult
	 */
	public function update($collection = '', $options = [])
	{
		if (empty($collection))
		{
			exit('No Mongo collection selected for update');
		}

		$bulk = new MongoDB\Driver\BulkWrite();
		$bulk->update($this->wheres, $this->updates, $options);

		$writeConcern = new MongoDB\Driver\WriteConcern(MongoDB\Driver\WriteConcern::MAJORITY, 1000);

		try
		{
			$write = $this->db->executeBulkWrite($this->database . "." . $collection, $bulk, $writeConcern);
			$this->_clear();
			return $write;
		}
		catch (MongoDB\Driver\Exception\BulkWriteException $e)
		{
			$result = $e->getWriteResult();

			if ($writeConcernError = $result->getWriteConcernError())
			{
				if (isset($this->debug) == true && $this->debug == true)
				{
					exit('WriteConcern failure : ' . $writeConcernError->getMessage());
				}
				else
				{
					exit('WriteConcern failure');
				}
			}
		}
		catch (MongoDB\Driver\Exception\Exception $e)
		{
			if (isset($this->debug) == true && $this->debug == true)
			{
				exit('Update of data into MongoDB failed: ' . $e->getMessage());
			}
			else
			{
				exit('Update of data into MongoDB failed');
			}
		}
	}

	/**
	 * Update all
	 *
	 * Updates a collection of documents
	 *
	 * @usage: $this->mongo_db->update_all('collection', $data = []);
	 *
	 * @param string $collection
	 * @param array  $data
	 * @param array  $options
	 *
	 * @return \MongoDB\Driver\WriteResult
	 */
	public function update_all($collection = '', $data = [], $options = [])
	{
		if (empty($collection))
		{
			exit('No Mongo collection selected to update');
		}
		if (is_array($data) && count($data) > 0)
		{
			$this->updates = array_merge($data, $this->updates);
		}
		if (count($this->updates) == 0)
		{
			exit('Nothing to update in Mongo collection or update is not an array');
		}

		$options = array_merge(['multi' => true], $options);

		$bulk = new MongoDB\Driver\BulkWrite();
		$bulk->update($this->wheres, $this->updates, $options);

		$writeConcern = new MongoDB\Driver\WriteConcern(MongoDB\Driver\WriteConcern::MAJORITY, 1000);

		try
		{
			$write = $this->db->executeBulkWrite($this->database . "." . $collection, $bulk, $writeConcern);
			$this->_clear();
			return $write;
		}
		catch (MongoDB\Driver\Exception\BulkWriteException $e)
		{
			$result = $e->getWriteResult();

			if ($writeConcernError = $result->getWriteConcernError())
			{
				if (isset($this->debug) == true && $this->debug == true)
				{
					exit('WriteConcern failure : ' . $writeConcernError->getMessage());
				}
				else
				{
					exit('WriteConcern failure');
				}
			}
		}
		catch (MongoDB\Driver\Exception\Exception $e)
		{
			if (isset($this->debug) == true && $this->debug == true)
			{
				exit('Update of data into MongoDB failed: ' . $e->getMessage());
			}
			else
			{
				exit('Update of data into MongoDB failed');
			}
		}
	}

	/**
	 * Delete
	 *
	 * delete document from the passed collection based upon certain criteria
	 *
	 * @usage : $this->mongo_db->delete('collection');
	 *
	 * @param string $collection
	 *
	 * @return \MongoDB\Driver\WriteResult
	 */
	public function delete($collection = '')
	{
		if (empty($collection))
		{
			exit('No Mongo collection selected for update');
		}

		$options = ['limit' => true];
		$bulk = new MongoDB\Driver\BulkWrite();
		$bulk->delete($this->wheres, $options);

		$writeConcern = new MongoDB\Driver\WriteConcern(MongoDB\Driver\WriteConcern::MAJORITY, 1000);

		try
		{
			$write = $this->db->executeBulkWrite($this->database . "." . $collection, $bulk, $writeConcern);
			$this->_clear();
			return $write;
		}
		catch (MongoDB\Driver\Exception\BulkWriteException $e)
		{
			$result = $e->getWriteResult();

			if ($writeConcernError = $result->getWriteConcernError())
			{
				if (isset($this->debug) == true && $this->debug == true)
				{
					exit('WriteConcern failure : ' . $writeConcernError->getMessage());
				}
				else
				{
					exit('WriteConcern failure');
				}
			}
		}
		catch (MongoDB\Driver\Exception\Exception $e)
		{
			if (isset($this->debug) == true && $this->debug == true)
			{
				exit('Update of data into MongoDB failed: ' . $e->getMessage());
			}
			else
			{
				exit('Update of data into MongoDB failed');
			}
		}
	}

	/**
	 * Delete all
	 *
	 * Delete all documents from the passed collection based upon certain criteria
	 *
	 * @usage : $this->mongo_db->delete_all('collection', $data = []);
	 *
	 * @param string $collection
	 *
	 * @return \MongoDB\Driver\WriteResult
	 */
	public function delete_all($collection = '')
	{
		if (empty($collection))
		{
			exit('No Mongo collection selected for delete');
		}

		$options = ['limit' => false];
		$bulk = new MongoDB\Driver\BulkWrite();
		$bulk->delete($this->wheres, $options);

		$writeConcern = new MongoDB\Driver\WriteConcern(MongoDB\Driver\WriteConcern::MAJORITY, 1000);

		try
		{
			$write = $this->db->executeBulkWrite($this->database . "." . $collection, $bulk, $writeConcern);
			$this->_clear();
			return $write;
		}
		catch (MongoDB\Driver\Exception\BulkWriteException $e)
		{
			$result = $e->getWriteResult();

			if ($writeConcernError = $result->getWriteConcernError())
			{
				if (isset($this->debug) == true && $this->debug == true)
				{
					exit('WriteConcern failure : ' . $writeConcernError->getMessage());
				}
				else
				{
					exit('WriteConcern failure');
				}
			}
		}
		catch (MongoDB\Driver\Exception\Exception $e)
		{
			if (isset($this->debug) == true && $this->debug == true)
			{
				exit('Delete of data into MongoDB failed: ' . $e->getMessage());
			}
			else
			{
				exit('Delete of data into MongoDB failed');
			}
		}
	}

	/**
	 * Aggregation Operation
	 *
	 * Perform aggregation on mongodb collection
	 *
	 * @usage : $this->mongo_db->aggregate('collection', $ops = []);
	 *
	 * @param $collection
	 * @param $operation
	 *
	 * @return object
	 */
	public function aggregate($collection, $operation)
	{
		if (empty($collection))
		{
			exit('In order to retrieve documents from MongoDB, a collection name must be passed');
		}

		if (empty($operation) && !is_array($operation))
		{
			exit('Operation must be an array to perform aggregate');
		}

		$command = ['aggregate' => $collection, 'pipeline' => $operation, 'cursor' => new stdClass];
		return $this->command($command);
	}

	/**
	 * Order by
	 *
	 * Sort the documents based on the parameters passed. To set values to descending order,
	 * you must pass values of either -1, false, 'desc', or 'DESC', else they will be
	 * set to 1 (ASC).
	 *
	 * @usage : $this->mongo_db->order_by(array('field' => 'ASC'))->get('collection');
	 *
	 * @param array $fields
	 *
	 * @return $this
	 */
	public function order_by($fields = [])
	{
		foreach ($fields as $col => $val)
		{
			if ($val == -1 || $val === false || strtolower($val) == 'desc')
			{
				$this->sorts[$col] = -1;
			}
			else
			{
				$this->sorts[$col] = 1;
			}
		}
		return $this;
	}

	/**
	 * Mongo timestamp
	 *
	 * Create new MongoDate object from current time or pass timestamp to create mongodb.
	 *
	 * @usage : $this->mongo_db->timestamp();
	 *
	 * @param bool $stamp
	 * @param bool $timezone
	 *
	 * @return array
	 */
	public function timestamp($stamp = false, $timezone = false)
	{
		if ($stamp == false)
		{
			$timestamp = new MongoDB\BSON\UTCDateTime();
		}
		else
		{
			$timestamp = new MongoDB\BSON\UTCDateTime($stamp);
		}

		$datetime = $timestamp->toDateTime();

		if ($timezone !== false)
		{
			$datetime->setTimeZone(new DateTimeZone($timezone));
		}

		$datetime->timestamp = $datetime->format('U.u');

		return (array)$datetime;
	}

	/**
	 * Limit results
	 *
	 * Limit the result set to $result_count number of documents
	 *
	 * @usage : $this->mongo_db->limit($result_count);
	 *
	 * @param int $result_count
	 *
	 * @return $this
	 */
	public function limit($result_count = 99999)
	{
		if ($result_count !== null && is_numeric($result_count) && $result_count >= 1)
		{
			$this->limit = (int)$result_count;
		}
		return $this;
	}

	/**
	 * Offset
	 *
	 * Offset the result set to skip $result_count number of documents
	 *
	 * @usage : $this->mongo_db->offset($result_count);
	 *
	 * @param int $result_count
	 *
	 * @return $this
	 */
	public function offset($result_count = 0)
	{
		if ($result_count !== null && is_numeric($result_count) && $result_count >= 1)
		{
			$this->offset = (int)$result_count;
		}
		return $this;
	}

	/**
	 * Command
	 *
	 * Runs a MongoDB command
	 *
	 * @param array $command
	 * @param string : Collection name, array $query The command query
	 *
	 * @return object or array
	 * @usage  : $this->mongo_db->command({array});
	 * @access public
	 */
	public function command($command = [], $return_type = 'array')
	{
		try
		{
			$cursor = $this->db->executeCommand($this->database, new MongoDB\Driver\Command($command));

			return $this->_parser($cursor, $return_type);

		}
		catch (MongoDB\Driver\Exception\Exception $e)
		{
			if (isset($this->debug) == true && $this->debug == true)
			{
				exit('MongoDB query failed: ' . $e->getMessage());
			}
			else
			{
				exit('MongoDB query failed');
			}
		}
	}

	/**
	 * Add indexes
	 *
	 * Ensure an index of the keys in a collection with optional parameters. To set values to descending order,
	 * you must pass values of either -1, false, 'desc', or 'DESC', else they will be
	 * set to 1 (ASC).
	 *
	 * @usage : $this->mongo_db->add_index('collection', array('field' => 'ASC', 'field2' => -1), array('unique' => true));
	 *
	 * @param string $collection
	 * @param array  $keys
	 * @param array  $options
	 *
	 * @return object
	 */
	public function add_index($collection = '', $keys = [], $options = [])
	{
		if (empty($collection))
		{
			exit('No Mongo collection specified to add index to');
		}

		if (empty($keys) || !is_array($keys))
		{
			exit('Index could not be created to MongoDB Collection because no keys were specified');
		}

		foreach ($keys as $col => $val)
		{
			if ($val == -1 || $val === false || strtolower($val) == 'desc')
			{
				$keys[$col] = -1;
			}
			else
			{
				$keys[$col] = 1;
			}
		}
		$command = [];
		$command['createIndexes'] = $collection;
		$command['indexes'] = [$keys];

		return $this->command($command);
	}

	/**
	 * Remove index
	 *
	 * Remove an index of the keys in a collection. To set values to descending order,
	 * you must pass values of either -1, false, 'desc', or 'DESC', else they will be
	 * set to 1 (ASC).
	 *
	 * @usage : $this->mongo_db->remove_index('collection', 'index_1');
	 *
	 * @param string $collection
	 * @param string $name
	 *
	 * @return object
	 */
	public function remove_index($collection = '', $name = '')
	{
		if (empty($collection))
		{
			exit('No Mongo collection specified to remove index from');
		}

		if (empty($keys))
		{
			exit('Index could not be removed from MongoDB Collection because no index name were specified');
		}

		$command = [];
		$command['dropIndexes'] = $collection;
		$command['index'] = $name;

		return $this->command($command);
	}

	/**
	 * List indexes
	 *
	 * Lists all indexes in a collection.
	 *
	 * @usage : $this->mongo_db->list_indexes('collection');
	 *
	 * @param string $collection
	 *
	 * @return object
	 */
	public function list_indexes($collection = '')
	{
		if (empty($collection))
		{
			exit('No Mongo collection specified to list all indexes from');
		}
		$command = [];
		$command['listIndexes'] = $collection;

		return $this->command($command);
	}

	/**
	 * Drop database
	 *
	 * Drop a Mongo database
	 *
	 * @usage: $this->mongo_db->drop_db('database_name');
	 *
	 * @param string $database
	 *
	 * @return object
	 */
	public function drop_db($database = '')
	{
		if (empty($database))
		{
			exit('Failed to drop MongoDB database because name is empty');
		}

		$command = [];
		$command['dropDatabase'] = 1;

		return $this->command($command);
	}

	/**
	 * Drop collection
	 *
	 * Drop a Mongo collection
	 *
	 * @usage: $this->mongo_db->drop_collection('collection');
	 *
	 * @param string $col
	 *
	 * @return object
	 */
	public function drop_collection($col = '')
	{
		if (empty($col))
		{
			exit('Failed to drop MongoDB collection because collection name is empty');
		}

		$command = [];
		$command['drop'] = $col;

		return $this->command($command);
	}

	/**
	 * Row
	 *
	 * @param string $type
	 *
	 * @return array|object
	 */
	public function row($type = 'array')
	{
		$this->limit = 1;
		$data = $this->_read_object($type);

		return (!empty($data) ? (is_array($data) ? current($data) : $data) : []);
	}

	/**
	 * Result
	 *
	 * @param string $type
	 *
	 * @return array|object
	 */
	public function result($type = 'array')
	{
		$data = $this->_read_object($type);

		return (!empty($data) ? $data : []);
	}

	/**
	 * PRIVATE METHODS
	 */

	/**
	 * Parser
	 *
	 * @param $cursor
	 * @param $return_type
	 *
	 * @return array|object
	 */
	private function _parser($cursor, $return_type)
	{
		$returns = [];

		if ($cursor instanceof MongoDB\Driver\Cursor)
		{
			$it = new IteratorIterator($cursor);
			$it->rewind();

			while ($doc = (array)$it->current())
			{
				if ($return_type == 'object')
				{
					$returns[] = (object)$this->_convert_document_id($doc);
				}
				else
				{
					$returns[] = (array)$this->_convert_document_id($doc);
				}
				$it->next();
			}
		}

		if ($this->count)
		{
			$output = count($returns);
		}
		elseif ($return_type == 'object')
		{
			$output = (object)$returns;
		}
		else
		{
			$output = $returns;
		}

		$this->_clear();

		return $output;
	}

	/**
	 * Read Object
	 *
	 * @param $return_type
	 *
	 * @return array|object
	 */
	private function _read_object($return_type)
	{
		if (empty($this->collection))
		{
			exit('In order to retrieve documents from MongoDB, a collection name must be passed');
		}

		try
		{
			if ($this->distinct)
			{
				$cmd = new MongoDB\Driver\Command([
					'distinct' => $this->collection, // specify the collection name
					'key' => $this->fields, // specify the field for which we want to get the distinct values
					'query' => (object)$this->wheres // criteria to filter documents
				]);
				$cursor = $this->db->executeCommand($this->database, $cmd); // retrieve the results

				return current($cursor->toArray())->values;

			}

			$read_concern = new MongoDB\Driver\ReadConcern($this->read_concern);
			$read_preference = new MongoDB\Driver\ReadPreference($this->read_preference);

			$options = [];
			$options['projection'] = $this->projection;
			$options['sort'] = $this->sorts;
			$options['skip'] = (int)$this->offset;
			$options['limit'] = (int)$this->limit;
			$options['readConcern'] = $read_concern;

			$query = new MongoDB\Driver\Query($this->wheres, $options);

			$cursor = $this->db->executeQuery($this->database . "." . $this->collection, $query, $read_preference);

			return $this->_parser($cursor, $return_type);
		}
		catch (MongoDB\Driver\Exception\Exception $e)
		{
			if (isset($this->debug) == true && $this->debug == true)
			{
				exit('MongoDB query failed: ' . $e->getMessage());
			}
			else
			{
				exit('MongoDB query failed');
			}
		}
	}

	/**
	 * _clear
	 *
	 * Resets the class variables to default settings
	 */
	private function _clear()
	{
		$this->selects = [];
		$this->updates = [];
		$this->wheres = [];
		$this->limit = 999999;
		$this->offset = 0;
		$this->sorts = [];
		$this->distinct = false;
		$this->count = false;
	}

	/**
	 * Where initializer
	 *
	 * Prepares parameters for insertion in $wheres array().
	 *
	 * @param $param
	 */
	private function _wheres($param)
	{
		if (!isset($this->wheres[$param]))
		{
			$this->wheres[$param] = [];
		}
	}

	/**
	 * Update initializer
	 *
	 * Prepares parameters for insertion in $updates array().
	 *
	 * @param $method
	 */
	private function _updates($method)
	{
		if (!isset($this->updates[$method]))
		{
			$this->updates[$method] = [];
		}
	}

	/**
	 * Convert Document ID
	 *
	 * Converts document ID and returns document back.
	 *
	 * @param array $document
	 *
	 * @return array
	 */
	private function _convert_document_id($document)
	{
		if (($this->legacy_support === true) && (isset($document['_id'])) && ($document['_id'] instanceof MongoDB\BSON\ObjectId))
		{
			$new_id = $document['_id']->__toString();
			unset($document['_id']);
			$document['_id'] = new \stdClass();
			$document['_id']->{'$id'} = $new_id;
		}
		return $document;
	}
}
