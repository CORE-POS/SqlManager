<?php

namespace COREPOS;

/**
  @class SqlManagerCommonBase
  Base class for classes implemnting the SqlManagerInterface

  Provides all the alias method names so proper implementations
  can simply use the single camelCase method
*/
class SqlManagerCommonBase 
{
    protected $QUERY_LOG;

    /** Array of connections **/
    public $connections;
    /** Default database connection */
    public $default_db;

    /** throw exception on failed query **/
    protected $throw_on_fail = false;

    /** utility methods **/
    public function whichConnection($which_connection)
    {
        $which = $which_connection == '' ? $this->default_db : $which_connection;
        if (isset($this->connections[$which])) {
            return $this->connections[$which];
        } else {
            return false;
        }
    }

    /** non-camelCase alias methods **/
    public function addConnection($server,$type,$database,$username,$password='',$persistent=false,$new=false) { }
    public function add_connection($server,$type,$database,$username,$password='',$persistent=false, $new=false)
    {
        return $this->addConnection($server, $type, $database, $username, $password, $persistent, $new);
    }

    /**
      Execute a query on all connected databases
      @param $query_text The query
      @return An array keyed by database name. Entries
      will be result objects where queries succeeded
      and False where they failed
    */
    public function queryAll($query_text)
    {
        return false;
    }
    public function query_all($query_text)
    {
        return $this->queryAll($query_text);
    }

    public function identifierEscape($str,$which_connection='')
    {
        return false;
    }
    public function identifier_escape($str,$which_connection='')
    {
        return $this->identifierEscape($str, $which_connection);
    }

    /**
      Get number of rows in a result set
      @param $result_object A result set
      @param $which_connection see method close()
      @return Integer number or False if there's an error
    */
    public function numRows($result_object,$which_connection='')
    {
        return false;
    }
    public function num_rows($result_object,$which_connection='')
    {
        return $this->numRows($result_object, $which_connection);
    }
    
    /**
      Move result cursor to specified record
      @param $result_object A result set
      @param $rownum The record index
      @param $which_connection see method close()
      @return True on success, False on failure
    */
    public function dataSeek($result_object,$rownum,$which_connection='')
    {
        return false;
    }
    public function data_seek($result_object,$rownum,$which_connection='')
    {
        return $this->dataSeek($result_object, $rownum, $which_connection);
    }

    /**
      Get number of fields in a result set
      @param $result_object A result set
      @param $which_connection see method close()
      @return Integer number or False if there's an error
    */
    public function numFields($result_object,$which_connection='')
    {
        return false;
    }
    public function num_fields($result_object,$which_connection='')
    {
        return $this->numFields($result_object, $which_connection);
    }

    /**
      Get next record from a result set
      @param $result_object A result set
      @param $which_connection see method close()
      @return An array of values
    */
    public function fetchArray($result_object,$which_connection='')
    {
        return false;
    }
    public function fetch_array($result_object,$which_connection='')
    {
        return $this->fetchArray($result_object, $which_connection);
    }
    public function fetchRow($result_object,$which_connection='')
    {
        return $this->fetchArray($result_object, $which_connection);
    }
    public function fetch_row($result_object,$which_connection='')
    {
        return $this->fetchArray($result_object, $which_connection);
    }

    /**
      Get next record from a result set but as an object
      @param $result_object A result set
      @param $which_connection see method close()
      @return An object with member containing values
    */
    public function fetchObject($result_object,$which_connection='')
    {
        return false;
    }
    public function fetch_object($result_object,$which_connection='')
    {
        return $this->fetchObject($result_object, $which_connection);
    }

    /**
      Get a column name by index
      @param $result_object A result set
      @param $index Integer index
      @param $which_connection see method close()
      @return The column name
    */
    public function fetchField($result_object,$index,$which_connection='')
    {
        return false;
    }
    public function fetch_field($result_object,$index,$which_connection='')
    {
        return $this->fetchField($result_object, $index, $which_connection);
    }

    /**
      Get column type
      @param $result_object A result set
      @param $index Integer index
      @param $which_connection see method close()
      @return The column type
    */
    public function fieldType($result_object,$index,$which_connection='')
    {
        return false;
    }
    public function field_type($result_object,$index,$which_connection='')
    {
        return $this->fieldType($reuslt_object, $index, $which_connection);
    }
    public function field_name($result_object,$index,$which_connection='')
    {
        return $this->fieldType($reuslt_object, $index, $which_connection);
    }

    /**
       Check whether the given table exists
       @param $table_name The table's name
       @param which_connection see method close
       @return
        - True The table exists
        - False The table doesn't exist
        - -1 Operation not supported for this database type
    */
    public function tableExists($table_name,$which_connection='')
    {
        return false;
    }
    public function table_exists($table_name,$which_connection='')
    {
        return $this->tableExists($table_name, $which_connection);
    }

    /**
       Get the table's definition
       @param $table_name The table's name
       @param which_connection see method close
       @return
        - Array of (column name, column type) table found
        - False No such table
        - -1 Operation not supported for this database type
    */
    public function tableDefinition($table_name,$which_connection='')
    {
        return false;
    }
    public function table_definition($table_name,$which_connection='')
    {
        return $this->tableDefinition($table_name, $which_connection);
    }

    /**
       Get list of tables/views
       @param which_connection see method close
    */
    public function getTables($which_connection='')
    {
        return false;
    }
    public function get_tables($which_connection='')
    {
        return $this->getTables($which_connection);
    }

    /**
      Add row limit to a select query
      @param $query The select query
      @param $int_limit Max rows
      @param which_connection see method close

      This method currently only suport MySQL and MSSQL
    */
    public function addSelectLimit($query,$int_limit,$which_connection='')
    {
        return false;
    }
    public function add_select_limit($query,$int_limit,$which_connection='')
    {
        return $this->addSelectLimit($query, $int_limit, $which_connection);
    }

    /**
      Get name of database driver
      @param which_connection see method close
      @return String name
    */
    public function dbmsName($which_connection='')
    {
        return false;
    }
    public function dbms_name($which_connection='')
    {
        return $this->dbmsName($which_connection);
    }

    /**
      Get auto incremented ID from last insert
      @param which_connection see method close
      @return The new ID value
    */
    public function insertID($which_connection='')
    {
        return false;
    }
    public function insert_id($which_connection='')
    {
        return $this->insertID($which_connection);
    }

    /**
      Check how many rows the last query affected
      @param which_connection see method close
      @returns Number of rows
    */
    public function affectedRows($which_connection='')
    {
        return false;
    }
    public function affected_rows($which_connection='')
    {
        return $this->affectedRows($which_connection);
    }

    /**
      Insert as much data as possible
      @param $table_name Table to insert into
      @param $values An array of column name => column value
      @param which_connection see method close
      @return Same as INSERT via query() method

      This method polls the table to see which columns actually
      exist then inserts those values
     */
    public function smartInsert($table_name,$values,$which_connection='')
    {
        return false;
    }
    public function smart_insert($table_name,$values,$which_connection='')
    {
        return $this->smartInsert($table_name, $values, $which_connection);
    }

    /**
      Update as much data as possible
      @param $table_name The table to update
      @param $values An array of column name => column value
      @param $where_clause The query WHERE clause
      @param which_connection see method close
      @return Same as an UPDATE via query() method
      
      This method checks which columns actually exist then
      updates those values

      Caveat: There are a couple places this could break down
       - If your WHERE clause requires a column that doesn't exist,
         the query will fail. No way around it. Auto-modifying
         WHERE clauses seems like a terrible idea
       - This only works with a single table. Updates involving joins
         are rare in the code base though.
     */
    public function smartUpdate($table_name,$values,$where_clause,$which_connection='')
    {
        return false;
    }
    public function smart_update($table_name,$values,$where_clause,$which_connection='')
    {
        return $this->smartUpdate($table_name, $values, $where_clause, $which_connection);
    }

    /**
      Create a prepared statement
      @param $sql SQL expression
      @param which_connection see method close
      @return
        - If ADOdb supports prepared statements, an
          array of (input string $sql, statement object)
        - If ADOdb does not supported prepared statements,
          then just the input string $sql

      The return value of this function should be handed
      to SQLManager::exec_statement for execution
    */
    public function prepare($sql,$which_connection="")
    {
        return false;
    }
    public function prepare_statement($sql,$which_connection="")
    {
        return $this->prepare($sql, $which_connection);
    }

    /**
      Execute a prepared statement with the given
      set of parameters
      @param $sql a value from SQLManager::prepare_statement
      @param $input_array an array of values
      @param which_connection see method close
      @return same as SQLManager::query

      This is essentially a helper function to flip the
      parameter order on SQLManager::query so existing code
      works as expected
    */
    public function execute($sql, $input_array=array(), $which_connection='')
    {
        return false;
    }
    public function exec_statement($sql, $input_array=array(), $which_connection='')
    {
        return $this->execute($sql, $input_array, $which_connection);
    }

    /**
      See if a datetime is on a given date using BETWEEN
      @param $col datetime expression
      @param $dateStr String date
      @return SQL BETWEEN comparision

      Which MySQL partitioning by date this is MUCH
      faster than using datediff($col,$dateStr)==0
    */
    public function dateEquals($col,$dateStr)
    {
        return false;
    }
    public function date_equals($col,$dateStr)
    {
        return $this->dateEquals($col, $dateStr);
    }
}

