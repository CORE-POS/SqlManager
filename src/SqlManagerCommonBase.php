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

    /**
      Enable or disable exceptions on failed queries
      @param $mode boolean
    */
    public function throwOnFailure($mode)
    {
        $this->throw_on_fail = $mode;
    }

    /**
       Copy a table from one database to another, not necessarily on
       the same server or format.
    
       @param $source_db The database name of the source
       @param $select_query The query that will get the data
       @param $dest_db The database name of the destination
       @param $insert_query The beginning of the query that will add the
        data to the destination (specify everything before VALUES)
       @return False if any record cannot be transfered, True otherwise
    */
    public function transfer($source_db,$select_query,$dest_db,$insert_query)
    {
        $result = $this->query($select_query,$source_db);
        if (!$result) {
            return false;
        }

        $num_fields = $this->numFields($result,$source_db);

        $unquoted = array("money"=>1,"real"=>1,"numeric"=>1,
            "float4"=>1,"float8"=>1,"bit"=>1);
        $strings = array("varchar"=>1,"nvarchar"=>1,"string"=>1,
            "char"=>1);
        $dates = array("datetime"=>1);
        $queries = array();

        while($row = $this->fetchArray($result,$source_db)) {
            $full_query = $insert_query." VALUES (";
            for ($i=0; $i<$num_fields; $i++) {
                $type = $this->fieldType($result,$i,$source_db);
                if ($row[$i] == "" && strstr(strtoupper($type),"INT")) {
                    $row[$i] = 0;
                } elseif ($row[$i] == "" && isset($unquoted[$type])) {
                    $row[$i] = 0;
                }
                if (isset($dates[$type])) {
                    $row[$i] = $this->cleanDateTime($row[$i]);
                } elseif (isset($strings[$type])) {
                    $row[$i] = str_replace("'","''",$row[$i]);
                }
                if (isset($unquoted[$type])) {
                    $full_query .= $row[$i].",";
                } else {
                    $full_query .= "'".$row[$i]."',";
                }
            }
            $full_query = substr($full_query,0,strlen($full_query)-1).")";
            array_push($queries,$full_query);
        }

        $ret = true;
        $this->startTransaction($dest_db);
        foreach ($queries as $q) {
            if(!$this->query($q,$dest_db)) {
                $ret = false;
            }
        }
        if ($ret === true) {
            $this->commitTransaction($dest_db);
        } else {
            $this->rollbackTransaction($dest_db);
        }

        return $ret;
    }

    /**
      Reformat a datetime to YYYY-MM-DD HH:MM:SS
      @param $str A datetime string
      @return The reformatted string

      This is a utility method to support transfer()
    */
    public function cleanDateTime($str)
    {
        $stdFmt = "/(\d\d\d\d)-(\d\d)-(\d\d) (\d+?):(\d\d):(\d\d)/";
        if (preg_match($stdFmt,$str,$group)) {
            return $str;
        }

        $msqlFmt = "/(\w\w\w) (\d\d) (\d\d\d\d) (\d+?):(\d\d)(\w)M/";

        $months = array(
            "jan"=>"01",
            "feb"=>"02",
            "mar"=>"03",
            "apr"=>"04",
            "may"=>"05",
            "jun"=>"06",
            "jul"=>"07",
            "aug"=>"08",
            "sep"=>"09",
            "oct"=>"10",
            "nov"=>"11",
            "dec"=>"12"
        );

        $info = array(
            "month" => 1,
            "day" => 1,
            "year" => 1900,
            "hour" => 0,
            "min" => 0
        );
        
        if (preg_match($msqlFmt,$str,$group)) {
            $info["month"] = $months[strtolower($group[1])];
            $info["day"] = $group[2];
            $info["year"] = $group[3];
            $info["hour"] = $group[4];
            $info["min"] = $group[5];
            if ($group[6] == "P") {
                $info["hour"] = ($info["hour"] + 12) % 24;
            }
        }

        $ret = $info["year"]."-";
        $ret .= str_pad($info["month"],2,"0",STR_PAD_LEFT)."-";
        $ret .= str_pad($info["day"],2,"0",STR_PAD_LEFT)." ";
        $ret .= str_pad($info["hour"],2,"0",STR_PAD_LEFT).":";
        $ret .= str_pad($info["min"],2,"0",STR_PAD_LEFT);

        return $ret;
    }

    /**
      Get list of columns that exist in both tables
      @param $table1 [string] name of first table
      @param $which_connection1 [string] name of first database connection
      @param $table2 [string] name of second table
      @param $which_connection2 [string] name of second database connection
      @return [string] list of column names or [boolean] false
    */
    public function getMatchingColumns($table1, $which_connection1, $table2, $which_connection2)
    {
        $ret = '';
        $def1 = $this->tableDefinition($table1, $which_connection1);
        $def2 = $this->tableDefinition($table2, $which_connection2);
        foreach ($def1 as $column_name => $info) {
            if (isset($def2[$column_name])) {
                $ret .= $column_name . ',';
            }
        }
        if ($ret === '') {
            return false;
        } else {
            return substr($ret, 0, strlen($ret)-1);
        }
    }

    /**
      Get column names common to both tables
      @param $table1 [string] table name
      @param $table2 [string] table name
      $which_connection [optiona] see close()
      @return [array] of [string] column names
    */
    public function matchingColumns($table1, $table2, $which_connection='')
    {
        if ($which_connection == '') {
            $which_connection=$this->default_db;
        }
        
        $definition1 = $this->tableDefinition($table1, $which_connection);
        $definition2 = $this->tableDefinition($table2, $which_connection);
        $matches = array();
        foreach ($definition1 as $col_name => $info) {
            if (isset($definition2[$col_name])) {
                $matches[] = $col_name;
            }
        }

        return $matches;
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
        if ($which_connection == '') {
            $which_connection=$this->default_db;
        }

        $exists = $this->tableExists($table_name,$which_connection);

        if (!$exists) {
            return false;
        }
        if ($exists === -1) {
            return -1;
        }

        $t_def = $this->tableDefinition($table_name,$which_connection);

        $cols = "(";
        $vals = "(";
        $args = array();
        foreach($values as $k=>$v) {
            if (isset($t_def[$k])) {
                $col_name = $k;
                $vals .= '?,';
                $args[] = $v;
                $cols .= $this->identifierEscape($col_name, $which_connection) . ',';
            } else {
                // implication: column isn't in the table
            }
        }
        $cols = substr($cols,0,strlen($cols)-1).")";
        $vals = substr($vals,0,strlen($vals)-1).")";
        $insertQ = "INSERT INTO $table_name $cols VALUES $vals";
        $insertP = $this->prepare($insertQ, $which_connection);
        $ret = $this->execute($insertP, $args, $which_connection);

        return $ret;
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
        if ($which_connection == '') {
            $which_connection=$this->default_db;
        }

        $exists = $this->tableExists($table_name,$which_connection);

        if (!$exists) {
            return false;
        }
        if ($exists === -1) {
            return -1;
        }

        $t_def = $this->tableDefinition($table_name,$which_connection);

        $sets = "";
        $args = array();
        foreach($values as $k=>$v) {
            if (isset($t_def[$k])) {
                $col_name = $k;
                $sets .= $this->identifierEscape($col_name) . ' = ?,';
                $args[] = $v;
            } else {
                // implication: column isn't in the table
            }
        }
        $sets = rtrim($sets,",");
        $upQ = "UPDATE $table_name SET $sets WHERE $where_clause";
        $upP = $this->prepare($upQ, $which_connection);

        $ret = $this->execute($upP, $args, $which_connection);

        return $ret;
    }

    /**
       Log a string to the query log.
       @param $str The string
       @return A True on success, False on failure
    */
    public function logger($str)
    {
        $ql = $this->QUERY_LOG;
        if (is_writable($ql)) {
            $fp = fopen($ql,'a');
            fputs($fp,$_SERVER['PHP_SELF'].": ".date('r').': '.$str."\n");
            fclose($fp);
            return true;
        } else {
            return false;
        }
    }

    /** 
        Everything past this is just 
        non-camelCase alias methods 
        for comaptibility with older code 
    **/
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
    public function aff_rows($which_connection='')
    {
        return $this->affectedRows($which_connection);
    }

    public function smart_insert($table_name,$values,$which_connection='')
    {
        return $this->smartInsert($table_name, $values, $which_connection);
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

