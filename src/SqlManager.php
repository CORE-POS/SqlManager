<?php

namespace COREPOS;

/**
  If Composer's autoloader is not active but is available
  within the package directory, include it */ 
if (
    !class_exists('\\Composer\\Autoload\\ClassLoader', false) &&
    file_exists(dirname(__FILE__) . '/../vendor/autoload.php')
    ) {
        include(dirname(__FILE__) . '/../vendor/autoload.php');
}

/**
 @class SqlManager
 @brief A SQL abstraction layer

 Custom SQL abstraction based on ADOdb.
 Provides some limited functionality for queries
 across two servers that are useful for lane-server
 communication 
*/

class SqlManager extends SqlManagerCommonBase implements SqlManagerInterface
{
    /** Constructor
        @param $server Database server host
        @param $type Database type. Most supported are
        'mysql' and 'mssql' but anything ADOdb supports
        will kind of work
        @param $database Database name
        @param $username Database username
        @param $password Database password
        @param $persistent Make persistent connection.
        @param $new Force new connection
    */
    public function __construct($server,$type,$database,$username,$password='',$persistent=false, $new=false)
    {
        $this->QUERY_LOG = dirname(__FILE__) . '/log/queries.log';
        $this->connections=array();
        $this->default_db = $database;
        $this->addConnection($server,$type,$database,$username,$password,$persistent,$new);
    }

    /** Add another connection
        @param $server Database server host
        @param $type Database type. Most supported are
        'mysql' and 'mssql' but anything ADOdb supports
        will kind of work
        @param $database Database name
        @param $username Database username
        @param $password Database password
        @param $persistent Make persistent connection.
        @param $new Force new connection

        When dealing with multiple connections, user the
        database name to distinguish which is to be used
    */
    public function addConnection($server,$type,$database,$username,$password='',$persistent=false,$new=false)
    {
        if (empty($type)) {
            return false;
        }

        $conn = ADONewConnection($type);
        $conn->SetFetchMode(ADODB_FETCH_BOTH);
        $ok = false;
        if (isset($this->connections[$database]) || $new) {
            $ok = $conn->NConnect($server,$username,$password,$database);
        } else {
            if ($persistent) {
                $ok = $conn->PConnect($server,$username,$password,$database);
            } else {
                $ok = $conn->Connect($server,$username,$password,$database);
            }
        }
        $this->connections[$database] = $conn;

        if (!$ok) {
            $conn = ADONewConnection($type);
            $conn->SetFetchMode(ADODB_FETCH_BOTH);
            $ok = $conn->Connect($server,$username,$password);
            if ($ok) {
                $stillok = $conn->Execute("CREATE DATABASE $database");
                if (!$stillok) {
                    $this->connections[$database] = false;
                    return false;
                }
                $conn->Execute("USE $database");
                $this->connections[$database] = $conn;
            } else {
                $this->connections[$database] = false;
                return false;
            }
        }

        return true;
    }

    /**
      Verify object is connected to the database
      @param $which_connection [string] database name (optional)
      @return [boolean]
    */
    public function isConnected($which_connection='')
    {
        if ($which_connection == '') {
            $which_connection=$this->default_db;
        }
        if (isset($this->connections[$which_connection]) &&
            is_object($this->connections[$which_connection])) {
            return true;
        } else {
            return false;
        }
    }

    /**
      Close a database connection
      @param $which_connection
      If there are multiple connections, this is
      the database name for the connection you want to close
    */
    public function close($which_connection='')
    {
        if ($which_connection == '') {
            $which_connection=$this->default_db;
        }
        $con = $this->connections[$which_connection];
        unset($this->connections[$which_connection]);

        return $con->Close();
    }

    public function setDefaultDB($db_name)
    {
        /** verify connection **/
        if (!isset($this->connections[$db_name])) {
            return false;
        }

        $this->default_db = $db_name;
        if ($this->isConnected()) {
            $this->query('use ' . $db_name, $db_name);
            $this->connections[$db_name]->database = $db_name;
        }
    }

    /**
      Execute a query
      @param $query_text The query
      @param which_connection see method close
      @return A result object on success, False on failure
    */
    public function query($query_text,$which_connection='',$params=false)
    {
        $ql = $this->QUERY_LOG;
        $con = $this->whichConnection($which_connection);

        $ok = (!is_object($con)) ? false : $con->Execute($query_text,$params);
        if (!$ok) {

            if (is_array($query_text)) {
                $query_text = $query_text[0];
            }

            $errorMsg = $_SERVER['PHP_SELF'] . ': ' . date('r') . ': ' . $query_text . "\n";
            $errorMsg .= $this->error($which_connection) . "\n\n";

            if (is_writable($ql)) {
                $fp = fopen($ql,'a');
                fwrite($fp, $errorMsg);
                fclose($fp);
            } else {
                echo str_replace("\n", '<br />', $errorMsg);
            }

            if ($this->throw_on_fail) {
                throw new Exception($errorMsg);
            }
        }

        return $ok;
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
        $ret = array();
        foreach($this->connections as $db_name => $con) {
            $ret[$db_name] = $this->query($query_text,$db_name);
        }

        return $ret;
    }

    /**
      Escape a string for SQL-safety
      @param $query_text The string to escape
      @param $which_connection see method close()
      @return The escaped string

      Note that the return value will include start and
      end single quotes
    */
    public function escape($query_text,$which_connection='')
    {
        if ($which_connection == '') {
            $which_connection = $this->default_db;
        }

        return $this->connections[$which_connection]->qstr($query_text);
    }

    public function identifierEscape($str,$which_connection='')
    {
        if ($which_connection == '') {
            $which_connection = $this->default_db;
        }
        switch($this->connections[$which_connection]->databaseType) {
            case 'mysql':
            case 'mysqli':
            case 'pdo':
                return '`'.$str.'`';
            case 'mssql':
                return '['.$str.']';
        }

        return $str;
    }
    
    /**
      Get number of rows in a result set
      @param $result_object A result set
      @param $which_connection see method close()
      @return Integer number or False if there's an error
    */
    public function numRows($result_object,$which_connection='')
    {
        if ($which_connection == '') {
            $which_connection = $this->default_db;
        }

        return $result_object->RecordCount();
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
        if ($which_connection == '') {
            $which_connection = $this->default_db;
        }

        return $result_object->Move((int)$rownum);
    }

    /**
      Get number of fields in a result set
      @param $result_object A result set
      @param $which_connection see method close()
      @return Integer number or False if there's an error
    */
    public function numFields($result_object,$which_connection='')
    {
        if ($which_connection == '') {
            $which_connection = $this->default_db;
        }

        return $result_object->FieldCount();
    }

    /**
      Get next record from a result set
      @param $result_object A result set
      @param $which_connection see method close()
      @return An array of values
    */
    public function fetchArray($result_object,$which_connection='')
    {
        if (is_null($result_object)) return false;
        if ($result_object === false) return false;

        if ($which_connection == '') {
            $which_connection = $this->default_db;
        }
        $ret = $result_object->fields;
        if ($result_object) {
            $result_object->MoveNext();
        }

        return $ret;
    }

    /**
      Get next record from a result set but as an object
      @param $result_object A result set
      @param $which_connection see method close()
      @return An object with member containing values
    */
    public function fetchObject($result_object,$which_connection='')
    {
        return $result_object->FetchNextObject(False);
    }
    
    /**
      Get the database's function for present time
      @param $which_connection see method close()
      @return The appropriate function

      For example, with MySQL this will return the
      string 'NOW()'.
    */
    public function now($which_connection='')
    {
        if ($which_connection == '') {
            $which_connection = $this->default_db;
        }

        return $this->connections[$which_connection]->sysTimeStamp;
    }

    /**
      Get the database's function for current day
      @param $which_connection see method close()
      @return The appropriate function

      For example, with MySQL this will return the
      string 'CURDATE()'.
    */
    public function curdate($which_connection='')
    {
        if ($which_connection == '') {
            $which_connection = $this->default_db;
        }

        return $this->connections[$which_connection]->sysDate;
    }

    /**
      Get the database's date difference function
      @param $date1 First date
      @param $date2 Second date
      @param $which_connection see method close()
      @return The appropriate function

      Arguments are inverted for some databases to
      ensure consistent results. If $date1 is today
      and $date2 is yesterday, this method returns
      a SQL function that evaluates to 1.
    */
    public function datediff($date1,$date2,$which_connection='')
    {
        if ($which_connection == '') {
            $which_connection = $this->default_db;
        }
        switch($this->connections[$which_connection]->databaseType) {
            case 'mysql':
            case 'mysqli':
            case 'pdo':
                return "datediff($date1,$date2)";
            case 'mssql':
                return "datediff(dd,$date2,$date1)";
        }
    }

    /**
      Get the databases' month difference function
      @param $date1 First date
      @param $date2 Second date
      @param $which_connection see method close()
      @return The SQL expression

      Arguments are inverted for some databases to
      ensure consistent results. If $date1 is this month
      and $date2 is last month, this method returns
      a SQL expression that evaluates to 1.
    */

    public function monthdiff($date1,$date2,$which_connection='')
    {
        if ($which_connection == '') {
            $which_connection = $this->default_db;
        }
        switch($this->connections[$which_connection]->databaseType) {
            case 'mysql':
            case 'mysqli':
            case 'pdo':
                return "period_diff(date_format($date1, '%Y%m'), date_format($date2, '%Y%m'))";
            case 'mssql':
                return "datediff(mm,$date2,$date1)";
        }
    }

    /**
      Get the difference between two dates in seconds
      @param $date1 First date (or datetime)
      @param $date2 Second date (or datetime)
      @param $which_connection see method close()
      @return The SQL expression

      This method currently only suport MySQL and MSSQL
    */
    public function seconddiff($date1,$date2,$which_connection='')
    {
        if ($which_connection == '') {
            $which_connection = $this->default_db;
        }
        switch($this->connections[$which_connection]->databaseType) {
            case 'mysql':
            case 'mysqli':
            case 'pdo':
                return "TIMESTAMPDIFF(SECOND,$date1,$date2)";
            case 'mssql':
                return "datediff(ss,$date2,$date1)";
        }
    }

    /**
      Get a date formatted YYYYMMDD
      @param $date1 The date (or datetime)
      @param $which_connection see method close()
      @return The SQL expression

      This method currently only supports MySQL and MSSQL
    */
    public function dateymd($date1,$which_connection='')
    {
        if ($which_connection == '') {
            $which_connection = $this->default_db;
        }
        switch($this->connections[$which_connection]->databaseType) {
            case 'mysql':
            case 'mysqli':
            case 'pdo':
                return "DATE_FORMAT($date1,'%Y%m%d')";
            case 'mssql':
                return "CONVERT(CHAR(11),$date1,112)";
        }
    }

    /**
      Get a SQL convert function
      @param $expr An SQL expression
      @param $type Convert to this SQL type
      @param $which_connection see method close()
      @return The SQL expression

      This method currently only supports MySQL and MSSQL

    */
    public function convert($expr,$type,$which_connection='')
    {
        if ($which_connection == '') {
            $which_connection = $this->default_db;
        }
        switch($this->connections[$which_connection]->databaseType) {
            case 'mysql':
            case 'mysqli':
            case 'pdo':
                if(strtoupper($type)=='INT') {
                    $type='SIGNED';
                }
                return "CONVERT($expr,$type)";
            case 'mssql':
                return "CONVERT($type,$expr)";
        }

        return "";
    }

    /**
      Find index of a substring within a larger string
      @param $substr Search string (needle)
      @param $str Target string (haystack)
      @param $which_connection see method close()
      @return The SQL expression

      This method currently only supports MySQL and MSSQL
    */
    public function locate($substr,$str,$which_connection='')
    {
        if ($which_connection == '') {
            $which_connection = $this->default_db;
        }
        switch($this->connections[$which_connection]->databaseType) {
            case 'mysql':
            case 'mysqli':
            case 'pdo':
                return "LOCATE($substr,$str)";
            case 'mssql':
                return "CHARINDEX($substr,$str)";
        }

        return "";
    }

    /**
      Concatenate strings
      @param Arbitrary; see below
      @return The SQL expression

      This function takes an arbitrary number of arguments
      and concatenates them. The last argument is the
      standard $which_connection but in this case it is
      not optional. You may pass the empty string to use
      the default database though.

      This method currently only supports MySQL and MSSQL
    */
    public function concat()
    {
        $args = func_get_args();
        $ret = "";
        $which_connection = $args[count($args)-1];
        if ($which_connection == '') {
            $which_connection = $this->default_db;
        }
        switch($this->connections[$which_connection]->databaseType) {
            case 'mysql':
            case 'mysqli':
            case 'pdo':
                $ret .= "CONCAT(";
                for($i=0;$i<count($args)-1;$i++) {
                    $ret .= $args[$i].",";
                }
                $ret = rtrim($ret,",").")";
            break;
            case 'mssql':
                for($i=0;$i<count($args)-1;$i++) {
                    $ret .= $args[$i]."+";
                }
                $ret = rtrim($ret,"+");
            break;
        }

        return $ret;
    }

    /**
      Get the differnces between two dates in weeks
      @param $date1 First date
      @param $date2 Second date
      @param $which_connection see method close()
      @return The SQL expression

      This method currently only supports MySQL and MSSQL
    */
    public function weekdiff($date1,$date2,$which_connection='')
    {
        if ($which_connection == '') {
            $which_connection = $this->default_db;
        }
        switch($this->connections[$which_connection]->databaseType) {
            case 'mysql':
            case 'mysqli':
            case 'pdo':
                return "week($date1) - week($date2)";
            case 'mssql':
                return "datediff(wk,$date2,$date1)";
        }
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
        if ($which_connection == '') {
            $which_connection = $this->default_db;
        }

        return $result_object->FetchField($index);
    }

    /**
      Start a transaction
      @param $which_connection see method close()
    */
    public function startTransaction($which_connection='')
    {
        if ($which_connection == '') {
            $which_connection = $this->default_db;
        }

        return $this->connections[$which_connection]->BeginTrans();
    }

    /**
      Finish a transaction
      @param $which_connection see method close()
    */
    public function commitTransaction($which_connection='')
    {
        if ($which_connection == '') {
            $which_connection = $this->default_db;
        }

        return $this->connections[$which_connection]->CommitTrans();
    }

    /**
      Abort a transaction
      @param $which_connection see method close()
    */
    public function rollbackTransaction($which_connection='')
    {
        if ($which_connection == '') {
            $which_connection = $this->default_db;
        }

        return $this->connections[$which_connection]->RollbackTrans();
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
        if ($which_connection == '') {
            $which_connection = $this->default_db;
        }
        $fld = $result_object->FetchField($index);

        return $fld->type;
    }

    /**
      Alias of method fetchField()
    */
    public function field_name($result_object,$index,$which_connection='')
    {
        $field = $this->fetchField($result_object, $index, $which_connection);

        if (is_object($field) && property_exists($field, 'name')) {
            return $field->name;
        } else {
            return '';
        }
    }

    /**
      Get day of week number
      @param $field A date expression
      @param $which_connection see method close()
      @return The SQL expression

      This method currently only suport MySQL and MSSQL
    */
    public function dayofweek($field,$which_connection='')
    {
        if ($which_connection == '') {
            $which_connection = $this->default_db;
        }
        // ado is inconsistent
        //$conn = $this->connections[$which_connection];
        //return $conn->SQLDate("w",$field);
        switch($this->connections[$which_connection]->databaseType) {
            case 'mysql':
            case 'mysqli':
            case 'pdo':
                return "DATE_FORMAT($field,'%w')+1";
            case 'mssql':
                return "DATEPART(dw,$field)";
        }

        return false;
    }

    /**
      Get the hour from a datetime
      @param $field A datetime expression
      @param $which_connection see method close()
      @return The SQL expression
    */
    public function hour($field,$which_connection='')
    {
        if ($which_connection == '') {
            $which_connection = $this->default_db;
        }
        $conn = $this->connections[$which_connection];

        return $conn->SQLDate("H",$field);
    }

    /**
      Get the week number from a datetime
      @param $field A datetime expression
      @param $which_connection see method close()
      @return The SQL expression
    */
    public function week($field,$which_connection='')
    {
        if ($which_connection == '') {
            $which_connection = $this->default_db;
        }
        $conn = $this->connections[$which_connection];

        return $conn->SQLDate("W",$field);
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
        if ($which_connection == '') {
            $which_connection=$this->default_db;
        }
        $conn = $this->connections[$which_connection];
        $cols = $conn->MetaColumns($table_name);
        if ($cols === false) return false;

        return true;
    }

    public function isView($table_name, $which_connection='')
    {
        if ($which_connection == '') {
            $which_connection=$this->default_db;
        }

        if (!$this->tableExists($table_name, $which_connection)) {
            return false;
        }

        $conn = $this->connections[$which_connection];
        $views = $conn->MetaTables('VIEW');
        if (in_array($table_name, $views)) {
            return true;
        } else {
            return false;
        }
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
        if ($which_connection == '') {
            $which_connection=$this->default_db;
        }
        $conn = $this->connections[$which_connection];
        $cols = $conn->MetaColumns($table_name);

        $return = array();
        if (is_array($cols)) {
            foreach($cols as $c) {
                $return[$c->name] = $c->type;
            }
            return $return;
        }

        return false;
    }

    /**
      More detailed table definition
       @param $table_name The table's name
       @param which_connection see method close
       @return
        - array of column name => info array
        - the info array has keys: 
            * type (string)
            * increment (boolean OR null if unknown)
            * primary_key (boolean OR null if unknown)
            * default (value OR null)
    */
    public function detailedDefinition($table_name,$which_connection='')
    {
        if ($which_connection == '') {
            $which_connection=$this->default_db;
        }
        $conn = $this->connections[$which_connection];
        $cols = $conn->MetaColumns($table_name);

        $return = array();
        if (is_array($cols)) {
            foreach($cols as $c) {
                $info = array();
                $type = strtoupper($c->type);
                if (property_exists($c, 'max_length') && $c->max_length != -1 && substr($type, -3) != 'INT') {
                    if (property_exists($c, 'scale') && $c->scale) {
                        $type .= '(' . $c->max_length . ',' . $c->scale . ')';
                    } else {
                        $type .= '(' . $c->max_length . ')';
                    }
                }
                if (property_exists($c, 'unsigned') && $c->unsigned) {
                    $type .= ' UNSIGNED';
                }
                $info['type'] = $type;
                if (property_exists($c, 'auto_increment') && $c->auto_increment) {
                    $info['increment'] = true;
                } else if (property_exists($c, 'auto_increment') && !$c->auto_increment) {
                    $info['increment'] = false;
                } else {
                    $info['increment'] = null;
                }
                if (property_exists($c, 'primary_key') && $c->primary_key) {
                    $info['primary_key'] = true;
                } else if (property_exists($c, 'primary_key') && !$c->primary_key) {
                    $info['primary_key'] = false;
                } else {
                    $info['primary_key'] = null;
                }

                if (property_exists($c, 'default_value') && $c->default_value !== 'NULL' && $c->default_value !== null) {
                    $info['default'] = $c->default_value;
                } else {
                    $info['default'] = null;
                }

                $return[$c->name] = $info;
            }

            return $return;
        }

        return false;
    }

    /**
       Get list of tables/views
       @param which_connection see method close
    */
    public function getTables($which_connection='')
    {
        if ($which_connection == '') {
            $which_connection=$this->default_db;
        }
        $conn = $this->connections[$which_connection];

        return $conn->MetaTables();
    }

    /**
      Get current default database
      for a given connection
      @param which_connection see method close
      @return [string] database name
        or [boolean] false on failure
    */
    public function defaultDatabase($which_connection='')
    {
        if ($which_connection == '') {
            $which_connection=$this->default_db;
        }

        if (count($this->connections) == 0) {
            return false;
        }

        $query ='';
        switch($this->connections[$which_connection]->databaseType) {
            case 'mysql':
            case 'mysqli':
            case 'pdo':
                $query = 'SELECT DATABASE() as dbname';
                break;
            case 'mssql':
                $query = 'SELECT DB_NAME() as dbname';
                break;
            // postgres is SELECT CURRENT_DATABASE()
            // should it ever come up
        }

        $ret = false;
        $try = $this->query($query, $which_connection);
        if ($try && $this->num_rows($try) > 0) {
            $row = $this->fetch_row($try);
            $ret = $row['dbname'];
        }

        return $ret;
    }

    /**
      Get database's currency type
      @param which_connection see method close
      @return The SQL type
    */
    public function currency($which_connection='')
    {
        if ($which_connection == '') {
            $which_connection=$this->default_db;
        }
        switch($this->connections[$which_connection]->databaseType) {
            case 'mysql':
            case 'mysqli':
            case 'pdo':
                return 'decimal(10,2)';
            case 'mssql':
                return 'money';
        }

        return 'decimal(10,2)';
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
        if ($which_connection == '') {
            $which_connection=$this->default_db;
        }
        switch($this->connections[$which_connection]->databaseType) {
            case 'mysql':
            case 'mysqli':
            case 'pdo':
                return sprintf("%s LIMIT %d",$query,$int_limit);
            case 'mssql':
                return str_ireplace("SELECT ","SELECT TOP $int_limit ",$query);
        }

        return $query;
    }

    /**
      Get database scope separator
      @param which_connection see method close
      @return String separator
    */
    public function sep($which_connection='')
    {
        if ($which_connection == '') {
            $which_connection=$this->default_db;
        }
        switch($this->connections[$which_connection]->databaseType) {
            case 'mysql':
            case 'mysqli':
            case 'pdo':
                return ".";
            case 'mssql':
                return ".dbo.";
        }

        return ".";
    }

    /**
      Get name of database driver
      @param which_connection see method close
      @return String name
    */
    public function dbmsName($which_connection='')
    {
        if ($which_connection == '') {
            $which_connection=$this->default_db;
        }

        return $this->connections[$which_connection]->databaseType;
    }

    /**
      Get last error message
      @param which_connection see method close
      @return The message
    */
    public function error($which_connection='')
    {
        $con = $this->whichConnection($which_connection);

        if (!is_object($con)) {
            return 'No database connection';
        }

        return $con->ErrorMsg();
    }

    /**
      Get auto incremented ID from last insert
      @param which_connection see method close
      @return The new ID value
    */
    public function insertID($which_connection='')
    {
        if ($which_connection == '') {
            $which_connection=$this->default_db;
        }
        $con = $this->connections[$which_connection];

        return $con->Insert_ID();
    }

    /**
      Check how many rows the last query affected
      @param which_connection see method close
      @returns Number of rows
    */
    public function affectedRows($which_connection='')
    {
        if ($which_connection == '') {
            $which_connection=$this->default_db;
        }
        $con = $this->connections[$which_connection];

        return $con->Affected_Rows();
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
        if ($which_connection == '') {
            $which_connection=$this->default_db;
        }
        $con = $this->connections[$which_connection];

        return $con->Prepare($sql);
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
        if ($which_connection == '') {
            $which_connection=$this->default_db;
        }
        if (!is_array($input_array)) {
            $input_array = array($input_array);
        }

        return $this->query($sql,$which_connection,$input_array);
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
        $dateStr = trim($dateStr,"'");
        $seconds = strtotime($dateStr);
        if ($seconds === false) {
            $seconds = time();
        }
        $base = date("Y-m-d",$seconds);
    
        return sprintf("(%s BETWEEN '%s 00:00:00' AND '%s 23:59:59')",
            $col,$base,$base);
    }

    /* compat layer; mimic functions of Brad's mysql class */
    public function get_result($host,$user,$pass,$data_base,$query)
    {
        return $this->query($query);
    }

    // skipping fetch_cell on purpose; generic-db way would be slow as heck

    /* end compat Brad's class */
}

