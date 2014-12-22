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
if (!interface_exists('\\COREPOS\\SqlManagerInterface')) {
    include(dirname(__FILE__) . '/SqlManagerInterface.php');
}
if (!class_exists('\\COREPOS\\SqlManagerCommonBase')) {
    include(dirname(__FILE__) . '/SqlManagerCommonBase.php');
}

/**
 @class SqlManager
 @brief A SQL abstraction layer

 Custom SQL abstraction based on ADOdb.
 Provides some limited functionality for queries
 across two servers that are useful for lane-server
 communication 
*/

class SqlManagerDoctrine extends SqlManagerCommonBase implements SqlManagerInterface
{
     private $most_recent_statement = null; 

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

        $connectionParams = array(
            'dbname' => $database,
            'user' => $username,
            'password' => $password,
            'host' => $server,
            'driver' => $type,
        );

        /**
          First just try a regular connection
        */
        $ok = false;
        try {
            $conn = \Doctrine\DBAL\DriverManager::getConnection($connectionParams);
            $ok = true;
            $this->connections[$database] = $conn;
        } catch (\Exception $e) {
            $ok = false;
            $this->connections[$database] = false;
        }

        /** if connection fails, try connecting without
            specifiying a database, then creating and
            selecting that database **/
        if (!$ok) {
            try {
                unset($connectionParams['dbname']);
                $conn = \Doctrine\DBAL\DriverManager::getConnection($connectionParams);
                $created = $conn->getSchemaManager()->createDatabase($database);
                if ($created && $conn->query('USE ' . $conn->quoteIdentifier)) {
                    $ok = true;
                    $this->connections[$database] = $conn;
                }
            } catch (\Exception $e) {
                $ok = false;
                $this->connections[$database] = false;
            }
        }

        return $ok;
    }

    /**
      Verify object is connected to the database
      @param $which_connection [string] database name (optional)
      @return [boolean]
    */
    public function isConnected($which_connection='')
    {
        $obj = $this->whichConnection($which_connection);
        if (is_object($obj) && $obj instanceof \Doctrine\DBAL\Connection) {
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

        return $con->close();
    }

    public function setDefaultDB($db_name)
    {
        /** verify connection **/
        if (!isset($this->connections[$db_name])) {
            return false;
        }

        $this->default_db = $db_name;
        if ($this->isConnected()) {
            $this->query('use ' . $this->identifierEscape($db_name), $db_name);
        }
    }

    /**
      Execute a query
      @param $query_text The query
      @param which_connection see method close
      @return A result object on success, False on failure
    */
    public function query($query_text, $which_connection='', $params=false)
    {
        $ql = $this->QUERY_LOG;
        $con = $this->whichConnection($which_connection);
        $ok = false;
        $stmt = false;
        if ($params === false || !is_array($params)) {
            $params = array();
        }

        if (is_object($con)) {
            if (!$query_text instanceof \Doctrine\DBAL\Statement) {
                $stmt = $con->prepare($query_text); 
            } else {
                $stmt = $query_text;
            }
            for ($i=0; $i<count($params); $i++) {
                // binding is 1-indexed
                $stmt->bindValue($i+1, $params[$i]);
            }
            try {
                $ok = $stmt->execute();
            } catch (\Exception $e) {
                $ok = false;
            } 

            $this->most_recent_statement = $stmt;
        }

        if (!$ok) {
            if ($query_text instanceof \Doctrine\DBAL\Statement) {
                $query_text = serialize($query_text);
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
                throw new \Exception($errorMsg);
            }
        } else {

            return $stmt;
        }
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
        $con = $this->whichConnection($which_connection);

        return $con->quote($query_text);
    }

    public function identifierEscape($str,$which_connection='')
    {
        $con = $this->whichConnection($which_connection);

        return $con->quoteIdentifier($query_text);
    }
    
    /**
      Get number of rows in a result set
      @param $result_object A result set
      @param $which_connection see method close()
      @return Integer number or False if there's an error
    */
    public function numRows($result_object, $which_connection='')
    {
        return $result_object->rowCount();
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
        // not implemented in doctrine
        return false;
    }

    /**
      Get number of fields in a result set
      @param $result_object A result set
      @param $which_connection see method close()
      @return Integer number or False if there's an error
    */
    public function numFields($result_object,$which_connection='')
    {
        return $result_object->columnCount();
    }

    /**
      Get next record from a result set
      @param $result_object A result set
      @param $which_connection see method close()
      @return An array of values
    */
    public function fetchArray($result_object,$which_connection='')
    {
        if ($result_object instanceof \Doctrine\DBAL\Statement) {
            return $result_object->fetch(\PDO::FETCH_BOTH);
        } else {
            return false;
        }
    }

    /**
      Get next record from a result set but as an object
      @param $result_object A result set
      @param $which_connection see method close()
      @return An object with member containing values
    */
    public function fetchObject($result_object,$which_connection='')
    {
        if ($result_object instanceof \Doctrine\DBAL\Statement) {
            return $result_object->fetch(\PDO::FETCH_OBJ);
        } else {
            return false;
        }
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
        $obj = $this->whichConnection($which_connection);

        return $obj->getDatabasePlatform()->getNowExpression();
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
        $obj = $this->whichConnection($which_connection);

        switch (strtolower($obj->getDriver()->getName())) {
            case 'mysqli':
            case 'pdo_mysql':
                return 'CURDATE()';
            case 'sqlsrv':
            case 'pdo_sqlsrv':
                return 'GETDATE()';
        }

        return false;
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
        $obj = $this->whichConnection($which_connection);

        return $obj->getDatabasePlatform()->getDateDiffExpression($date1, $date2);
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
        $obj = $this->whichConnection($which_connection);

        switch (strtolower($obj->getDriver()->getName())) {
            case 'mysqli':
            case 'pdo_mysql':
                return "period_diff(date_format($date1, '%Y%m'), date_format($date2, '%Y%m'))";
            case 'sqlsrv':
            case 'pdo_sqlsrv':
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
        $obj = $this->whichConnection($which_connection);

        switch (strtolower($obj->getDriver()->getName())) {
            case 'mysqli':
            case 'pdo_mysql':
                return "TIMESTAMPDIFF(SECOND,$date1,$date2)";
            case 'sqlsrv':
            case 'pdo_sqlsrv':
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
        $obj = $this->whichConnection($which_connection);

        switch (strtolower($obj->getDriver()->getName())) {
            case 'mysqli':
            case 'pdo_mysql':
                return "DATE_FORMAT($date1,'%Y%m%d')";
            case 'sqlsrv':
            case 'pdo_sqlsrv':
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
        $obj = $this->whichConnection($which_connection);

        switch (strtolower($obj->getDriver()->getName())) {
            case 'mysqli':
            case 'pdo_mysql':
                if(strtoupper($type)=='INT') {
                    $type='SIGNED';
                }
                return "CONVERT($expr,$type)";
            case 'sqlsrv':
            case 'pdo_sqlsrv':
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
    */
    public function locate($substr,$str,$which_connection='')
    {
        $obj = $this->whichConnection($which_connection);

        return $obj->getDatabasePlatform()->getLocateExpression($str, $substr);
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
    */
    public function concat()
    {
        $args = func_get_args();
        $which_connection = array_pop($args);
        $con = $this->whichConnection($which_connection);
        $platform = $con->getDatabasePlatform();

        return call_user_func_array(array($platform, 'getConcatExpression'), $args);
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
        $obj = $this->whichConnection($which_connection);

        switch (strtolower($obj->getDriver()->getName())) {
            case 'mysqli':
            case 'pdo_mysql':
                return "week($date1) - week($date2)";
            case 'sqlsrv':
            case 'pdo_sqlsrv':
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
        $raw = $result_object->getWrappedStatement();
        if (method_exists($raw, 'getColumnMeta')) {
            $info = $raw->getColumnMeta($index);
            $ret = new \stdClass();
            $ret->name = $info['name'];
            $ret->type = $info['native_type'];
            $ret->max_length = $info['len'];

            return $ret;
        } else {
            return false;
        }
    }

    /**
      Start a transaction
      @param $which_connection see method close()
    */
    public function startTransaction($which_connection='')
    {
        $obj = $this->whichConnection($which_connection);

        return $obj->beginTransaction();
    }

    /**
      Finish a transaction
      @param $which_connection see method close()
    */
    public function commitTransaction($which_connection='')
    {
        $obj = $this->whichConnection($which_connection);

        return $obj->commit();
    }

    /**
      Abort a transaction
      @param $which_connection see method close()
    */
    public function rollbackTransaction($which_connection='')
    {
        $obj = $this->whichConnection($which_connection);

        return $obj->rollback();
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

        $num_fields = $this->num_fields($result,$source_db);

        $unquoted = array("money"=>1,"real"=>1,"numeric"=>1,
            "float4"=>1,"float8"=>1,"bit"=>1);
        $strings = array("varchar"=>1,"nvarchar"=>1,"string"=>1,
            "char"=>1);
        $dates = array("datetime"=>1);
        $queries = array();

        while($row = $this->fetch_array($result,$source_db)) {
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
      Get column type
      @param $result_object A result set
      @param $index Integer index
      @param $which_connection see method close()
      @return The column type
    */
    public function fieldType($result_object,$index,$which_connection='')
    {
        $field = $this->fetchField($result_object, $index, $which_connection);

        if (is_object($field) && property_exists($field, 'type')) {
            return $field->type;
        } else {
            return false;
        }
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
            return false;
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
        $obj = $this->whichConnection($which_connection);

        switch (strtolower($obj->getDriver()->getName())) {
            case 'mysqli':
            case 'pdo_mysql':
                return "DATE_FORMAT($field,'%w')+1";
            case 'sqlsrv':
            case 'pdo_sqlsrv':
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
        return 'HOUR(' . $field . ')';
    }

    /**
      Get the week number from a datetime
      @param $field A datetime expression
      @param $which_connection see method close()
      @return The SQL expression
    */
    public function week($field,$which_connection='')
    {
        return 'WEEK(' . $field . ')';
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
        $obj = $this->whichConnection($which_connection);

        return $obj->getSchemaManager()->tablesExist(array($table_name));
    }

    public function isView($table_name, $which_connection='')
    {
        if (!$this->tableExists($table_name, $which_connection)) {
            return false;
        }

        $obj = $this->whichConnection($which_connection);
        $views = $obj->getSchemaManager()->listViews();
        foreach ($views as $view) {
            if ($table_name == $view->getName()) {
                return true;
            } elseif ($table_name == $view->getQuotedName()) {
                return true;
            }
        }

        return false;
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
        $obj = $this->whichConnection($which_connection);
        $columns = $obj->getSchemaManager()->listTableColumns($table_name);

        $return = array();
        foreach ($columns as $c) {
            $return[$c->getName()] = $c->getType();
        }

        return count($return) == 0 ? false : $return;
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
        $obj = $this->whichConnection($which_connection);
        $columns = $obj->getSchemaManager()->listTableColumns($table_name);

        $return = array();
        foreach($columns as $c) {
            $name = $c->getName();
            $return[$name] = array();
            $return[$name]['type'] = $c->getType();
            if ($c->getPrecision() && $c->getScale()) {
                $return[$name]['type'] .= '(' . $c->getPrecision() . ',' . $c->getScale() . ')';
            } elseif ($c->getLength()) {
                $return[$name]['type'] .= '(' . $c->getLength() . ')';
            }
            $return[$name]['default'] = $c->getDefault();
            $return[$name]['increment'] = $c->getAutoincrement() ? true : false;
        }

        $indexes = $obj->getSchemaManager()->listTableIndexes($table_name);
        foreach ($indexes as $i) {
            if ($i->isPrimary()) {
                foreach ($i->getColumns as $col) {
                    if (isset($return[$col])) {
                        $return[$col]['primary_key'] = true;
                    }
                }
            }
        }

        return count($return) == 0 ? false : $return;
    }

    /**
       Get list of tables/views
       @param which_connection see method close
    */
    public function getTables($which_connection='')
    {
        $obj = $this->whichConnection($which_connection);

        return $obj->getSchemaManager()->listTableNames();
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
        if (count($this->connections) == 0) {
            return false;
        }

        $obj = $this->whichConnection($which_connection);
        switch (strtolower($obj->getDriver()->getName())) {
            case 'mysqli':
            case 'pdo_mysql':
                $query = 'SELECT DATABASE() as dbname';
                break;
            case 'sqlsrv':
            case 'pdo_sqlsrv':
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
        $obj = $this->whichConnection($which_connection);
        switch (strtolower($obj->getDriver()->getName())) {
            case 'mysqli':
            case 'pdo_mysql':
                return 'decimal(10,2)';
            case 'sqlsrv':
            case 'pdo_sqlsrv':
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
        $obj = $this->whichConnection($which_connection);

        return $obj->getDatabasePlatform()->modifyLimitQuery($query, $int_limit);
    }

    /**
      Get database scope separator
      @param which_connection see method close
      @return String separator
    */
    public function sep($which_connection='')
    {
        $obj = $this->whichConnection($which_connection);
        switch (strtolower($obj->getDriver()->getName())) {
            case 'mysqli':
            case 'pdo_mysql':
                return ".";
            case 'sqlsrv':
            case 'pdo_sqlsrv':
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
        $obj = $this->whichConnection($which_connection);

        return strtolower($obj->getDriver()->getName());
    }

    /**
      Get last error message
      @param which_connection see method close
      @return The message
    */
    public function error($which_connection='')
    {
        if (!is_object($this->most_recent_statement)) {
            return 'No recent queries logged';
        }

        $e = $this->most_recent_statement->errorInfo();
        if (is_array($e)) {
            return isset($e[2]) && $e[2] ? $e[2] : '';
        } else {
            return $e;
        }
    }

    /**
      Get auto incremented ID from last insert
      @param which_connection see method close
      @return The new ID value
    */
    public function insertID($which_connection='')
    {
        $obj = $this->whichConnection($which_connection);

        return $obj->lastInsertId();
    }

    /**
      Check how many rows the last query affected
      @param which_connection see method close
      @returns Number of rows
    */
    public function affectedRows($which_connection='')
    {
        return $this->most_recent_statement ? $this->most_recent_statement->rowCount() : false;
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
        $con = $this->whichConnection($which_connection);

        return $con->prepare($sql);
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
        if (!is_array($input_array)) {
            $input_array = array($input_array);
        }

        return $this->query($sql, $which_connection, $input_array);
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

    public function aff_rows($result)
    {
        return $this->affected_rows($result);
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
        
        $definition1 = $this->table_definition($table1, $which_connection);
        $definition2 = $this->table_definition($table2, $which_connection);
        $matches = array();
        foreach($definition1 as $col_name => $info) {
            if (isset($definition2[$col_name])) {
                $matches[] = $col_name;
            }
        }

        return $matches;
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
      Enable or disable exceptions on failed queries
      @param $mode boolean
    */
    public function throwOnFailure($mode)
    {
        $this->throw_on_fail = $mode;
    }

    // skipping fetch_cell on purpose; generic-db way would be slow as heck

    /* end compat Brad's class */
}

