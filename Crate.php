<?php

class StopIteration extends Exception {
}

class DatabaseError extends Exception {
}

class NotImplemented extends DatabaseError {
}

class NotSupported extends DatabaseError {
}

class ProgrammingError extends DatabaseError {
}

class ConnectionError extends DatabaseError {
}

class Cursor {

    public $arraysize;
    public $connection;
    private $closed;
    private $result;

    private $rowcount;
    private $duration;

    private $rows;

    public function __construct(Connection $connection){
        $this->arraysize = 1;
        $this->connection = $connection;
        $this->closed = false;
        $this->result = null;
        $this->rows = new ArrayIterator(array());
    }

    public function execute($sql, array $parameters=NULL){
        if ($this->connection->is_closed()) {
            throw new ProgrammingError('Connection closed');
        }
        if ($this->closed) {
            throw new ProgrammingError('Cursor closed');
        }
        $this->result = $this->connection->client->sql($sql, $parameters);
        if (array_key_exists('rows', $this->result)) {
            $this->rows = new ArrayIterator($this->result->rows);
        } else {
            $this->rows = new ArrayIterator(array());
        }
    }

    public function executemany($sql, array $seq_of_parameters){
        $row_counts = array();
        $durations = array();
        foreach ($$seq_of_parameters as $params) {
            $this->execute($sql, $params);
            if ($this->rowcount > -1) {
                $row_counts[] = $this->rowcount;
            }
            if ($this->duration > -1) {
                $durations[] = $this->duration;
            }
        }
        $this->result = (object)array(
            'rowcount' => sum($row_counts),
            'duration' => '',
            'rows' => array(),
        );
        $this->rows = new ArrayIterator(array());
    }

    public function fetchone(){
        return $this->next();
    }

    public function fetchmany(int $count=NULL){
        throw new NotImplemented('TODO');
        if ($count === NULL) {
            $count = $this->arraysize;
        }
        if ($count === 0) {
            return $this->fetchall();
        }
        $result = array();
        foreach (range(0,$count) as $i) {
            try {
                $result[] = $this->_next();
            } catch (Exception $e) {
            }
        }
        return $result;
    }

    public function fetchall(){
        $result = array();
        $iterate = true;
        while ($iterate) {
            try {
                $result[] = $this->_next();
            } catch (Exception $e) {
                $iterate = false;
            }
        }
        return $result;
    }

    public function next(){
        try {
            return $this->_next();
        } catch (Exception $e) {
            return NULL;
        }
    }

    public function close(){
        $this->closed = true;
        $this->result = null;
    }

    public function setinputsizes($sizes){
        throw new NotSupported('Method not supported');
    }

    public function setoutputsizes($size, $column){
        throw new NotSupported('Method not supported');
    }

    private function _next(){
        if (!$this->closed) {
            if ($this->rows->valid()) {
                $o = $this->rows->current();
                $this->rows->next();
                return $o;
            } else {
                throw new StopIteration('Reached end of iterator');
            }
        } else {
            throw new ProgrammingError('Cursor closed');
        }
    }

    public function __get($name){
        if ($name == 'duration') {
            if ($this->closed || !$this->result || !array_key_exists('duration', $this->result)) {
                return -1;
            }
            return $this->result->duration || 0;
        }
        if ($name == 'rowcount') {
            if ($this->closed || !$this->result || !array_key_exists('rows', $this->result)) {
                return -1;
            }
            return $this->result->rowcount || count($this->result->rows);
        }
        if ($name == 'description') {
            if ($this->closed) {
                return;
            }
            $description = array();
            foreach ($this->results->cols as $col) {
                $description[] = array($col, NULL, NULL, NULL, NULL, NULL, NULL);
            }
            return $description;
        }
    }

}

class Server {

    private $host;

    public function __construct($server, $timeout=NULL){
        if (!(strpos($server, 'http') === 0)) {
            $server = 'http://' . $server;
        }
        $this->host = $server;
    }

    public function request($method, $path, $data){
        $url = $this->host . $path;
        // echo '--> host=' .$this->host. "\n";
        // echo '--> method=' .$method. ' path=' .$path. "\n";
        // echo '--> data=' .$data. "\n";
        // echo '--> url=' .$url. "\n";
        $ch = curl_init($url);
        curl_setopt($ch, CURLOPT_CUSTOMREQUEST, "POST");
        curl_setopt($ch, CURLOPT_POSTFIELDS, $data);
        curl_setopt($ch, CURLOPT_RETURNTRANSFER, true);
        curl_setopt($ch, CURLOPT_HTTPHEADER, array(
            'Content-Type: application/json',
            'Content-Length: ' . strlen($data))
        );
        $result = curl_exec($ch);
        if (curl_errno($ch)) {
            $msg = curl_error($ch);
            throw new ConnectionError($msg);
        } else {
            return $result;
        }

    }

}

class Client {

    private $sql_path = '/_sql';
    private $retry_interval = 30;
    private $default_server = '127.0.0.1:4200';

    private $active_servers;
    private $inactive_servers;
    private $http_timeout;

    private $server_pool;

    public function __construct($hosts=NULL, $timeout=NULL){
        if (!$hosts) {
            $hosts = array($this->default_server);
        } else {
            if (gettype($hosts) == 'string') {
                $hosts = explode(' ', $hosts);
            }
        }
        $this->active_servers = $hosts;
        $this->inactive_servers = array();
        $this->http_timeout = $timeout;

        $this->server_pool = array();
        $this->update_server_pool($hosts, $timeout);
    }

    public function sql($stmt, array $parameters=NULL){
        if ($stmt == NULL) {
            return NULL;
        }
        if (gettype($stmt) != 'string') {
            throw new Exception('$stmt is not a string type');
        }
        $data = array(
            'stmt' => $stmt
        );
        if ($parameters) {
            $data['args'] = $parameters;
        }
        return $this->json_request('POST', $this->sql_path, $data);
    }

    private function json_request($method, $path, $data=NULL){
        if ($data) {
            $data = json_encode($data);
        }
        $response = $this->request($method, $path, NULL, $data);
        // TODO: throw correct exceptions
        if (count($response) > 0) {
            try {
                return json_decode($response);
            } catch (Exception $e) {
                throw new ProgrammingError('Invalid server response');
            }
        }
        return $response;
    }

    private function request($method, $path, $server=NULL, $data=NULL){
        while (true) {
            if ($server) {
                $next_server = $server;
            } else {
                $next_server = $this->get_server();
            }
            $pool = $this->server_pool;
            try {
                return $pool[$next_server]->request($method, $path, $data);
            } catch (Exception $e) {
                $this->drop_active_server($next_server);
                if (count($this->active_servers) === 0) {
                    throw $e;
                }
            }
        }
    }

    private function update_server_pool(array $servers, int $timeout=NULL){
        $pool = &$this->server_pool;
        foreach ($servers as $server) {
            if (!array_key_exists($server, $pool)) {
                $pool[$server] = new Server($server, $timeout);
            }
        }
    }

    private function drop_active_server($server){
        $s = &$this->active_servers;
        $idx = array_search($server, $this->active_servers);
        unset($s[$idx]);
    }

    private function get_server(){
        $s = $this->active_servers;
        $server = $s[0];
        $this->roundrobin();
        return $server;
    }

    private function roundrobin(){
        $s = &$this->active_servers;
        $s[] = array_shift($this->active_servers);
    }

}

class Connection {

    public $client;
    private $closed;

    public function __construct($hosts, $timeout=NULL, $client=NULL){
        if ($client) {
            $this->client = $client;
        } else {
            $this->client = new Client($hosts, $timeout);
        }
        $this->closed = false;
    }

    public function cursor(){
        if (!$this->closed) {
            return new Cursor($this);
        } else {
            throw new ProgrammingError('Connection closed');
        }
    }

    public function close(){
        $this->closed = true;
    }

    public function is_closed(){
        return $this->closed;
    }

    public function commit(){
        if ($this->closed) {
            throw new ProgrammingError('Connection closed');
        }
    }


}

function connect($hosts, $timeout=NULL, $client=NULL){
    return new Connection($hosts, $timeout, $client);
}

?>
