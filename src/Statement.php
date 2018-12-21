<?php

namespace ClickHouseDB;

use ClickHouseDB\Exception\DatabaseException;
use ClickHouseDB\Exception\QueryException;
use ClickHouseDB\Query\Query;
use ClickHouseDB\Transport\CurlerRequest;
use ClickHouseDB\Transport\CurlerResponse;

class Statement
{
    /**
     * @var string|mixed
     */
    private $_rawData;

    /**
     * @var int
     */
    private $_http_code = -1;

    /**
     * @var CurlerRequest
     */
    private $_request = null;

    /**
     * @var bool
     */
    private $_init = false;

    /**
     * @var Query
     */
    private $query;

    /**
     * @var mixed
     */
    private $format;

    /**
     * @var string
     */
    private $sql = '';

    /**
     * @var array
     */
    private $meta;

    /**
     * @var array
     */
    private $totals;

    /**
     * @var array
     */
    private $extremes;

    /**
     * @var int
     */
    private $rows;

    /**
     * @var bool|integer
     */
    private $rows_before_limit_at_least = false;

    /**
     * @var array
     */
    private $array_data = [];

    /**
     * @var array|null
     */
    private $statistics = null;


    public function __construct(CurlerRequest $request)
    {
        $this->_request = $request;
        $this->format = $this->_request->getRequestExtendedInfo('format');
        $this->query = $this->_request->getRequestExtendedInfo('query');
        $this->sql = $this->_request->getRequestExtendedInfo('sql');
    }

    /**
     * @return CurlerRequest
     */
    public function getRequest()
    {
        return $this->_request;
    }

    /**
     * @return CurlerResponse
     * @throws Exception\TransportException
     */
    private function response()
    {
        return $this->_request->response();
    }

    /**
     * @return mixed
     * @throws Exception\TransportException
     */
    public function responseInfo()
    {
        return $this->response()->info();
    }

    /**
     * @return mixed|string
     */
    public function sql()
    {
        return $this->sql;
    }

    /**
     * @param string $body
     * @return array|bool
     */
    private function parseErrorClickHouse($body)
    {
        $body = trim($body);
        $mathes = [];

        // Code: 115, e.displayText() = DB::Exception: Unknown setting readonly[0], e.what() = DB::Exception
        // Code: 192, e.displayText() = DB::Exception: Unknown user x, e.what() = DB::Exception
        // Code: 60, e.displayText() = DB::Exception: Table default.ZZZZZ doesn't exist., e.what() = DB::Exception

        if (preg_match("%Code: (\d+),\se\.displayText\(\) \=\s*DB\:\:Exception\s*:\s*(.*)\,\s*e\.what.*%ius", $body, $mathes)) {
            return ['code' => $mathes[1], 'message' => $mathes[2]];
        }

        return false;
    }

    /**
     * @return bool
     * @throws Exception\TransportException
     */
    public function error()
    {
        if (!$this->isError()) {
            return false;
        }

        $body = $this->response()->body();
        $error_no = $this->response()->error_no();
        $error = $this->response()->error();

        if (!$error_no && !$error) {
            $parse = $this->parseErrorClickHouse($body);

            if ($parse) {
                throw new DatabaseException($parse['message'] . "\nIN:" . $this->sql(), $parse['code']);
            } else {
                $code = $this->response()->http_code();
                $message = "HttpCode:" . $this->response()->http_code() . " ; " . $this->response()->error() . " ;" . $body;
            }
        } else {
            $code = $error_no;
            $message = $this->response()->error();
        }

        throw new QueryException($message, $code);
    }

    /**
     * @return bool
     * @throws Exception\TransportException
     */
    public function isError()
    {
        return ($this->response()->http_code() !== 200 || $this->response()->error_no());
    }

    private function check() : bool
    {
        if (!$this->_request->isResponseExists()) {
            throw QueryException::noResponse();
        }

        if ($this->isError()) {
            $this->error();
        }

        return true;
    }

    /**
     * @return bool
     * @throws Exception\TransportException
     */
    private function init()
    {
        if ($this->_init) {
            return false;
        }

        $this->check();

        $this->_rawData = $this->response()->rawDataOrJson($this->format);

        $this->_init = true;

        if (!$this->_rawData) {
            return false;
        }

        foreach (['meta', 'totals', 'extremes', 'rows', 'rows_before_limit_at_least', 'statistics'] as $key) {

            if (isset($this->_rawData[$key])) {
                $this->{$key} = $this->_rawData[$key];
            }

        }

        $data=[];

        $isTSVWithNames = stripos('TSVWithNames|TabSeparatedWithNames|', $this->format.'|')!==false?true:false;
        $isTSVWithNamesAndTypes = stripos('TSVWithNamesAndTypes|TabSeparatedWithNamesAndTypes|', $this->format.'|')!==false?true:false;
        $isCSVWithNames = stripos($this->format,'CSVWithNames')!==false?true:false;
        $isJSONEachRow = stripos($this->format,'JSONEachRow')!==false?true:false;
        $isCSV = stripos($this->format,'CSV')!==false?true:false;
        $isTSV = stripos('TabSeparated|TSV|', $this->format.'|')!==false?true:false;

        if ($isTSVWithNames || $isTSVWithNamesAndTypes || $isCSVWithNames) { // TSVWithNames*/CSVWithNames, the 1st row should be columns' name
            $data = explode("\n", trim($this->_rawData)); // TODO: shoud we parse _rawData line-by-line to save memory???
            unset($this->_rawData); // no need to keep rawData

            $separatedChar = ($isCSVWithNames) ? ',' : "\t";
            $this->meta = array_map(function($v){ return ['name' => trim($v,'"')]; }, explode($separatedChar, $data[0]));
            unset($data[0]);
            if ($isTSVWithNamesAndTypes) { // the 2nd row should be columns' type
                $this->meta = array_map(function($u, $v){ $u['type'] = trim($v,"\t"); return $u; }, $this->meta, explode($separatedChar, $data[1]));
                unset($data[1]);
            }
            $this->rows = count($data);
            $this->array_data = array_values($data);

            unset($data);
        } elseif ($isJSONEachRow && !empty($this->_rawData)) {
            $data = explode("\n", trim($this->_rawData));
            unset($this->_rawData); // no need to keep rawData

            // parse row0 to get meta
            $this->meta = array_map(function($v){ return ['name' => $v]; }, array_keys(json_decode($data[0],true)));
            $this->array_data = $data;
            $this->rows = count($data);

            unset($data);
        } elseif ($isCSV || $isTSV) {
            $data = explode("\n", trim($this->_rawData));
            unset($this->_rawData); // no need to keep rawData

            $this->array_data = $data;
            $this->rows = count($data);

            unset($data);
        } elseif (isset($this->_rawData['data'])) {
            $this->array_data = $this->_rawData['data'];
            unset($this->_rawData); // no need to keep rawData
        }

        if (empty($this->meta) && !($isCSV || $isTSV)) { // no-meta excepts CSV & TSV
            throw  new QueryException('Can`t find meta');
        }

        return true;
    }

    /**
     * @return array
     * @throws \Exception
     */
    public function extremes()
    {
        $this->init();
        return $this->extremes;
    }

    /**
     * @return mixed
     * @throws Exception\TransportException
     */
    public function totalTimeRequest()
    {
        $this->check();
        return $this->response()->total_time();

    }

    /**
     * @return array
     * @throws \Exception
     */
    public function extremesMin()
    {
        $this->init();

        if (empty($this->extremes['min'])) {
            return [];
        }

        return $this->extremes['min'];
    }

    /**
     * @return array
     * @throws \Exception
     */
    public function extremesMax()
    {
        $this->init();

        if (empty($this->extremes['max'])) {
            return [];
        }

        return $this->extremes['max'];
    }

    /**
     * @return array
     * @throws Exception\TransportException
     */
    public function totals()
    {
        $this->init();
        return $this->totals;
    }

    /**
     *
     */
    public function dumpRaw()
    {
        print_r($this->_rawData);
    }

    /**
     *
     */
    public function dump()
    {
        $this->_request->dump();
        $this->response()->dump();
    }

    /**
     * @return bool|int
     * @throws Exception\TransportException
     */
    public function countAll()
    {
        $this->init();
        return $this->rows_before_limit_at_least;
    }

    /**
     * @param bool $key
     * @return array|mixed|null
     * @throws Exception\TransportException
     */
    public function statistics($key = false)
    {
        $this->init();
        if ($key)
        {
            if (!is_array($this->statistics)) {
                return null;
            }
            if (!isset($this->statistics[$key])) {
                return null;
            }
            return $this->statistics[$key];
        }
        return $this->statistics;
    }

    /**
     * @return int
     * @throws Exception\TransportException
     */
    public function count()
    {
        $this->init();
        return $this->rows;
    }

    /**
     * @return mixed|string
     * @throws Exception\TransportException
     */
    public function rawData()
    {
        if ($this->_init) {
            return $this->_rawData;
        }

        $this->check();

        return $this->response()->rawDataOrJson($this->format);
    }

    /**
     * @param string $key
     * @return mixed|null
     * @throws Exception\TransportException
     */
    public function fetchOne($key = '')
    {
        $this->init();

        if (isset($this->array_data[0])) {
            if ($key) {
                if (isset($this->array_data[0][$key])) {
                    return $this->array_data[0][$key];
                } else {
                    return null;
                }
            }

            return $this->array_data[0];
        }

        return null;
    }

    /**
     * @param string|null $path
     * @return array
     * @throws Exception\TransportException
     */
    public function rowsAsTree($path)
    {
        $this->init();

        $out = [];
        foreach ($this->array_data as $row) {
            $d = $this->array_to_tree($row, $path);
            $out = array_replace_recursive($d, $out);
        }

        return $out;
    }

    /**
     * Return size_upload,upload_content,speed_upload,time_request
     *
     * @return array
     * @throws Exception\TransportException
     */
    public function info_upload()
    {
        $this->check();
        return [
            'size_upload'    => $this->response()->size_upload(),
            'upload_content' => $this->response()->upload_content_length(),
            'speed_upload'   => $this->response()->speed_upload(),
            'time_request'   => $this->response()->total_time()
        ];
    }

    /**
     * Return size_upload,upload_content,speed_upload,time_request,starttransfer_time,size_download,speed_download
     *
     * @return array
     * @throws Exception\TransportException
     */
    public function info()
    {
        $this->check();
        return [
            'starttransfer_time'    => $this->response()->starttransfer_time(),
            'size_download'    => $this->response()->size_download(),
            'speed_download'    => $this->response()->speed_download(),
            'size_upload'    => $this->response()->size_upload(),
            'upload_content' => $this->response()->upload_content_length(),
            'speed_upload'   => $this->response()->speed_upload(),
            'time_request'   => $this->response()->total_time()
        ];
    }

    /**
     * get format in sql
     * @return mixed
     */
    public function getFormat()
    {
        return $this->format;
    }

    /**
     * @return array
     * @throws Exception\TransportException
     */
    public function getMeta()
    {
        $this->init();
        return $this->meta;
    }

    /**
     * @return array
     * @throws Exception\TransportException
     */
    public function rows()
    {
        $this->init();
        return $this->array_data;
    }

    /**
     * @param array|string $arr
     * @param null|string|array $path
     * @return array
     */
    private function array_to_tree($arr, $path = null)
    {
        if (is_array($path)) {
            $keys = $path;
        } else {
            $args = func_get_args();
            array_shift($args);

            if (sizeof($args) < 2) {
                $separator = '.';
                $keys = explode($separator, $path);
            } else {
                $keys = $args;
            }
        }

        //
        $tree = $arr;
        while (count($keys)) {
            $key = array_pop($keys);

            if (isset($arr[$key])) {
                $val = $arr[$key];
            } else {
                $val = $key;
            }

            $tree = array($val => $tree);
        }
        if (!is_array($tree)) {
            return [];
        }
        return $tree;
    }
}
