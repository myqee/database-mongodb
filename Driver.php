<?php
namespace MyQEE\Database\MongoDB;

use \Exception;
use \ArrayIterator;
use \MyQEE\Database\Expression;

/**
 * 数据库 MongoDB 驱动, 兼容 php5 的 MongoClient
 *
 * @author     呼吸二氧化碳 <jonwang@myqee.com>
 * @category   Database
 * @package    Driver
 * @subpackage MongoDB
 * @copyright  Copyright (c) 2008-2017 myqee.com
 * @license    http://www.myqee.com/license.html
 */
class Driver extends \MyQEE\Database\Driver
{
    /**
     * 默认端口
     *
     * @var int
     */
    protected $defaultPort = 27017;

    /**
     * 当前所在表
     *
     * @var string
     */
    protected $database = 'test';

    /**
     * 记录当前数据库所对应的页面编码
     * @var array
     */
    protected static $currentCharset = [];

    /**
     * 链接寄存器
     * @var array
     */
    protected static $connectionInstance = [];

    /**
     * DB链接寄存器
     *
     * @var array
     */
    protected static $connectionInstanceDB = [];

    /**
     * 记录connection id所对应的hostname
     *
     * @var array
     */
    protected static $currentConnectionIdToHostname = [];

    /**
     * 是否新的 MongoDB 驱动
     *
     * @var bool
     */
    protected static $isMongoDB;

    /**
     * 检查连接（每5秒钟间隔才检测）
     *
     * @param $id
     * @param int $limit 时间间隔（秒）, 0 表示一直检查
     * @return bool
     */
    protected function checkConnect($id, $limit = 5)
    {
        return true;
    }

    protected function doConnect(array & $config)
    {
        static $check = null;

        if (Func::isMongoDB())
        {
            $class = '\\MongoDB\\Driver\\Manager';
        }
        else
        {
            $class = '\\MongoClient';
        }

        if (null === $check)
        {
            $check = true;

            if (!class_exists($class, false))
            {
                throw new Exception('You do not have to install MongoDB extension, see http://php.net/manual/zh/mongodb.installation.php');
            }
        }

        $options = [
            'slaveOkay'       => true,        //在从数据库可以查询，避免出现 Cannot run command count(): not master 的错误
            'socketTimeoutMS' => 60000        //连接超时，默认60s
        ];

        // 长连接设计
        if ($config['persistent'])
        {
            $options['persist'] = is_string($config['persistent']) ? $config['persistent'] : 'x';
        }

        if ($config['username'])
        {
            $tmpLink = new $class("mongodb://{$config['username']}:{$config['password']}@{$config['hostname']}:{$config['port']}/", $options);
        }
        else
        {
            $tmpLink = new $class("mongodb://{$config['hostname']}:{$config['port']}/", $options);
        }

        if (!isset($config['readPreference']))
        {
            /**
             * @var \MongoDB\Driver\Manager $tmpLink
             */
            $config['readPreference'] = $tmpLink->getReadPreference();
        }

        if (isset($config['database']) && $config['database'])
        {
            # 切换库
            $this->selectDB($config['database']);
        }
        
        # 前缀
        if (!isset($config['table_prefix']) {
            $config['table_prefix'] = '';
        }

        # Core::debug()->info('MongoDB '. ($username ? $username .'@' : ''). $hostname .':'. $port .' connection time:' . (microtime(true) - $time));

        return $tmpLink;
    }


    /**
     * 连接数据库
     *
     * @param string $clusterName 集群名称（例如 slave, master）, 不设置则使用当前设置
     * @throws \MyQEE\Database\Exception
     */
    public function connect($clusterName = null)
    {
        $id   = parent::connect($clusterName);
        $link =& $this->connections[$id];

        # 构造一个 Func 兼容处理对象
        $link['func'] = new Func($link['resource'], $link['database']);

        return $id;
    }

    /**
     * 返回一个兼容方法的连接对象
     *
     * @param null $clusterName
     * @return Func
     */
    public function connectionFunc($clusterName = null)
    {
        parent::connection($clusterName);
        return $this->connections[$this->connectionId]['func'];
    }

    /**
     * 关闭链接
     */
    public function closeConnect()
    {
        if ($this->connections)
        {
            $this->connectionId = null;
            foreach ($this->connections as $link)
            {
                $resource = $link['resource'];
                if (method_exists($resource, 'close'))
                {
                    $resource->close();
                }
            }
            $this->connections  = [];
        }
    }

    /**
     * 切换表
     *
     * @param string $database Database
     * @return bool
     */
    public function selectDB($database)
    {
        if (!$database)return false;

        if (!$this->connectionId)return false;

        $connection = $this->connections[$this->connectionId];

        if ($connection['database'] !== $database)
        {
            $this->connections[$this->connectionId]['database'] = $database;
            $this->database = $database;
        }

        return true;
    }

    public function compile($builder, $type = 'select')
    {
        $where = [];

        if (!empty($builder['where']))
        {
            $where = $this->compileConditions($builder['where']);
        }

        if ($type === 'insert_update')
        {
            $type = 'replace';
        }

        if ($type === 'insert')
        {
            $sql = [
                'type'    => 'insert',
                'table'   => $builder['table'],
                'options' => ['safe' => true],
            ];

            if (count($builder['values']) > 1)
            {
                # 批量插入
                $sql['type'] = 'batchinsert';

                $data = [];
                foreach ($builder['columns'] as $field)
                {
                    foreach ($builder['values'] as $k => $v)
                    {
                        $data[$k][$field] = $builder['values'][$k][$field];
                    }
                }
                $sql['data'] = $data;
            }
            else
            {
                # 单条插入
                $data = [];
                foreach ($builder['columns'] as $field)
                {
                    $data[$field] = $builder['values'][0][$field];
                }
                $sql['data'] = $data;
            }
        }
        elseif ($type == 'update' || $type == 'replace')
        {
            $sql = array
            (
                'type'    => 'update',
                'table'   => $builder['table'],
                'where'   => $where,
                'options' => [
                    'multiple' => true,
                    'safe'     => true,
                ],
            );

            foreach ($builder['set'] as $item)
            {
                list ($key, $value, $op) = $item;
                if ($op === '+')
                {
                    # 递增
                    $op = '$inc';
                }
                elseif ($op === '-')
                {
                    # 递减
                    $value = -$value;
                    $op    = '$inc';
                }
                elseif (null === $value)
                {
                    # 如果值是 null, 则 unset 此字段
                    $op    = '$unset';
                    $value = '';
                }
                else
                {
                    # 设置字段
                    $op = '$set';
                }

                $sql['data'][$op][$key] = $value;
            }

            # 全部替换的模式
            if ($type === 'replace')
            {
                $sql['options']['upsert'] = true;

                if (isset($sql['data']['$set']) && $set = $sql['data']['$set'])
                {
                    unset($sql['data']['$set']);
                    $sql['data'] = array_merge($sql['data'], $set);
                }
            }
        }
        elseif ($type == 'delete')
        {
            $sql = [
                'type'  => 'remove',
                'table' => $builder['table'],
                'where' => $where,
            ];
        }
        else
        {
            $sql = [
                'type'  => $type,
                'table' => $builder['from'][0],
                'where' => $where,
                'limit' => $builder['limit'],
                'skip'  => $builder['offset'],
            ];

            if ($builder['distinct'])
            {
                $sql['distinct'] = $builder['distinct'] === true ? '_id' : $builder['distinct'];
            }

            // 查询
            if ($builder['select'])
            {
                $s = [];
                foreach ($builder['select'] as $item)
                {
                    if (is_string($item))
                    {
                        $item = trim($item);
                        if (preg_match('#^(.*) as (.*)$#i', $item , $m))
                        {
                            $s[$m[1]] = $m[2];
                            $sql['select_as'][$m[1]] = $m[2];
                        }
                        else
                        {
                            $s[$item] = 1;
                        }
                    }
                    elseif (is_object($item))
                    {
                        if ($item instanceof Expression)
                        {
                            $v = $item->value();
                            if ($v === 'COUNT(1) AS `total_row_count`')
                            {
                                $sql['totalCount'] = true;
                            }
                            else
                            {
                                $s[$v] = 1;
                            }
                        }
                        elseif (method_exists($item, '__toString'))
                        {
                            $s[(string)$item] = 1;
                        }
                    }
                }

                $sql['select'] = $s;
            }

            // 排序
            if (isset($builder['orderBy']) && $builder['orderBy'])
            {
                foreach ($builder['orderBy'] as $item)
                {
                    $sql['sort'][$item[0]] = $item[1] === 'DESC' ? -1 : 1;
                }
            }

            // group by
            if (isset($builder['groupBy']) && $builder['groupBy'])
            {
                $sql['groupBy'] = $builder['groupBy'];
            }

            // 高级查询条件
            if (isset($builder['selectAdv']) && $builder['selectAdv'])
            {
                $sql['selectAdv'] = $builder['selectAdv'];

                // 分组统计
                if (!$builder['groupBy'])
                {
                    $sql['groupBy'] = ['0'];
                }
            }

            if (isset($builder['groupConcat']) && $builder['groupConcat'])
            {
                $sql['groupConcat'] = $builder['groupConcat'];

                // 分组统计
                if (!$builder['groupBy'])
                {
                    $sql['groupBy'] = ['0'];
                }
            }

        }

        return $sql;
    }

    public function setCharset($charset)
    {

    }

    public function escape($value)
    {
        return $value;
    }

    /**
     * MongoDB 不需要处理
     *
     * @param mixed $value
     * @return mixed|string
     */
    public function quote($value)
    {
        return $value;
    }

    /**
     * 执行构造语法执行
     *
     * @param string $statement
     * @param array $inputParameters
     * @param null|bool|string $asObject
     * @param null|bool|string $connectionType
     * @return Result
     */
    public function execute($statement, array $inputParameters, $asObject = null, $connectionType = null)
    {
        throw new Exception('MongoDB statement not support.');
    }

    /**
     * 执行查询
     *
     * 目前支持插入、修改、保存（类似mysql的replace）查询
     *
     * @param array $options
     * @param string $asObject 是否返回对象, 若 $options 为一个字符, 则此参数可以传 array
     * @param string $clusterName 集群名称（例如 slave, master）, 不设置则使用当前设置
     * @return Result
     */
    public function query($options, $asObject = null, $clusterName = null)
    {
        //if (INCLUDE_MYQEE_CORE && IS_DEBUG)
        //{
        //    Core::debug()->log($options);
        //}

        $queryType = $this->getQueryType($options, $clusterName);

        /**
         * @var Func $connection
         */
        $connection = $this->connectionFunc($clusterName);

        if (is_string($options))
        {
            # 必需数组
            if (!is_array($asObject))$asObject = [];

            return $connection->command($options, $asObject);
        }

        if (!$options['table'])
        {
            throw new Exception('查询条件中缺少Collection');
        }

        $collection = $this->config['table_prefix'] . $options['table'];

        // TODO
        //if(INCLUDE_MYQEE_CORE && IS_DEBUG)
        //{
        //    static $isSqlDebug = null;
        //
        //    if (null === $isSqlDebug) $isSqlDebug = (bool)Core::debug()->profiler('sql')->isOpen();
        //
        //    if ($isSqlDebug)
        //    {
        //        $host      = $this->getHostnameByConnectionHash($this->connectionId());
        //        $benchmark = Core::debug()->profiler('sql')->start('Database', 'mongodb://'.($host['username']?$host['username'].'@':'') . $host['hostname'] . ($host['port'] && $host['port'] != '27017' ? ':' . $host['port'] : ''));
        //    }
        //}

        $explain = null;

        try
        {
            switch($queryType)
            {
                case 'SELECT':

                    if (isset($options['groupBy']) && $options['groupBy'])
                    {
                        $aliasKey = [];

                        $select = $options['select'];
                        # group by
                        $groupOpt = [];
                        if (1 === count($options['groupBy']))
                        {
                            $k = current($options['groupBy']);
                            $groupOpt['_id'] = '$'.$k;
                            if (!isset($select[$k]))$select[$k] = 1;
                        }
                        else
                        {
                            $groupOpt['_id'] = [];
                            foreach ($options['groupBy'] as $item)
                            {
                                if (false !== strpos($item, '.'))
                                {
                                    $key                   = str_replace('.', '->', $item);
                                    $groupOpt['_id'][$key] = '$'.$item;
                                    $aliasKey[$key]        = $item;
                                }
                                else
                                {
                                    $groupOpt['_id'][$item] = '$'.$item;
                                }

                                if (!isset($select[$item]))$select[$item] = 1;
                            }
                        }

                        $pipeline = [];
                        if ($options['where'])
                        {
                            $pipeline[] = [
                                '$match' => $options['where']
                            ];
                        }

                        $groupOpt['_count'] = ['$sum' => 1];
                        if ($select)
                        {
                            foreach ($select as $k => $v)
                            {
                                if (1 === $v || true === $v)
                                {
                                    if (false !== strpos($k, '.'))
                                    {
                                        $key            = str_replace('.', '->', $k);
                                        $groupOpt[$key] = ['$first' => '$'.$k];
                                        $aliasKey[$key] = $k;
                                    }
                                    else
                                    {
                                        $groupOpt[$k] = ['$first' => '$'.$k];
                                    }
                                }
                                else
                                {
                                    if (false !== strpos($v, '.'))
                                    {
                                        $key            = str_replace('.', '->', $k);
                                        $groupOpt[$key] = ['$first' => '$'.$k];
                                        $aliasKey[$key] = $k;
                                    }
                                    else
                                    {
                                        $groupOpt[$v] = ['$first' => '$'.$k];
                                    }
                                }
                            }
                        }

                        // 处理高级查询条件
                        if ($options['selectAdv'])foreach ($options['selectAdv'] as $item)
                        {
                            if (!is_array($item))continue;

                            if (is_array($item[0]))
                            {
                                $column = $item[0][0];
                                $alias  = $item[0][1];
                            }
                            else if (preg_match('#^(.*) AS (.*)$#i', $item[0] , $m))
                            {
                                $column = $m[1];
                                $alias  = $m[2];
                            }
                            else
                            {
                                $column = $alias = $item[0];
                            }

                            if (false !== strpos($alias, '.'))
                            {
                                $arr              = explode('.', $alias);
                                $alias            = implode('->', $arr);
                                $aliasKey[$alias] = implode('.', $arr);
                                unset($arr);
                            }

                            switch ($item[1])
                            {
                                case 'max':
                                case 'min':
                                case 'avg':
                                case 'first':
                                case 'last':
                                    $groupOpt[$alias] = [
                                        '$'.$item[1] => '$'.$column,
                                    ];
                                    break;
                                case 'addToSet':
                                case 'concat':
                                    $groupOpt[$alias] = [
                                        '$addToSet' => '$'.$column,
                                    ];
                                    break;
                                case 'sum':
                                    $groupOpt[$alias] = [
                                        '$sum' => isset($item[2])?$item[2]:'$'.$column,
                                    ];
                                    break;
                            }
                        }

                        if (isset($options['groupConcat']) && $options['groupConcat'])foreach($options['groupConcat'] as $item)
                        {
                            if (is_array($item[0]))
                            {
                                $column = $item[0][0];
                                $alias  = $item[0][1];
                            }
                            else if (preg_match('#^(.*) AS (.*)$#i', $item[0] , $m))
                            {
                                $column = $m[1];
                                $alias  = $m[2];
                            }
                            else
                            {
                                $column = $alias = $item[0];
                            }

                            if (false !== strpos($alias, '.'))
                            {
                                $arr              = explode('.', $alias);
                                $alias            = implode('->', $arr);
                                $aliasKey[$alias] = implode('.', $arr);
                                unset($arr);
                            }

                            if (isset($item[3]) && $item[3])
                            {
                                $fun = '$addToSet';
                            }
                            else
                            {
                                $fun = '$push';
                            }

                            $groupOpt[$alias] = [
                                $fun => '$' . $column,
                            ];

                            if (isset($item[1]) && $item[1])
                            {
                                $groupOpt[$alias] = [
                                    '$sort' => [
                                        $column => strtoupper($item[1]) === 'DESC' ? -1 : 1,
                                    ]
                                ];
                            }
                        }

                        if (isset($options['distinct']) && $options['distinct'])
                        {
                            # 唯一值

                            # 需要先把相应的数据$addToSet到一起
                            $groupOpt['_distinct_'.$options['distinct']] = [
                                '$addToSet' => '$' . $options['distinct'],
                            ];

                            $pipeline[] = [
                                '$group' => $groupOpt,
                            ];


                            $pipeline[] = [
                                '$unwind' => '$_distinct_'.$options['distinct']
                            ];

                            $groupDistinct = [];

                            # 将原来的group的数据重新加进来
                            foreach($groupOpt as $k => $v)
                            {
                                # 临时统计的忽略
                                if ($k === '_distinct_'. $options['distinct'])continue;

                                if ($k === '_id')
                                {
                                    $groupDistinct[$k] = '$'.$k;
                                }
                                else
                                {
                                    $groupDistinct[$k] = ['$first' => '$'. $k];
                                }
                            }
                            $groupDistinct[$options['distinct']] = [
                                '$sum' => 1
                            ];

                            $pipeline[] = [
                                '$group' => $groupDistinct
                            ];
                        }
                        else
                        {
                            $pipeline[] = [
                                '$group' => $groupOpt,
                            ];
                        }

                        if (isset($options['sort']) && $options['sort'])
                        {
                            $pipeline[]['$sort'] = $options['sort'];
                        }

                        if (isset($options['skip']) && $options['skip'] > 0)
                        {
                            $pipeline[]['$skip'] = $options['skip'];
                        }

                        if (isset($options['limit']) && $options['limit'] > 0)
                        {
                            $pipeline[]['$limit'] = $options['limit'];
                        }

                        $opt = [
                            'allowDiskUse' => true,
                            'maxTimeMS'    => 60000
                        ];
                        if (isset($options['option']))
                        {
                            $opt = $options['option'] + $opt;
                        }

                        $lastQuery = 'db.'. $collection .'.aggregate(' . json_encode($pipeline, JSON_UNESCAPED_UNICODE) .')';
                        $result    = $connection->selectCollection($collection)->aggregate($pipeline, $opt);

                        // 兼容不同版本的aggregate返回
                        if ($result && ($result['ok'] == 1 || !isset($result['errmsg'])))
                        {
                            if ($result['ok'] == 1 && is_array($result['result']))$result = $result['result'];

                            if ($aliasKey)foreach ($result as &$item)
                            {
                                // 处理 _ID 字段
                                if (is_array($item['_id']))foreach ($item['_id'] as $k=>$v)
                                {
                                    if (false !== strpos($k, '->'))
                                    {
                                        $item['_id'][str_replace('->', '.', $k)] = $v;
                                        unset($item['_id'][$k]);
                                    }
                                }

                                // 处理 select 的字段
                                foreach($aliasKey as $k => $v)
                                {
                                    $item[$v] = $item[$k];
                                    unset($item[$k]);
                                }
                            }

                            if (isset($options['totalCount']) && $options['totalCount'])
                            {
                                foreach ($result as &$item)
                                {
                                    $item['totalCount'] = $item['_count'];
                                }
                            }
                            //$count = count($result);

                            $rs = new Result(new ArrayIterator($result), $options, $asObject, $this->convertToUtf8FromCharset);
                        }
                        else
                        {
                            throw new Exception($result['errmsg'].'.query: '.$lastQuery);
                        }
                    }
                    else if (isset($options['distinct']) && $options['distinct'])
                    {
                        # 查询唯一值
                        $lastQuery = 'db.'. $collection .'.distinct('.$options['distinct'].', '.json_encode($options['where'], JSON_UNESCAPED_UNICODE).')';
                        $result = $connection->command([
                            'distinct' => $collection,
                            'key'      => $options['distinct'] ,
                            'query'    => $options['where']
                        ]);


                        //if(INCLUDE_MYQEE_CORE && IS_DEBUG && $isSqlDebug)
                        //{
                        //    $count = count($result['values']);
                        //}

                        if ($result && $result['ok'] == 1)
                        {
                            $rs = new Result(new ArrayIterator($result['values']), $options, $asObject, $this->convertToUtf8FromCharset);
                        }
                        else
                        {
                            throw new Exception($result['errmsg']);
                        }
                    }
                    else
                    {
                        if (!isset($options['select'])) {
                            $options['select'] = '';
                        }

                        $lastQuery  = 'db.'. $collection .'.find(';
                        $lastQuery .= $options['where'] ? json_encode($options['where'], JSON_UNESCAPED_UNICODE) : '{}';
                        $lastQuery .= $options['select'] ? ', '. json_encode($options['select'], JSON_UNESCAPED_UNICODE) : '';
                        $lastQuery .= ')';

                        if (isset($options['totalCount']) && $options['totalCount'])
                        {
                            $lastQuery .= '.count()';
                            $result = $connection->selectCollection($collection)->count($options['where']);
                            # 仅统计count
                            $rs = new Result(new ArrayIterator([['total_row_count' => $result]]), $options, $asObject, $this->convertToUtf8FromCharset);
                        }
                        else
                        {
                            if (isset($options['option']))
                            {
                                $opt = $options['option'];
                            }
                            else
                            {
                                $opt = [];
                            }

                            if (isset($options['sort']) && $options['sort'])
                            {
                                $opt['sort'] = $options['sort'];
                                $lastQuery .= '.sort('. json_encode($options['sort']) .')';
                            }

                            if (isset($options['skip']) && $options['skip'])
                            {
                                $opt['skip'] = $options['skip'];
                                $lastQuery .= '.skip('. json_encode($options['skip']) .')';
                            }

                            if (isset($options['limit']) && $options['limit'])
                            {
                                $opt['limit'] = $options['limit'];
                                $lastQuery   .= '.limit('. json_encode($options['limit']) .')';
                            }
                            $result = $connection->selectCollection($collection)->find($options['where'], (array)$options['select'], 60000, $opt);

                            //if(IS_DEBUG && $isSqlDebug && !Func::isMongoDB())
                            //{
                            //    $explain = $result->explain();
                            //    $count   = $result->count();
                            //}

                            $rs = new Result($result, $options, $asObject, $this->convertToUtf8FromCharset);
                        }
                    }
                    break;

                case 'UPDATE':
                    $result = $connection->selectCollection($collection)->update($options['where'], $options['data'], $options['options']);
                    $count = $rs = $result['n'];
                    $lastQuery = 'db.'.$collection.'.update('. json_encode($options['where']) .','. json_encode($options['data']).')';
                    break;

                case 'INSERT':
                    $lastQuery = 'db.'. $collection .'.insert('. json_encode($options['data'], JSON_UNESCAPED_UNICODE) .')';
                    $rs        = $connection->selectCollection($collection)->insert($options['data'], $options['options']);
                    $count     = $rs[1];
                    break;

                case 'BATCHINSERT':
                    # 批量插入

                    $lastQuery = '';
                    foreach ($options['data'] as $d)
                    {
                        $lastQuery .= 'db.'. $collection .'.insert('. json_encode($d, JSON_UNESCAPED_UNICODE) .');'."\n";
                    }
                    $lastQuery = trim($lastQuery);

                    $result = $connection->selectCollection($collection)->batchInsert($options['data'], $options['options']);

                    $rs = array
                    (
                        '',
                        $result,
                    );
                    break;

                case 'REMOVE':
                    $rs = $count = $connection->selectCollection($collection)->remove($options['where']);

                    $lastQuery = 'db.'. $collection .'.remove('. json_encode($options['where'], JSON_UNESCAPED_UNICODE) .')';
                    break;

                default:
                    throw new Exception('不支持的操作类型');
            }
        }
        catch (Exception $e)
        {
            //if(INCLUDE_MYQEE_CORE && IS_DEBUG && isset($benchmark))
            //{
            //    Core::debug()->profiler('sql')->stop();
            //}

            throw $e;
        }

        $this->lastQuery = $lastQuery;

        # 记录调试
        /*
        if(INCLUDE_MYQEE_CORE && IS_DEBUG)
        {
            Core::debug()->info($lastQuery, 'MongoDB');

            if (isset($benchmark))
            {
                if ($isSqlDebug)
                {
                    $data = array();
                    $data[0]['db']              = $host['hostname'] . '/' . $this->config['connection']['database'] . '/';
                    $data[0]['cursor']          = '';
                    $data[0]['nscanned']        = '';
                    $data[0]['nscannedObjects'] = '';
                    $data[0]['n']               = '';
                    $data[0]['millis']          = '';
                    $data[0]['row']             = $count;
                    $data[0]['query']           = '';
                    $data[0]['nYields']         = '';
                    $data[0]['nChunkSkips']     = '';
                    $data[0]['isMultiKey']      = '';
                    $data[0]['indexOnly']       = '';
                    $data[0]['indexBounds']     = '';

                    if ($explain)
                    {
                        foreach ($explain as $k=>$v)
                        {
                            $data[0][$k] = $v;
                        }
                    }

                    $data[0]['query'] = $lastQuery;
                }
                else
                {
                    $data = null;
                }

                Core::debug()->profiler('sql')->stop($data);
            }
        }
        */

        return $rs;
    }

    /**
     * 创建一个数据库
     *
     * @param string $database
     * @param string $charset 编码，不传则使用数据库连接配置相同到编码
     * @param string $collate 整理格式
     * @return boolean
     * @throws Exception
     */
    public function createDatabase($database, $charset = null, $collate = null)
    {
        // mongodb 不需要手动创建，可自动创建

        return true;
    }

    /**
     * 返回是否支持对象数据
     *
     * @var bool
     */
    public function isSupportObjectValue()
    {
        return true;
    }

    protected function compileSetData($op, $value)
    {
        $op = strtolower($op);
        $opArr = [
            '>'  => 'gt',
            '>=' => 'gte',
            '<'  => 'lt',
            '<=' => 'lte',
            '!=' => 'ne',
            '<>' => 'ne',
        ];

        $option = [];

        if ($op === 'between' && is_array($value))
        {
            list ($min, $max) = $value;

            $option['$gte'] = $min;

            $option['$lte'] = $max;
        }
        elseif ($op === '=')
        {
            if (is_object($value))
            {
                if ($value instanceof \MongoDB\BSON\Javascript)
                {
                    $option['$where'] = $value;
                }
                elseif ($value instanceof \MongoCode)
                {
                    $option['$where'] = $value;
                }
                elseif ($value instanceof Expression)
                {
                    $option = $value->value();
                }
                else
                {
                    $option = $value;
                }
            }
            else
            {
                $option = $value;
            }
        }
        elseif ($op === 'in')
        {
            $option = ['$in' => $value];
        }
        elseif ($op === 'not in')
        {
            $option = ['$nin' => $value];
        }
        elseif ($op === 'mod')
        {
            if ($value[2] === '=')
            {
                $option = ['$mod' => [$value[0],$value[1]]];
            }
            elseif ($value[2] === '!=' || $value[2] === 'not')
            {
                $option = [
                    '$ne' => ['$mod' => [$value[0], $value[1]]]
                ];
            }
            elseif (substr($value[2], 0, 1) === '$')
            {
                $option = [
                    $value[2] => ['$mod' => [$value[0], $value[1]]]
                ];
            }
            elseif (isset($value[2]))
            {
                $option = [
                    '$'.$value[2] => ['$mod' => [$value[0],$value[1]]]
                ];
            }
        }
        elseif ($op === 'like')
        {
            // 将like转换成正则处理
            $value = preg_quote($value, '/');

            if (substr($value, 0, 1) === '%')
            {
                $value = substr($value, 1);
            }
            else
            {
                $value = '^'. $value;
            }

            if (substr($value, -1) === '%')
            {
                $value = substr($value, 0, -1);
            }
            else
            {
                $value = $value .'$';
            }

            $value = str_replace('%', '*', $value);

            if (Func::isMongoDB())
            {
                $option = new \MongoDB\BSON\Regex($value, 'i');
            }
            else
            {
                $option = new \MongoRegex("/$value/i");
            }
        }
        else
        {
            if (isset($opArr[$op]))
            {
                $option['$'.$opArr[$op]] = $value;
            }
        }

        return $option;
    }

    protected function compilePasteData(& $tmpQuery , $tmpOption , $lastLogic , $nowLogic , $column = null)
    {
        if ($lastLogic != $nowLogic)
        {
            // 当$and $or 不一致时，则把前面所有的条件合并为一条组成一个$and|$or的条件
            if ($column)
            {
                $tmpQuery = [$nowLogic => $tmpQuery ? [$tmpQuery, [$column=>$tmpOption]] : [[$column=>$tmpOption]]];
            }
            else
            {
                $tmpQuery = [$nowLogic => $tmpQuery ? [$tmpQuery, $tmpOption] : [$tmpOption]];
            }
        }
        elseif (isset($tmpQuery[$nowLogic]))
        {
            // 如果有 $and $or 条件，则加入
            if (is_array($tmpOption) || !$column)
            {
                $tmpQuery[$nowLogic][] = $tmpOption;
            }
            else
            {
                $tmpQuery[$nowLogic][] = [$column => $tmpOption];
            }
        }
        else if ($column)
        {
            if (isset($tmpQuery[$column]))
            {
                // 如果有相应的字段名，注，这里面已经不可能$logic=='$or'了
                if (is_array($tmpOption) && is_array($tmpQuery[$column]))
                {
                    // 用于合并类似 $tmp_query = array('field_1' => array('$lt'=>1));
                    // $tmp_option = array('field_1' => array('$gt' => 10)); 这种情况
                    // 最后的合并结果就是 array('field_1' => array('$lt' => 1, '$gt'=>10));
                    $needReset = false;
                    foreach ($tmpOption as $tmpK => $tmpV)
                    {
                        if (isset($tmpQuery[$column][$tmpK]))
                        {
                            $needReset = true;
                            break;
                        }
                    }

                    if ($needReset)
                    {
                        $tmpQueryBak = $tmpQuery;      // 给一个数据copy
                        $tmpQuery    = ['$and' => []]; // 清除$tmp_query

                        // 将条件全部加入$and里
                        foreach ($tmpQueryBak as $tmpK => $tmpV)
                        {
                            $tmpQuery['$and'][] = array($tmpK => $tmpV);
                        }
                        unset($tmpQueryBak);

                        // 新增加的条件也加入进去
                        foreach ($tmpOption as $tmpK => $tmpV)
                        {
                            $tmpQuery['$and'][] = [
                                $column => [$tmpK => $tmpV]
                            ];
                        }
                    }
                    else
                    {
                        // 无需重新设置数据则合并
                        foreach ($tmpOption as $tmpK => $tmpV)
                        {
                            $tmpQuery[$column][$tmpK] = $tmpV;
                        }
                    }

                }
                else
                {
                    $tmpQuery['$and'] = [
                        [$column => $tmpQuery[$column]],
                        [$column => $tmpOption],
                    ];
                    unset($tmpQuery[$column]);
                }
            }
            else
            {
                // 直接加入字段条件
                $tmpQuery[$column] = $tmpOption;
            }
        }
        else
        {
            $tmpQuery = array_merge($tmpQuery, $tmpOption);
        }

        return $tmpQuery;
    }

    /**
     * Compiles an array of conditions into an SQL partial. Used for WHERE
     * and HAVING.
     *
     * @param   array  $conditions condition statements
     * @return  array
     */
    protected function compileConditions(array $conditions)
    {
        $lastLogic    = '$and';
        $tmpQueryList = [];
        $query        = [];
        $tmpQuery     =& $query;

        foreach ($conditions as $group)
        {
            foreach ($group as $logic => $condition)
            {
                $logic = '$'. strtolower($logic);        //$or,$and

                if ($condition === '(')
                {
                    $tmpQueryList[] = [];                                              //增加一行数据
                    unset($tmpQuery);                                                  //删除引用关系，这样数据就保存在了$tmp_query_list里
                    $tmpQuery        =& $tmpQueryList[count($tmpQueryList) - 1];       //把指针移动到新的组里
                    $lastLogicList[] = $lastLogic;                                     //放一个备份
                    $lastLogic       = '$and';                                         //新组开启，把$last_logic设置成$and
                }
                elseif ($condition === ')')
                {
                    # 关闭一个组
                    $lastLogic = array_pop($lastLogicList);                            //恢复上一个$lastLogic

                    # 将最后一个移除
                    $tmpQuery2 = array_pop($tmpQueryList);

                    $c = count($tmpQueryList);
                    unset($tmpQuery);
                    if ($c)
                    {
                        $tmpQuery =& $tmpQueryList[$c-1];
                    }
                    else
                    {
                        $tmpQuery =& $query;
                    }
                    $this->compilePasteData($tmpQuery, $tmpQuery2, $lastLogic, $logic);

                    unset($tmpQuery2, $c);
                }
                else
                {
                    list($column, $op, $value) = $condition;
                    $tmp_option = $this->compileSetData($op, $value);
                    $this->compilePasteData($tmpQuery, $tmp_option, $lastLogic, $logic, $column);

                    $lastLogic = $logic;
                }

            }
        }

        return $query;
    }

    protected function getQueryType($options, & $clusterName)
    {
        $type = strtoupper($options['type']);

        $slaveType = [
            'SELECT',
            'SHOW',
            'EXPLAIN'
        ];

        if (in_array($type, $slaveType))
        {
            if (true === $clusterName)
            {
                $clusterName = 'master';
            }
            else if (is_string($clusterName))
            {
                if (!preg_match('#^[a-z0-9_]+$#i', $clusterName))$clusterName = 'master';
            }
            else
            {
                $clusterName = 'slave';
            }
        }
        else
        {
            $clusterName = 'master';
        }

        return $type;
    }

    /**
     * 获取一个MongoDB Server对象
     *
     * @return \MongoDB\Driver\Server
     */
    protected function getMongoDBServer()
    {
        if (isset($this->currentConnectionConfig['readPreference']))
        {
            $preference = $this->currentConnectionConfig['readPreference'];
            $preference = new \MongoDB\Driver\ReadPreference($preference);
        }
        else
        {
            $preference = null;
        }

        /**
         * @var \MongoDB\Driver\Manager $connection
         */
        return $connection->selectServer($preference);
    }
}
