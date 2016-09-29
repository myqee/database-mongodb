<?php
namespace MyQEE\Database\MongoDB;

use \Exception;
use \ArrayIterator;
use \MyQEE\Database\Expression;

/**
 * 数据库Mongo驱动
 *
 * @author     呼吸二氧化碳 <jonwang@myqee.com>
 * @category   Database
 * @package    Driver
 * @subpackage MongoDB
 * @copyright  Copyright (c) 2008-2015 myqee.com
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
     * 记录当前连接所对应的数据库
     * @var array
     */
    protected static $currentDatabases = [];

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


    protected function doConnect()
    {
        if ($this->tryUseExistsConnection())
        {
            return;
        }

        $database = $hostname = $port = $username = $password = $persistent = $readpreference = null;
        extract($this->config['connection']);

        # 错误服务器
        static $error_host = array();

        $last_error = null;
        while (true)
        {
            $hostname = $this->getRandClusterHost($error_host);
            if (false === $hostname)
            {
                if (INCLUDE_MYQEE_CORE && IS_DEBUG)Core::debug()->warn($error_host, 'error_host');

                if ($last_error && $last_error instanceof Exception)throw $last_error;
                throw new Exception(__('connect mongodb server error.'));
            }

            $connectionId = $this->getConnectionHash($hostname, $port, $username);
            static::$currentConnectionIdToHostname[$connectionId] = $hostname.':'.$port;

            try
            {
                $time = microtime(true);

                $options = [
                    'slaveOkay' => true,        //在从数据库可以查询，避免出现 Cannot run command count(): not master 的错误
                ];

                // 长连接设计
                if ($persistent)
                {
                    $options['persist'] = is_string($persistent) ? $persistent : 'x';
                }

                static $check = null;

                if (null === $check)
                {
                    $check = true;

                    if (!class_exists('\\MongoClient', false))
                    {
                        if (class_exists('\\Mongo', false))
                        {
                            throw new Exception(__('your mongoclient version is too low.'));
                        }
                        else
                        {
                            throw new Exception(__('You do not have to install mongodb extension,see http://php.net/manual/zh/mongo.installation.php'));
                        }
                    }
                }

                $error_code = 0;
                try
                {
                    if ($username)
                    {
                        $tmpLink = new \MongoClient("mongodb://{$username}:{$password}@{$hostname}:{$port}/", $options);
                    }
                    else
                    {
                        $tmpLink = new \MongoClient("mongodb://{$hostname}:{$port}/", $options);
                    }
                }
                catch (Exception $e)
                {
                    $error_code = $e->getCode();
                    $tmpLink    = false;
                }

                if (false === $tmpLink)
                {
                    if (INCLUDE_MYQEE_CORE && IS_DEBUG)
                    {
                        throw $e;
                    }
                    else
                    {
                        $error_msg = 'connect mongodb server error.';
                    }
                    throw new Exception($error_msg, $error_code);
                }

                if (null !== $readpreference)
                {
                    $tmpLink->setReadPreference($readpreference);
                }

                Core::debug()->info('MongoDB '. ($username ? $username .'@' : ''). $hostname .':'. $port .' connection time:' . (microtime(true) - $time));

                # 连接ID
                $this->connectionIds[$this->connectionType] = $connectionId;
                static::$connectionInstance[$connectionId]  = $tmpLink;

                unset($tmpLink);

                break;
            }
            catch (Exception $e)
            {
                if (INCLUDE_MYQEE_CORE && IS_DEBUG)
                {
                    Core::debug()->error(($username?$username.'@':'').$hostname.':'.$port, 'connect mongodb server error');
                    $last_error = new Exception($e->getMessage(), $e->getCode());
                }
                else
                {
                    $last_error = new Exception('connect mongodb server error', $e->getCode());
                }

                if (!in_array($hostname, $error_host))
                {
                    $error_host[] = $hostname;
                }
            }
        }
    }

    /**
     * @return bool
     * @throws Exception
     */
    protected function tryUseExistsConnection()
    {
        # 检查下是否已经有连接连上去了
        if (static::$connectionInstance)
        {
            $hostname = $this->config['connection']['hostname'];

            if (is_array($hostname))
            {
                $hostConfig = $hostname[$this->connectionType];

                if (!$hostConfig)
                {
                    throw new Exception('指定的数据库连接主从配置中('.$this->connectionType.')不存在，请检查配置');
                }

                if (!is_array($hostConfig))
                {
                    $hostConfig = [$hostConfig];
                }
            }
            else
            {
                $hostConfig = [$hostname];
            }

            # 先检查是否已经有相同的连接连上了数据库
            foreach ($hostConfig as $host)
            {
                $connectionId = $this->getConnectionHash($host, $this->config['connection']['port'], $this->config['connection']['username']);

                if (isset(static::$connectionInstance[$connectionId]))
                {
                    $this->connectionIds[$this->connectionType] = $connectionId;

                    return true;
                }
            }
        }

        return false;
    }

    /**
     * 关闭链接
     */
    public function closeConnect()
    {
        if ($this->connectionIds)foreach ($this->connectionIds as $key => $connectionId)
        {
            if ($connectionId && static::$connectionInstance[$connectionId])
            {
                $id = static::$currentConnectionIdToHostname[$connectionId];

                # 销毁对象
                static::$connectionInstance[$connectionId]    = null;
                static::$connectionInstanceDB[$connectionId] = null;

                unset(static::$connectionInstance[$connectionId]);
                unset(static::$connectionInstanceDB[$connectionId]);
                unset(static::$currentDatabases[$connectionId]);
                unset(static::$currentCharset[$connectionId]);
                unset(static::$currentConnectionIdToHostname[$connectionId]);

                if (INCLUDE_MYQEE_CORE && IS_DEBUG)Core::debug()->info('close '. $key .' mongo '. $id .' connection.');
            }

            $this->connectionIds[$key] = null;
        }
    }

    /**
     * 切换表
     *
     * @param string $database Database
     * @return void
     */
    public function selectDatabase($database)
    {
        if (!$database)return;

        $connectionId = $this->connectionId();

        if (!$connectionId || !isset(static::$currentDatabases[$connectionId]) || $database !== static::$currentDatabases[$connectionId])
        {
            if (!static::$connectionInstance[$connectionId])
            {
                $this->connect();
                $this->selectDatabase($database);
                return;
            }

            $connection = static::$connectionInstance[$connectionId]->selectDB($database);
            if (!$connection)
            {
                throw new Exception('选择Mongo数据表错误');
            }
            else
            {
                static::$connectionInstanceDB[$connectionId] = $connection;
            }

            if (INCLUDE_MYQEE_CORE && IS_DEBUG)
            {
                Core::debug()->log('mongodb change to database:'. $database);
            }

            # 记录当前已选中的数据库
            static::$currentDatabases[$connectionId] = $database;
        }
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
                $sql['distinct'] = $builder['distinct'];
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
                            if ($v==='COUNT(1) AS `total_row_count`')
                            {
                                $sql['total_count'] = true;
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
            if ($builder['order_by'])
            {
                foreach ($builder['order_by'] as $item)
                {
                    $sql['sort'][$item[0]] = $item[1] === 'DESC' ? -1 : 1;
                }
            }

            // group by
            if ($builder['group_by'])
            {
                $sql['group_by'] = $builder['group_by'];
            }

            // 高级查询条件
            if ($builder['select_adv'])
            {
                $sql['select_adv'] = $builder['select_adv'];

                // 分组统计
                if (!$builder['group_by'])
                {
                    $sql['group_by'] = ['0'];
                }
            }

            if ($builder['group_concat'])
            {
                $sql['group_concat'] = $builder['group_concat'];

                // 分组统计
                if (!$builder['group_by'])
                {
                    $sql['group_by'] = ['0'];
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

    }

    /**
     * 执行查询
     *
     * 目前支持插入、修改、保存（类似mysql的replace）查询
     *
     * $use_connection_type 默认不传为自动判断，可传true/false,若传字符串(只支持a-z0-9的字符串)，则可以切换到另外一个连接，比如传other,则可以连接到$this->_connection_other_id所对应的ID的连接
     *
     * @param array $options
     * @param string $asObject 是否返回对象
     * @param boolean $clusterName 是否使用主数据库，不设置则自动判断
     * @return Result
     */
    public function query($options, $asObject = null, $clusterName = null)
    {
        if (INCLUDE_MYQEE_CORE && IS_DEBUG)Core::debug()->log($options);

        if (is_string($options))
        {
            # 设置连接类型
            $this->setConnectionType($clusterName);

            # 必需数组
            if (!is_array($asObject))$asObject = [];

            # 执行字符串式查询语句
            return $this->connection()->execute($options, $asObject);
        }

        $clusterName = $this->getQueryType($options, $clusterName);

        # 设置连接类型
        $this->setConnectionType($clusterName);

        # 连接数据库
        $connection = $this->connection();

        if (!$options['table'])
        {
            throw new Exception('查询条件中缺少Collection');
        }

        $tableName = $this->config['table_prefix'] . $options['table'];

        if(INCLUDE_MYQEE_CORE && IS_DEBUG)
        {
            static $isSqlDebug = null;

            if (null === $isSqlDebug) $isSqlDebug = (bool)Core::debug()->profiler('sql')->isOpen();

            if ($isSqlDebug)
            {
                $host      = $this->getHostnameByConnectionHash($this->connectionId());
                $benchmark = Core::debug()->profiler('sql')->start('Database', 'mongodb://'.($host['username']?$host['username'].'@':'') . $host['hostname'] . ($host['port'] && $host['port'] != '27017' ? ':' . $host['port'] : ''));
            }
        }

        $explain = null;

        try
        {
            switch ($clusterName)
            {
                case 'SELECT':

                    if ($options['group_by'])
                    {
                        $aliasKey = [];

                        $select = $options['select'];
                        # group by
                        $groupOpt = [];
                        if (1 === count($options['group_by']))
                        {
                            $k = current($options['group_by']);
                            $groupOpt['_id'] = '$'.$k;
                            if (!isset($select[$k]))$select[$k] = 1;
                        }
                        else
                        {
                            $groupOpt['_id'] = [];
                            foreach ($options['group_by'] as $item)
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

                        $lastQuery = 'db.'. $tableName .'.aggregate(';
                        $ops       = [];
                        if ($options['where'])
                        {
                            $lastQuery .= '{$match:'.json_encode($options['where']).'}';
                            $ops[] = [
                                '$match' => $options['where']
                            ];
                        }

                        $groupOpt['_count'] = ['$sum' => 1];
                        if ($select)
                        {
                            foreach ($select as $k=>$v)
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
                                        $groupOpt[$v] = ['$first'=>'$'.$k];
                                    }
                                }
                            }
                        }

                        // 处理高级查询条件
                        if ($options['select_adv'])foreach ($options['select_adv'] as $item)
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

                        if ($options['group_concat'])foreach($options['group_concat'] as $item)
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

                        if ($options['distinct'])
                        {
                            # 唯一值

                            # 需要先把相应的数据$addToSet到一起
                            $groupOpt['_distinct_'.$options['distinct']] = [
                                '$addToSet' => '$' . $options['distinct'],
                            ];

                            $ops[] = [
                                '$group' => $groupOpt,
                            ];

                            $lastQuery .= ', {$group:'.json_encode($groupOpt).'}';


                            $ops[] = [
                                '$unwind' => '$_distinct_'.$options['distinct']
                            ];

                            $lastQuery .= ', {$unwind:"$_distinct_'.$options['distinct'].'"}';

                            $groupDistinct = [];

                            # 将原来的group的数据重新加进来
                            foreach($groupOpt as $k => $v)
                            {
                                # 临时统计的忽略
                                if ($k=='_distinct_'.$options['distinct'])continue;

                                if ($k=='_id')
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

                            $ops[] = [
                                '$group' => $groupDistinct
                            ];
                            $lastQuery .= ', {$group:'. json_encode($groupDistinct) .'}';
                        }
                        else
                        {
                            $ops[] = [
                                '$group' => $groupOpt,
                            ];

                            $lastQuery .= ', {$group:'.json_encode($groupOpt).'}';
                        }

                        if (isset($options['sort']) && $options['sort'])
                        {
                            $ops[]['$sort'] = $options['sort'];
                            $lastQuery .= ', {$sort:'.json_encode($options['sort']).'}';
                        }

                        if (isset($options['skip']) && $options['skip'] > 0)
                        {
                            $ops[]['$skip'] = $options['skip'];
                            $lastQuery .= ', {$skip:'.$options['skip'].'}';
                        }

                        if (isset($options['limit']) && $options['limit'] > 0)
                        {
                            $ops[]['$limit'] = $options['limit'];
                            $lastQuery .= ', {$limit:'.$options['limit'].'}';
                        }

                        $lastQuery .= ')';

                        $result = $connection->selectCollection($tableName)->aggregate($ops);

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

                            if ($options['total_count'])
                            {
                                foreach ($result as &$item)
                                {
                                    $item['total_count'] = $item['_count'];
                                }
                            }
                            $count = count($result);

                            $rs = new Result(new ArrayIterator($result), $options, $asObject, $this->config);
                        }
                        else
                        {
                            throw new Exception($result['errmsg'].'.query: '.$lastQuery);
                        }
                    }
                    else if ($options['distinct'])
                    {
                        # 查询唯一值
                        $result = $connection->command([
                            'distinct' => $tableName,
                            'key'      => $options['distinct'] ,
                            'query'    => $options['where']
                        ]);

                        $lastQuery = 'db.'. $tableName .'.distinct('.$options['distinct'].', '.json_encode($options['where']).')';

                        if(INCLUDE_MYQEE_CORE && IS_DEBUG && $isSqlDebug)
                        {
                            $count = count($result['values']);
                        }

                        if ($result && $result['ok']==1)
                        {
                            $rs = new Result(new ArrayIterator($result['values']), $options, $asObject, $this->config);
                        }
                        else
                        {
                            throw new Exception($result['errmsg']);
                        }
                    }
                    else
                    {
                        $lastQuery  = 'db.'. $tableName .'.find(';
                        $lastQuery .= $options['where'] ? json_encode($options['where']) : '{}';
                        $lastQuery .= $options['select'] ? ', '.json_encode($options['select']) : '';
                        $lastQuery .= ')';

                        $result = $connection->selectCollection($tableName)->find($options['where'], (array)$options['select']);

                        if(INCLUDE_MYQEE_CORE && IS_DEBUG && $isSqlDebug)
                        {
                            $explain = $result->explain();
                            $count   = $result->count();
                        }

                        if ($options['total_count'])
                        {
                            $lastQuery .= '.count()';
                            $result     = $result->count();
                            # 仅统计count
                            $rs         = new Result(new ArrayIterator(array(array('total_row_count'=>$result))), $options, $asObject, $this->config);
                        }
                        else
                        {
                            if ($options['sort'])
                            {
                                $lastQuery .= '.sort('.json_encode($options['sort']).')';
                                $result     = $result->sort($options['sort']);
                            }

                            if ($options['skip'])
                            {
                                $lastQuery .= '.skip('.json_encode($options['skip']).')';
                                $result     = $result->skip($options['skip']);
                            }

                            if ($options['limit'])
                            {
                                $lastQuery .= '.limit('.json_encode($options['limit']).')';
                                $result     = $result->limit($options['limit']);
                            }

                            $rs = new Result($result, $options, $asObject, $this->config);
                        }
                    }

                    break;

                case 'UPDATE':
                    $result = $connection->selectCollection($tableName)->update($options['where'], $options['data'], $options['options']);
                    $count = $rs = $result['n'];
                    $lastQuery = 'db.'.$tableName.'.update('.json_encode($options['where']).','.json_encode($options['data']).')';
                    break;

                case 'SAVE':
                case 'INSERT':
                case 'BATCHINSERT':
                    $fun = strtolower($clusterName);
                    $result = $connection->selectCollection($tableName)->$fun($options['data'], $options['options']);

                    if ($clusterName === 'BATCHINSERT')
                    {
                        $count = count($options['data']);
                        # 批量插入
                        $rs = array
                        (
                            '',
                            $count,
                        );
                    }
                    elseif (isset($result['data']['_id']) && $result['data']['_id'] instanceof \MongoId)
                    {
                        $count = 1;
                        $rs = [
                            (string)$result['data']['_id'] ,
                            1 ,
                        ];
                    }
                    else
                    {
                        $count = 0;
                        $rs = [
                            '',
                            0,
                        ];
                    }

                    if ($clusterName === 'BATCHINSERT')
                    {
                        $lastQuery = '';
                        foreach ($options['data'] as $d)
                        {
                            $lastQuery .= 'db.'.$tableName.'.insert('.json_encode($d).');'."\n";
                        }
                        $lastQuery = trim($lastQuery);
                    }
                    else
                    {
                        $lastQuery = 'db.'.$tableName.'.'.$fun.'('.json_encode($options['data']).')';
                    }
                    break;

                case 'REMOVE':
                    $result = $connection->selectCollection($tableName)->remove($options['where']);
                    $rs     = $result['n'];

                    $lastQuery = 'db.'.$tableName.'.remove('.json_encode($options['where']).')';
                    break;

                default:
                    throw new Exception('不支持的操作类型');
            }
        }
        catch (Exception $e)
        {
            if(INCLUDE_MYQEE_CORE && IS_DEBUG && isset($benchmark))
            {
                Core::debug()->profiler('sql')->stop();
            }

            throw $e;
        }

        $this->lastQuery = $lastQuery;

        # 记录调试
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
                if ($value instanceof \MongoCode)
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
                $value = '/'. substr($value,1);
            }
            else
            {
                $value = '/^'. $value;
            }

            if (substr($value, -1) === '%')
            {
                $value = substr($value, 0, -1) . '/i';
            }
            else
            {
                $value = $value .'$/i';
            }

            $value = str_replace('%', '*', $value);

            $option = new \MongoRegex($value);
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
     * @return  string
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
                    list ($column, $op, $value) = $condition;
                    $tmp_option = $this->compileSetData($op, $value);
                    $this->compilePasteData($tmpQuery, $tmp_option, $lastLogic, $logic, $column);

                    $lastLogic = $logic;
                }

            }
        }

        return $query;
    }

    protected function getQueryType($options, & $connectionType)
    {
        $type = strtoupper($options['type']);

        $slaveType = [
            'SELECT',
            'SHOW',
            'EXPLAIN'
        ];

        if (in_array($type, $slaveType))
        {
            if (true === $connectionType)
            {
                $connectionType = 'master';
            }
            else if (is_string($connectionType))
            {
                if (!preg_match('#^[a-z0-9_]+$#i', $connectionType))$connectionType = 'master';
            }
            else
            {
                $connectionType = 'slave';
            }
        }
        else
        {
            $connectionType = 'master';
        }

        return $type;
    }
}