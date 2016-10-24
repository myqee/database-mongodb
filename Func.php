<?php
namespace MyQEE\Database\MongoDB;

/**
 * 数据库兼容函数
 *
 * @author     呼吸二氧化碳 <jonwang@myqee.com>
 * @category   Database
 * @package    Driver
 * @subpackage MongoDB
 * @copyright  Copyright (c) 2008-2017 myqee.com
 * @license    http://www.myqee.com/license.html
 */
class Func
{
    /**
     * @var \MongoDB\Driver\Manager|\MongoClient
     */
    public $connection;

    /**
     * 数据库
     *
     * @var string
     */
    public $databaseName = 'test';

    /**
     * @var \MongoDB|\MongoDB\Driver\Server
     */
    public $database;

    /**
     * 集合
     *
     * @var string
     */
    public $collectionName = '';

    /**
     * 当前的数据表连接
     *
     * @var \MongoCollection
     */
    public $collection;

    public static $isMongoDB = null;

    /**
     * 默认返回的类型
     *
     * @see http://php.net/manual/en/mongodb-driver-cursor.settypemap.php
     * @var array
     */
    public static $defaultTypeMap = [
        'root'     => 'array',
        'document' => 'array',
        'array'    => 'array',
    ];

    public function __construct($connection, $database)
    {
        $this->connection   = $connection;
        $this->databaseName = $database;

        if (self::isMongoDB())
        {
            /**
             * @var \MongoDB\Driver\Manager $connection
             */
            $preference     = $connection->getReadPreference();
            $this->database = $this->connection->selectServer($preference);
        }
        else
        {
            /**
             * @var \MongoClient $connection
             */
            $this->database = $connection->selectDB($database);
        }
    }

    /**
     * 执行一个统计
     *
     * @param $pipeline
     * @return array
     */
    public function aggregate($pipeline, $opt = [])
    {
        if (self::isMongoDB())
        {
            /**
             * @var \MongoDB\Driver\Manager $connection
             */
            $cmd = [
                'aggregate' => $this->collectionName,
                'pipeline'  => $pipeline,
            ];

            $cursor = $this->command($cmd, $opt);

            if (is_array($cursor))
            {
                $result = $cursor;
            }
            else
            {
                $cursor->setTypeMap(static::$defaultTypeMap);
                $result       = current($cursor->toArray());
                $result['ok'] = 1;
            }
        }
        else
        {
            $result = $this->collection->aggregate($pipeline, $opt);
        }

        return $result;
    }



    /**
     * 执行一个命令
     *
     * @param array|string $data
     * @param array $opt
     */
    public function command($data, $opt = [])
    {
        if (self::isMongoDB())
        {
            $cursor = $this->database->executeCommand($this->databaseName, new \MongoDB\Driver\Command($data + $opt));

            # 设置返回类型
            $cursor->setTypeMap(static::$defaultTypeMap);

            $result = current($cursor->toArray());

            return $result;
        }
        else
        {
            if (is_array($data))
            {
                return $this->database->command($data, $opt);
            }
            else
            {
                return $this->database->execute($data, $opt);
            }
        }
    }

    /**
     * 删除数据, 返回作用行数
     *
     * @param $where
     * @param $opt
     * @return int
     */
    public function remove($where)
    {
        if (self::isMongoDB())
        {
            $bulk = new \MongoDB\Driver\BulkWrite();
            $bulk->delete($where, ['limit' => 0]);
            $writeResult = $this->database->executeBulkWrite($this->databaseName .'.'. $this->collectionName, $bulk);

            return $writeResult->getDeletedCount();
        }
        else
        {
            $rs = $this->collection->remove($where, ['timeout' => 60000]);

            return $rs['n'];
        }
    }

    /**
     * 更新数据, 返回作用行数
     *
     * @param $where
     * @param $data
     * @param $opt
     * @return int
     */
    public function update($where, $data, $opt)
    {
        if (self::isMongoDB())
        {
            $bulk = new \MongoDB\Driver\BulkWrite();
            $bulk->update($where, $data, $opt);
            $writeResult = $this->database->executeBulkWrite($this->databaseName .'.'. $this->collectionName, $bulk);

            return $writeResult->getModifiedCount();
        }
        else
        {
            if (isset($opt['multi']))
            {
                $opt['multiple'] = true;
                unset($opt['multi']);
            }

            $arr = $this->collection->update($where, $data, $opt);
            return $arr['n'];
        }
    }

    /**
     * 插入数据
     *
     * 返回的第一个是掺入的ID, 第二个是插入数
     *
     * @param $data
     * @param $opt
     * @return array
     */
    public function insert($data, $opt)
    {
        if (self::isMongoDB())
        {
            $bulk       = new \MongoDB\Driver\BulkWrite();
            $insertedId = $bulk->insert($data);

            if ($insertedId === null)
            {
                $insertedId = self::extractIdFromInsertedDocument($data);
            }

            $writeResult = $this->database->executeBulkWrite($this->databaseName .'.'. $this->collectionName, $bulk);

            return [(string)$insertedId, $writeResult->getInsertedCount()];
        }
        else
        {
            $result = $this->collection->insert($data, $opt);

            if (isset($data['_id']) &&  $data['_id'] instanceof \MongoId)
            {
                $rs = [
                    (string)$data['_id'],
                    1,
                ];
            }
            elseif (isset($result['data']['_id']) &&  $result['data']['_id'] instanceof \MongoId)
            {
                $rs = [
                    (string)$result['data']['_id'],
                    1,
                ];
            }
            elseif ($result['ok'] == 1)
            {
                $rs = [
                    '',
                    1,
                ];
            }
            else
            {
                $rs = [
                    '',
                    0,
                ];
            }

            return $rs;
        }
    }

    /**
     * 批量插入
     *
     * @param $data
     * @param $opt
     * @return int
     */
    public function batchInsert($data, $opt = array())
    {
        if (self::isMongoDB())
        {
            $bulk = new \MongoDB\Driver\BulkWrite();

            foreach ($data as $i => $item)
            {
                $bulk->insert($item);
            }

            $writeResult = $this->database->executeBulkWrite($this->databaseName .'.'. $this->collectionName, $bulk);

            return $writeResult->getInsertedCount();
        }
        else
        {
            $rs = $this->collection->batchInsert($data, $opt);

            if ($rs['ok'] == 1)
            {
                return count($data);
            }
            else
            {
                return 0;
            }
        }
    }

    /**
     * @param $where
     * @param $fields
     * @return \ArrayIterator|\MongoCursor
     */
    public function find($where, $fields, $timeout = 60000, $opt = array())
    {
        if (self::isMongoDB())
        {
            $opt['modifiers'] = [
                '$maxTimeMS' => $timeout,
            ];
            if ($fields)
            {
                $opt['projection'] = $fields;
            }

            $cursor = $this->database->executeQuery($this->databaseName .'.'. $this->collectionName, new \MongoDB\Driver\Query($where, $opt));
            $cursor->setTypeMap(static::$defaultTypeMap);

            return new \ArrayIterator($cursor->toArray());
        }
        else
        {
            $rs = $this->collection->find($where, $fields)->timeout($timeout);

            if (isset($opt['sort']))
            {
                $rs = $rs->sort($opt['sort']);
            }

            if (isset($opt['skip']) && $opt['skip'] > 0)
            {
                $rs = $rs->skip($opt['skip']);
            }

            if (isset($opt['limit']))
            {
                $rs = $rs->limit($opt['limit']);
            }

            return $rs;
        }
    }

    /**
     * @param       $where
     * @param array $opt
     * @return int
     */
    public function count($where, $opt = array())
    {
        $cmd = [
            'count' => $this->collectionName,
        ];

        if ($where)
        {
            $cmd['query'] = $where;
        }

        $cmd += $opt;

        $cursor = $this->command($cmd);
        if (!is_array($cursor))
        {
            $cursor = current($cursor->toArray());
        }

        return (int)$cursor['n'];
    }

    /**
     * 选择一个集合
     *
     * @param $collection
     * @return $this
     */
    public function selectCollection($collection)
    {
        $this->collectionName = $collection;

        if (!self::isMongoDB())
        {
            $this->collection = $this->database->selectCollection($collection);
        }

        return $this;
    }

    /**
     * 是否新版的MongoDB库
     *
     * @return bool
     */
    public static function isMongoDB()
    {
        if (null === self::$isMongoDB)
        {
            self::$isMongoDB = version_compare(PHP_VERSION, '7.0', '>=') ? true : false;
        }

        return self::$isMongoDB;
    }


    /**
     * Extracts an ID from an inserted document.
     *
     * This function is used when BulkWrite::insert() does not return a generated
     * ID, which means that the ID should be fetched from an array offset, public
     * property, or in the data returned by bsonSerialize().
     *
     * @internal
     * @see https://jira.mongodb.org/browse/PHPC-382
     * @param array|object $document Inserted document
     * @return mixed
     */
    protected static function extractIdFromInsertedDocument($document)
    {
        if ($document instanceof \MongoDB\BSON\Serializable)
        {
            return self::extractIdFromInsertedDocument($document->bsonSerialize());
        }

        return is_array($document) ? $document['_id'] : $document->_id;
    }
}